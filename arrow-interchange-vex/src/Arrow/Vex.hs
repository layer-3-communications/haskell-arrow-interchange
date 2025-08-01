{-# language DataKinds #-}
{-# language LambdaCase #-}
{-# language MagicHash #-}
{-# language DuplicateRecordFields #-}
{-# language TypeApplications #-}
{-# language TypeOperators #-}
{-# language ScopedTypeVariables #-}
{-# language OverloadedRecordDot #-}

module Arrow.Vex
  ( Column(..)
  , NamedColumn(..)
  , Contents(..)
  , MaskedColumn(..)
  , Compression(..)
  , Footer(..)
  , Block(..)
  , Schema(..)
  , encode
    -- * Schema
  , makeSchema
    -- * Streaming
  , encodeBatch
  , encodePreludeAndSchema
  , encodeFooterAndEpilogue
  ) where

import Arrow.Builder.Raw

import Data.Word
import Data.Int

import Arithmetic.Types (Fin(Fin))
import Control.Monad (join)
import Control.Monad.ST (runST)
import Data.Primitive (SmallArray,ByteArray(ByteArray))
import Data.Primitive.Unlifted.Array (UnliftedArray)
import Data.Primitive (PrimArray)
import Data.Text (Text)
import GHC.TypeNats (type (+))

import qualified Arithmetic.Fin as Fin
import qualified Arithmetic.Types as Arithmetic
import qualified Arithmetic.Nat as Nat
import qualified Arithmetic.Lt as Lt
import qualified Arithmetic.Lte as Lte
import qualified Data.Primitive.Contiguous as C
import qualified Data.Primitive as PM
import qualified Data.Text as T
import qualified Data.Primitive.Unlifted.Array as PM
import qualified Flatbuffers.Builder as B
import qualified Data.Builder.Catenable.Bytes as Catenable
import qualified Vector.Word8.Internal as Word8
import qualified Vector.Word16.Internal as Word16
import qualified Vector.Word32.Internal as Word32
import qualified Vector.Word64.Internal as Word64
import qualified Vector.Word128.Internal as Word128
import qualified Vector.Word256.Internal as Word256
import qualified Vector.Int64.Internal as Int64
import qualified Vector.Bool.Internal as Bool
import qualified Vector.ShortText.Internal as ShortText
import qualified Vector.ShortTexts.Internal as ShortTexts
import qualified Data.ByteString.Short as SBS
import qualified Data.Text.Short as TS

data Column n
  = PrimitiveBool !(Bool.Vector n)
  | PrimitiveWord8 !(Word8.Vector n)
  | PrimitiveWord16 !(Word16.Vector n)
  | PrimitiveWord32 !(Word32.Vector n)
  | PrimitiveWord64 !(Word64.Vector n)
  | PrimitiveWord256 !(Word256.Vector n)
  | Word128FixedSizeListWord8 !(Word128.Vector n)
    -- ^ Encode a vector of unsigned 128-bit words as FixedSizeList<byte>[16]
  | Word128FixedSizeBinary !(Word128.Vector n)
    -- ^ Encode a vector of unsigned 128-bit words as FixedSizeBinary(16)
  | PrimitiveInt64 !(Int64.Vector n)
  | Date64 !(Int64.Vector n)
  | VariableBinaryUtf8 !(ShortText.Vector n)
  | ListVariableBinaryUtf8 !(ShortTexts.Vector n)
  | NoColumn -- We use this when encoding structs. It has no payload.

data Contents n
  = Values !(MaskedColumn n)
  | Children !(SmallArray (NamedColumn n))

data MaskedColumn n = MaskedColumn
  { mask :: !(Bool.Vector n)
  , column :: !(Column n)
  }

data NamedColumn n = NamedColumn
  { name :: !Text
  , contents :: !(Contents n)
  }

flattenNamedColumn :: Arithmetic.Nat n -> NamedColumn n -> SmallArray (MaskedColumn n)
flattenNamedColumn !n NamedColumn{contents} = case contents of
  Values m -> pure m
  Children xs -> C.insertAt (flattenNamedColumns n xs) 0 MaskedColumn{mask=makeTrueVec n,column=NoColumn}

flattenNamedColumns :: Arithmetic.Nat n -> SmallArray (NamedColumn n) -> SmallArray (MaskedColumn n)
flattenNamedColumns !n !xs = xs >>= flattenNamedColumn n

makePayloads :: Arithmetic.Nat n -> SmallArray (NamedColumn n) -> UnliftedArray ByteArray
makePayloads !n !cols = payloadsToArray (makePayloadsBackwardsOnto n cols PayloadsNil)

makePayloadsBackwardsOnto :: Arithmetic.Nat n -> SmallArray (NamedColumn n) -> Payloads -> Payloads
makePayloadsBackwardsOnto !n !cols !acc0 = C.foldl'
  (\acc NamedColumn{contents} -> 
    let finishPrimitive !exposedMask !exposed = 
          consPrimitive exposed exposedMask acc
     in case contents of
          Children xs -> makePayloadsBackwardsOnto n xs (consValidityMask (Bool.expose (makeTrueVec n)) acc)
          Values MaskedColumn{mask,column} -> case column of
            -- Bool does not actually use primitive but the layout is the same
            PrimitiveBool v -> finishPrimitive (Bool.expose mask) (Bool.expose v)
            PrimitiveWord8 v -> finishPrimitive (Bool.expose mask) (Word8.expose v)
            PrimitiveWord16 v -> finishPrimitive (Bool.expose mask) (Word16.expose v)
            PrimitiveWord32 v -> finishPrimitive (Bool.expose mask) (Word32.expose v)
            PrimitiveWord64 v -> finishPrimitive (Bool.expose mask) (Word64.expose v)
            -- Note: Word128 and Word256 are not handled correctly. We need to
            -- ensure a big-endian byte order.
            Word128FixedSizeListWord8 v -> consPrimitive (Word128.expose v) (Bool.expose (makeTrueVec n))
              $ consValidityMask (Bool.expose mask) acc
            Word128FixedSizeBinary v -> finishPrimitive (Bool.expose mask) (Word128.expose v)
            PrimitiveWord256 v -> finishPrimitive (Bool.expose mask) (Word256.expose v)
            PrimitiveInt64 v -> finishPrimitive (Bool.expose mask) (Int64.expose v)
            Date64 v -> finishPrimitive (Bool.expose mask) (Int64.expose v)
            VariableBinaryUtf8 v ->
              let (offsets, totalPayloadSize) = makeVariableBinaryOffsets n v
                  exposed = unsafeConcatenate totalPayloadSize n v
                  exposedOffsets = Word32.expose offsets
                  exposedMask = Bool.expose mask
                  acc' = consVariableBinary exposed exposedMask exposedOffsets acc
               in acc'
            ListVariableBinaryUtf8 txts -> case explodeVarBinList n txts of
              StringListExplosion{elementCount=m,elements=v,indices=listOffsets} -> 
                let (offsets, totalPayloadSize) = makeVariableBinaryOffsets m v
                    exposed = unsafeConcatenate totalPayloadSize m v
                    acc' = consVariableBinary exposed (Bool.expose (makeTrueVec m)) (Word32.expose offsets)
                      (consListMetadata (Bool.expose mask) (Word32.expose listOffsets) acc)
                 in acc'
  ) acc0 cols

unsafeConcatenate :: Int -> Arithmetic.Nat n -> ShortText.Vector n -> ByteArray
unsafeConcatenate !sz !n !v = runST $ do
  dst <- PM.newByteArray sz
  total <- Fin.ascendM n (0 :: Int) $ \(Fin ix lt) !offset -> do
    let val = ShortText.index lt v ix
    case TS.toShortByteString val of
      SBS.SBS b -> do
        let len = PM.sizeofByteArray (ByteArray b)
        PM.copyByteArray dst offset (ByteArray b) 0 len
        pure (offset + len)
  if total == sz
    then PM.unsafeFreezeByteArray dst
    else errorWithoutStackTrace "arrow-interchange:ArrowBuilder.unsafeConcatenate"

data StringListExplosion n = forall m. StringListExplosion
  { elementCount :: !(Arithmetic.Nat m)
  , elements :: !(ShortText.Vector m)
  , indices :: !(Word32.Vector (n + 1)) -- indices into texts array (must be in nondescending order)
  }

countAllElements :: Arithmetic.Nat n -> ShortTexts.Vector n -> Int
countAllElements !n !src = Fin.ascend' n 0 (\(Fin ix lt) acc -> PM.sizeofUnliftedArray (ShortTexts.index lt src ix) + acc)

explodeVarBinList :: forall n. Arithmetic.Nat n -> ShortTexts.Vector n -> StringListExplosion n
explodeVarBinList !n !src = runST $ do
  ixsDst <- Word32.uninitialized (Nat.succ n)
  let totalElements = countAllElements n src
  Nat.with totalElements $ \m -> do
    elemsDst <- ShortText.uninitialized m
    ShortText.set Lte.reflexive elemsDst Nat.zero m TS.empty
    finalOffset <- Fin.ascendM n (0 :: Int) $ \(Fin ix lt) !offset0 -> C.foldlM'
      (\offset txt -> case Fin.fromInt m offset of
        Nothing -> errorWithoutStackTrace "ArrowBuilder.explodeVarBinList: implementation mistake A"
        Just (Fin off offLt) -> do
          ShortText.write offLt elemsDst off txt
          pure (offset + 1)
      ) offset0 (ShortTexts.index lt src ix)
    if finalOffset /= totalElements
      then errorWithoutStackTrace "ArrowBuilder.explodeVarBinList: implementation mistake B"
      else do
        elemsDst' <- ShortText.unsafeFreeze elemsDst
        finalIxOff <- Fin.ascendM n (0 :: Int) $ \(Fin ix lt) !offset0 -> do
          let txts = ShortTexts.index lt src ix
          Word32.write (Lt.weakenR @1 lt) ixsDst ix (fromIntegral offset0 :: Word32)
          pure (offset0 + PM.sizeofUnliftedArray txts)
        if finalIxOff /= Nat.demote m
          then errorWithoutStackTrace "ArrowBuilder.explodeVarBinList: implementation mistake C"
          else do
            Word32.write (Lt.incrementL @n Lt.zero) ixsDst n (fromIntegral finalIxOff :: Word32)
            ixsDst' <- Word32.unsafeFreeze ixsDst
            pure StringListExplosion
              { elementCount = m
              , elements = elemsDst'
              , indices = ixsDst'
              }

-- Returns the offsets vector and the total size of the concatenated strings
makeVariableBinaryOffsets :: forall n. Arithmetic.Nat n -> ShortText.Vector n -> (Word32.Vector (n + 1), Int)
makeVariableBinaryOffsets !n !src = runST $ do
  dst <- Word32.uninitialized (Nat.succ n)
  total <- Fin.ascendM n (0 :: Int) $ \(Fin ix lt) !offset -> do
    let val = ShortText.index lt src ix
    Word32.write (Lt.weakenR @1 lt) dst ix (fromIntegral offset :: Word32)
    pure (offset + SBS.length (TS.toShortByteString val))
  Word32.write (Lt.incrementL @n Lt.zero) dst n (fromIntegral total :: Word32)
  dst' <- Word32.unsafeFreeze dst
  pure (dst',total)

columnToType :: Column n -> Type
columnToType = \case
  PrimitiveBool{} -> Bool
  PrimitiveWord8{} -> Int TableInt{bitWidth=8,isSigned=False}
  PrimitiveWord16{} -> Int TableInt{bitWidth=16,isSigned=False}
  PrimitiveWord32{} -> Int TableInt{bitWidth=32,isSigned=False}
  PrimitiveWord64{} -> Int TableInt{bitWidth=64,isSigned=False}
  Word128FixedSizeListWord8{} -> FixedSizeList TableFixedSizeList{listSize=16}
  Word128FixedSizeBinary{} -> FixedSizeBinary TableFixedSizeBinary{byteWidth=16}
  PrimitiveWord256{} -> FixedSizeBinary TableFixedSizeBinary{byteWidth=32}
  PrimitiveInt64{} -> Int TableInt{bitWidth=64,isSigned=True}
  Date64{} -> Date (TableDate DateMillisecond)
  VariableBinaryUtf8{} -> Utf8
  ListVariableBinaryUtf8{} -> List

namedColumnToField :: NamedColumn n -> Field
namedColumnToField NamedColumn{name,contents} = case contents of
  Children children -> Field
    { name = name
    , nullable = False
    , type_ = Struct
    , dictionary = Nothing
    , children = C.map' namedColumnToField children
    }
  Values MaskedColumn{column} -> Field
    { name = name
    , nullable = True
    , type_ = columnToType column
    , dictionary = Nothing
    , children = case column of
        ListVariableBinaryUtf8{} -> pure Field
          { name = T.empty
          , nullable = False
          , type_ = Utf8
          , dictionary = Nothing
          , children = mempty
          }
        -- We currently only used fixed-size lists for unsigned 8-bit words.
        Word128FixedSizeListWord8{} -> pure Field
          { name = T.empty
          , nullable = False
          , type_ = Int TableInt{bitWidth=8,isSigned=False}
          , dictionary = Nothing
          , children = mempty
          }
        _ -> mempty
    }

-- | Convert named columns to a description of the schema.
makeSchema :: SmallArray (NamedColumn n) -> Schema
makeSchema !namedColumns = Schema
  { endianness = 0
  , fields = fmap namedColumnToField namedColumns
  }

-- | Includes the following:
--
-- * Continuation bytes
-- * Metadata length (4 bytes)
-- * Metadata
-- * Payloads
--
-- A 'Block' is provided so that the caller has the information needed to
-- construct the footer. This 'Block' has the offset set to zero (we do
-- not have enough data to compute it here), so the caller must reset that
-- field.
encodeBatch ::
     Arithmetic.Nat n
  -> Compression
  -> SmallArray (NamedColumn n)
  -> (Catenable.Builder,Block)
encodeBatch !n cmpr !namedColumns =
  let payloads = makePayloads n namedColumns :: UnliftedArray ByteArray
      (body,buffers) = case cmpr of
        None -> encodePayloadsUncompressed payloads
        Lz4 -> encodePayloadsLz4 payloads
      bodyLength = Catenable.length body
      recordBatch = makeRecordBatch n cmpr buffers namedColumns
      encodedRecordBatch = B.encode $ encodeMessage $ Message
        { header=MessageHeaderRecordBatch recordBatch
        , bodyLength=fromIntegral bodyLength
        }
      partB = encodePartB encodedRecordBatch
      block = Block
        { offset = 0
        , metaDataLength = fromIntegral @Int @Int32 (Catenable.length partB)
        , bodyLength = fromIntegral @Int @Int64 bodyLength
        }
   in (partB <> body,block)

-- | Encode a single batch of records.
encode :: Arithmetic.Nat n -> Compression -> SmallArray (NamedColumn n) -> Catenable.Builder
encode !n cmpr !namedColumns = 
  let partA = encodePreludeAndSchema schema
      (partB,block) = encodeBatch n cmpr namedColumns
      block' = Block
        { offset=fromIntegral (Catenable.length partA)
        , metaDataLength = block.metaDataLength
        , bodyLength = block.bodyLength
        }
   in partA
      <>
      partB
      <>
      encodeFooterAndEpilogue schema (C.singleton block')
  where
  schema = makeSchema namedColumns

-- TODO: Replace this. It is slow and awful.
naivePopCount :: Arithmetic.Nat n -> Bool.Vector n -> Int
naivePopCount !n !v = Fin.ascend' n 0 $ \(Fin ix lt) acc -> acc + (if Bool.index lt v ix then 1 else 0)

makeRecordBatch ::
     Arithmetic.Nat n
  -> Compression
  -> PrimArray Buffer
  -> SmallArray (NamedColumn n)
  -> RecordBatch
makeRecordBatch !n cmpr buffers !cols = RecordBatch
  { length = fromIntegral (Nat.demote n)
    -- Note: Try to rewrite this at some point. This builds a
    -- SmallArray and then converts to a PrimArray.
  , nodes = C.convert $ join $ C.map'
      (\MaskedColumn{mask,column} -> case column of
        ListVariableBinaryUtf8 txts -> C.doubleton
          ( FieldNode
            { length=fromIntegral (Nat.demote n)
            , nullCount=fromIntegral @Int @Int64 (Nat.demote n - naivePopCount n mask)
            }
          )
          ( FieldNode
            { length=fromIntegral (countAllElements n txts)
            , nullCount=0
            }
          ) :: SmallArray FieldNode
        Word128FixedSizeListWord8 _ -> C.doubleton
          ( FieldNode
            { length=fromIntegral (Nat.demote n)
            , nullCount=fromIntegral @Int @Int64 (Nat.demote n - naivePopCount n mask)
            }
          )
          ( FieldNode
            { length=fromIntegral (16 * Nat.demote n)
            , nullCount=0
            }
          )
        _ -> C.singleton $! FieldNode
          { length=fromIntegral (Nat.demote n)
          , nullCount=fromIntegral @Int @Int64 (Nat.demote n - naivePopCount n mask)
          }
      ) (flattenNamedColumns n cols)
  , buffers = buffers
  , compression = marshallCompression cmpr
  }

makeTrueVec :: Arithmetic.Nat n -> Bool.Vector n
makeTrueVec !n = Bool.runST $ do
  dst <- Bool.uninitialized n
  Bool.set Lte.reflexive dst Nat.zero n True
  Bool.unsafeFreeze dst
