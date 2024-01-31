{-# language DataKinds #-}
{-# language LambdaCase #-}
{-# language MagicHash #-}
{-# language DuplicateRecordFields #-}
{-# language TypeApplications #-}
{-# language TypeOperators #-}
{-# language OverloadedRecordDot #-}

module ArrowBuilder
  ( Column(..)
  , NamedColumn(..)
  , Contents(..)
  , MaskedColumn(..)
  , Compression(..)
  , encode
    -- * Schema
  , makeSchema
    -- * Streaming
  , encodeBatch
  , encodePreludeAndSchema
  , encodeFooterAndEpilogue
  ) where

import ArrowFile
import ArrowSchema
import ArrowMessage

import Data.Word
import Data.Int

import ArrowBuilderNoColumn

import Control.Monad.ST (runST)
import Data.Primitive (SmallArray,ByteArray(ByteArray))
import Data.Text (Text)
import Arithmetic.Types (Fin(Fin))
import GHC.TypeNats (type (+))

import qualified Arithmetic.Fin as Fin
import qualified Arithmetic.Types as Arithmetic
import qualified Arithmetic.Nat as Nat
import qualified Arithmetic.Lt as Lt
import qualified Arithmetic.Lte as Lte
import qualified Data.List as List
import qualified Data.Primitive.Contiguous as C
import qualified Data.Primitive as PM
import qualified GHC.Exts as Exts
import qualified Data.Bytes.Builder.Bounded as Bounded
import qualified Data.Bytes as Bytes
import qualified Data.Bytes.Chunks as Chunks
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
import qualified Data.ByteString.Short as SBS
import qualified Data.ByteString.Short.Internal as SBS
import qualified Data.Text.Short as TS

data Column n
  = PrimitiveWord8 !(Word8.Vector n)
  | PrimitiveWord16 !(Word16.Vector n)
  | PrimitiveWord32 !(Word32.Vector n)
  | PrimitiveWord64 !(Word64.Vector n)
  | PrimitiveWord128 !(Word128.Vector n)
  | PrimitiveWord256 !(Word256.Vector n)
  | PrimitiveInt64 !(Int64.Vector n)
  | VariableBinaryUtf8 !(ShortText.Vector n)
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

makePayloads :: Arithmetic.Nat n -> SmallArray (NamedColumn n) -> [Payload]
makePayloads !n !cols = List.reverse (makePayloadsBackwardsOnto n cols [])

makePayloadsBackwardsOnto :: Arithmetic.Nat n -> SmallArray (NamedColumn n) -> [Payload] -> [Payload]
makePayloadsBackwardsOnto !n !cols !acc0 = C.foldl'
  (\acc NamedColumn{contents} -> 
    let finishPrimitive !exposedMask !exposed = 
          consPrimitive exposed exposedMask acc
     in case contents of
          Children xs -> makePayloadsBackwardsOnto n xs (consValidityMask (Bool.expose (makeTrueVec n)) acc)
          Values MaskedColumn{mask,column} -> case column of
            PrimitiveWord8 v -> finishPrimitive (Bool.expose mask) (Word8.expose v)
            PrimitiveWord16 v -> finishPrimitive (Bool.expose mask) (Word16.expose v)
            PrimitiveWord32 v -> finishPrimitive (Bool.expose mask) (Word32.expose v)
            PrimitiveWord64 v -> finishPrimitive (Bool.expose mask) (Word64.expose v)
            -- Note: Word128 and Word256 are not handled correctly
            PrimitiveWord128 v -> finishPrimitive (Bool.expose mask) (Word128.expose v)
            PrimitiveWord256 v -> finishPrimitive (Bool.expose mask) (Word256.expose v)
            PrimitiveInt64 v -> finishPrimitive (Bool.expose mask) (Int64.expose v)
            VariableBinaryUtf8 v ->
              let (offsets, totalPayloadSize) = makeVariableBinaryOffsets n v
                  exposed = unsafeConcatenate totalPayloadSize n v
                  exposedOffsets = Word32.expose offsets
                  exposedMask = Bool.expose mask
                  acc' = consVariableBinary exposed exposedMask exposedOffsets acc
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
  PrimitiveWord8{} -> Int TableInt{bitWidth=8,isSigned=False}
  PrimitiveWord16{} -> Int TableInt{bitWidth=16,isSigned=False}
  PrimitiveWord32{} -> Int TableInt{bitWidth=32,isSigned=False}
  PrimitiveWord64{} -> Int TableInt{bitWidth=64,isSigned=False}
  PrimitiveWord128{} -> FixedSizeBinary TableFixedSizeBinary{byteWidth=16}
  PrimitiveWord256{} -> FixedSizeBinary TableFixedSizeBinary{byteWidth=32}
  PrimitiveInt64{} -> Int TableInt{bitWidth=64,isSigned=True}
  VariableBinaryUtf8{} -> Utf8

namedColumnToField :: NamedColumn n -> Field
namedColumnToField NamedColumn{name,contents} = case contents of
  Children children -> Field
    { name = name
    , nullable = False
    , type_ = Struct
    , dictionary = ()
    , children = C.map' namedColumnToField children
    }
  Values MaskedColumn{column} -> Field
    { name = name
    , nullable = True
    , type_ = columnToType column
    , dictionary = ()
    , children = mempty
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
  let payloads = Exts.fromList (makePayloads n namedColumns) :: SmallArray Payload
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
  -> SmallArray Buffer
  -> SmallArray (NamedColumn n)
  -> RecordBatch
makeRecordBatch !n cmpr buffers !cols = RecordBatch
  { length = fromIntegral (Nat.demote n)
  , nodes = C.map'
      (\MaskedColumn{mask} -> FieldNode
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
