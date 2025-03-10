{-# language DataKinds #-}
{-# language LambdaCase #-}
{-# language MagicHash #-}
{-# language DuplicateRecordFields #-}
{-# language PatternSynonyms #-}
{-# language TypeApplications #-}
{-# language TypeOperators #-}
{-# language OverloadedRecordDot #-}
{-# language PatternSynonyms #-}

module Arrow.Builder.Vext
  ( Column(..)
  , VariableBinary(..)
  , NamedColumn(..)
  , Compression(..)
  , encode
    -- * Schema
  , makeSchema
    -- * Streaming
  , encodeBatch
  , encodePreludeAndSchema
  , encodeFooterAndEpilogue
  ) where

import Arrow.Builder.Raw

import Data.Int

import Arithmetic.Types (Fin32#)
import Data.Bytes.Types (ByteArrayN(ByteArrayN))
import Data.Primitive (SmallArray,ByteArray(ByteArray))
import Data.Primitive.Unlifted.Array (UnliftedArray)
import Data.Text (Text)
import Data.Unlifted (Bool#, pattern True#)
import Data.Unlifted (PrimArray#(PrimArray#))
import GHC.Exts (Int32#,Int64#,Word32#)
import GHC.TypeNats (type (+))

import qualified Arithmetic.Fin as Fin
import qualified Arithmetic.Nat as Nat
import qualified Arithmetic.Types as Arithmetic
import qualified Data.Builder.Catenable.Bytes as Catenable
import qualified Data.Primitive as PM
import qualified Data.Primitive.Contiguous as C
import qualified Data.Text as T
import qualified Flatbuffers.Builder as B
import qualified GHC.TypeNats as GHC
import qualified Vector.Bit as Bit
import qualified Vector.Int32 as Int32
import qualified Vector.Int64 as Int64
import qualified Vector.Word32 as Word32

data Column n
  = PrimitiveInt32
      !(Int32.Vector n Int32#)
  | PrimitiveWord32
      !(Word32.Vector n Word32#)
  | PrimitiveInt64
      !(Int64.Vector n Int64#)
  | VariableBinaryUtf8 !(VariableBinary n)
  | TimestampUtcMillisecond
      !(Int64.Vector n Int64#)
  | DurationMillisecond
      !(Int64.Vector n Int64#)
  | Date32
      !(Int32.Vector n Int32#)
  | Date64
      !(Int64.Vector n Int64#)

data VariableBinary (n :: GHC.Nat) = forall (m :: GHC.Nat). VariableBinary
  !(ByteArrayN m)
  -- Invariant unenforced by type system: these finite numbers must
  -- be in nondescending order. The first element should be zero, and the
  -- last element should be m.
  !(Int32.Vector (n + 1) (Fin32# (m + 1)))


data NamedColumn n = NamedColumn
  { name :: !Text
  , mask :: !(Bit.Vector n Bool#)
  , column :: !(Column n)
  }

makePayloads :: Arithmetic.Nat n -> SmallArray (NamedColumn n) -> UnliftedArray ByteArray
makePayloads !_ !cols = go 0 PayloadsNil
  where
  go :: Int -> Payloads -> UnliftedArray ByteArray
  go !ix !acc = if ix < PM.sizeofSmallArray cols
    then
      let NamedColumn{mask,column} = PM.indexSmallArray cols ix
          finishPrimitive !exposed = case Bit.expose mask of
            PrimArray# b -> 
              let exposedMask = ByteArray b
                  acc' = consPrimitive exposed exposedMask acc
               in go (ix + 1) acc'
       in case column of
            VariableBinaryUtf8 (VariableBinary (ByteArrayN b) szs) ->
              let !acc' = consVariableBinary b
                    (ByteArray (case Bit.expose mask of PrimArray# x -> x))
                    (ByteArray (case Int32.expose szs of PrimArray# x -> x))
                    acc
               in go (ix + 1) acc'
            PrimitiveInt32 v -> case Int32.expose v of
              PrimArray# b ->
                let b' = ByteArray b
                 in finishPrimitive b'
            PrimitiveWord32 v -> case Word32.expose v of
              PrimArray# b ->
                let b' = ByteArray b
                 in finishPrimitive b'
            PrimitiveInt64 v -> case Int64.expose v of
              PrimArray# b ->
                let b' = ByteArray b
                 in finishPrimitive b'
            TimestampUtcMillisecond v -> case Int64.expose v of
              PrimArray# b ->
                let b' = ByteArray b
                 in finishPrimitive b'
            DurationMillisecond v -> case Int64.expose v of
              PrimArray# b ->
                let b' = ByteArray b
                 in finishPrimitive b'
            Date32 v -> case Int32.expose v of
              PrimArray# b ->
                let b' = ByteArray b
                 in finishPrimitive b'
            Date64 v -> case Int64.expose v of
              PrimArray# b ->
                let b' = ByteArray b
                 in finishPrimitive b'
    else payloadsToArray acc

columnToType :: Column n -> Type
columnToType = \case
  PrimitiveInt32{} -> Int TableInt{bitWidth=32,isSigned=True}
  PrimitiveWord32{} -> Int TableInt{bitWidth=32,isSigned=False}
  PrimitiveInt64{} -> Int TableInt{bitWidth=64,isSigned=True}
  TimestampUtcMillisecond{} ->
    Timestamp TableTimestamp{unit=Millisecond,timezone=T.pack "UTC"}
  DurationMillisecond{} -> Duration Millisecond
  Date32{} -> Date (TableDate Day)
  Date64{} -> Date (TableDate DateMillisecond)
  VariableBinaryUtf8{} -> Utf8

namedColumnToField :: NamedColumn n -> Field
namedColumnToField NamedColumn{name,column} = Field
  { name = name
  , nullable = True
  , type_ = columnToType column
  , dictionary = Nothing
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
        { offset = 0 -- Offset is bogus. This is replaced by the true offset in encode.
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
naivePopCount :: Arithmetic.Nat n -> Bit.Vector n Bool# -> Int
naivePopCount !n !v = Fin.ascend' n 0 $ \fin acc ->
  acc + case Bit.index v (Fin.unlift fin) of
    True# -> 1
    _ -> 0

makeRecordBatch ::
     Arithmetic.Nat n
  -> Compression
  -> SmallArray Buffer
  -> SmallArray (NamedColumn n)
  -> RecordBatch
makeRecordBatch !n cmpr buffers !cols = RecordBatch
  { length = fromIntegral (Nat.demote n)
  , nodes = C.map'
      (\NamedColumn{mask} -> FieldNode
        { length=fromIntegral (Nat.demote n)
        , nullCount=fromIntegral @Int @Int64 (Nat.demote n - naivePopCount n mask)
        }
      ) cols
  , buffers = buffers
  , compression = marshallCompression cmpr
  }

