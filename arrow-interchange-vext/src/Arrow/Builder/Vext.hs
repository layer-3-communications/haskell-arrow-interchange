{-# language DataKinds #-}
{-# language MultiWayIf #-}
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
  , NamedColumns(..)
  , Compression(..)
  , encode
  , encodeNamedColumns
  , decode
    -- * Schema
  , makeSchema
    -- * Streaming
  , encodeBatch
  , encodePreludeAndSchema
  , encodeFooterAndEpilogue
  ) where

import Arrow.Builder.Raw

import Data.Int

import Control.Monad (when)
import Arithmetic.Types (Fin32#,Nat#,(:=:#))
import Data.Bytes.Types (ByteArrayN(ByteArrayN),Bytes(Bytes))
import Data.Primitive (SmallArray,ByteArray(ByteArray))
import Data.Primitive.Unlifted.Array (UnliftedArray)
import Data.Text (Text)
import Data.Unlifted (Bool#, pattern True#)
import Data.Unlifted (PrimArray#(PrimArray#))
import Data.Maybe.Void (pattern JustVoid#)
import Data.Word (Word32)
import GHC.Exts (Int8#,Int32#,Int64#,Word64#,Word32#,Word16#,Word8#)
import GHC.Int (Int64(I64#),Int(I#))
import GHC.TypeNats (Nat, type (+))

import qualified Data.List as List
import qualified Arithmetic.Fin as Fin
import qualified Arithmetic.Nat as Nat
import qualified Arithmetic.Types as Arithmetic
import qualified ArrowParser
import qualified Data.Builder.Catenable.Bytes as Catenable
import qualified Data.Primitive as PM
import qualified Data.Primitive.Contiguous as C
import qualified Data.Text as T
import qualified Flatbuffers.Builder as B
import qualified GHC.Exts as Exts
import qualified GHC.TypeNats as GHC
import qualified Lz4.Frame
import qualified Data.Primitive.ByteArray.LittleEndian as LE
import qualified Vector.Bit as Bit
import qualified Vector.Int32 as Int32
import qualified Vector.Int64 as Int64
import qualified Vector.Word8 as Word8
import qualified Vector.Int8 as Int8
import qualified Vector.Word16 as Word16
import qualified Vector.Word32 as Word32
import qualified Vector.Word64 as Word64

data Column n
  = PrimitiveInt8
      !(Int8.Vector n Int8#)
  | PrimitiveInt32
      !(Int32.Vector n Int32#)
  | PrimitiveWord8
      !(Word8.Vector n Word8#)
  | PrimitiveWord16
      !(Word16.Vector n Word16#)
  | PrimitiveWord32
      !(Word32.Vector n Word32#)
  | PrimitiveWord64
      !(Word64.Vector n Word64#)
  | PrimitiveInt64
      !(Int64.Vector n Int64#)
  | VariableBinaryUtf8 !(VariableBinary n)
  | TimestampUtcMillisecond
      !(Int64.Vector n Int64#)
  | TimestampUtcSecond
      !(Int64.Vector n Int64#)
  | DurationMillisecond
      !(Int64.Vector n Int64#)
  | Date32
      !(Int32.Vector n Int32#)
  | Date64
      !(Int64.Vector n Int64#)

showColumn :: Arithmetic.Nat# n -> Column n -> String
showColumn n (PrimitiveInt32 x) = Int32.show n x
showColumn n (PrimitiveInt64 x) = Int64.show n x
showColumn n (PrimitiveWord8 x) = Word8.show n x
showColumn n (PrimitiveWord16 x) = Word16.show n x
showColumn n (PrimitiveWord32 x) = Word32.show n x
showColumn n (PrimitiveWord64 x) = Word64.show n x

showNamedColumn :: Nat# n -> NamedColumn n -> String
showNamedColumn n (NamedColumn name mask column) = "NamedColumn " ++ show name ++ " " ++ "???" ++ " " ++ showColumn n column

instance Show NamedColumns where
  show (NamedColumns n xs) = "NamedColumn " ++ show (I# (Nat.demote# n)) ++ " [" ++ strs ++ "]"
    where
    strs = List.intercalate "," (map (showNamedColumn n) (Exts.toList xs))

eqNamedColumn :: Nat# n -> m :=:# n -> NamedColumn n -> NamedColumn m -> Bool
eqNamedColumn n eq x y =
  x.name == y.name
  &&
  Bit.equals n x.mask (Bit.substitute eq y.mask)
  &&
  eqColumn n eq x.column y.column

eqColumn :: Nat# n -> m :=:# n -> Column n -> Column m -> Bool
{-# noinline eqColumn #-}
eqColumn n eq x y = go x y where
  go (PrimitiveInt32 a) (PrimitiveInt32 b) = Int32.equals n a (Int32.substitute eq b)
  go (PrimitiveInt64 a) (PrimitiveInt64 b) = Int64.equals n a (Int64.substitute eq b)
  go (PrimitiveWord64 a) (PrimitiveWord64 b) = Word64.equals n a (Word64.substitute eq b)
  go (PrimitiveWord32 a) (PrimitiveWord32 b) = Word32.equals n a (Word32.substitute eq b)
  go (PrimitiveWord16 a) (PrimitiveWord16 b) = Word16.equals n a (Word16.substitute eq b)
  go (PrimitiveWord8 a) (PrimitiveWord8 b) = Word8.equals n a (Word8.substitute eq b)
  go _ _ = error "Arrow.Builder.Vext.eqColumn: finish writing this"

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
            PrimitiveWord64 v -> case Word64.expose v of
              PrimArray# b ->
                let b' = ByteArray b
                 in finishPrimitive b'
            PrimitiveWord8 v -> case Word8.expose v of
              PrimArray# b ->
                let b' = ByteArray b
                 in finishPrimitive b'
            PrimitiveWord16 v -> case Word16.expose v of
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
            TimestampUtcSecond v -> case Int64.expose v of
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
  PrimitiveWord64{} -> Int TableInt{bitWidth=64,isSigned=False}
  PrimitiveWord32{} -> Int TableInt{bitWidth=32,isSigned=False}
  PrimitiveWord16{} -> Int TableInt{bitWidth=16,isSigned=False}
  PrimitiveWord8{} -> Int TableInt{bitWidth=8,isSigned=False}
  PrimitiveInt64{} -> Int TableInt{bitWidth=64,isSigned=True}
  TimestampUtcMillisecond{} ->
    Timestamp TableTimestamp{unit=Millisecond,timezone=T.pack "UTC"}
  TimestampUtcSecond{} ->
    Timestamp TableTimestamp{unit=Second,timezone=T.pack "UTC"}
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

encodeNamedColumns :: Compression -> NamedColumns -> Catenable.Builder
encodeNamedColumns cmpr (NamedColumns size columns) =
  encode (Nat.lift size) cmpr columns

data NamedColumns = forall (n :: Nat). NamedColumns
  { size :: Nat# n
  , columns :: !(SmallArray (NamedColumn n))
  }

instance Eq NamedColumns where
  NamedColumns szA columnsA == NamedColumns szB columnsB = case Nat.testEqual# szB szA of
    JustVoid# eq ->
      Prelude.length columnsA == Prelude.length columnsB
      &&
      C.foldrZipWith (\a b acc -> eqNamedColumn szA eq a b && acc) True columnsA columnsB
    _ -> False

-- | Decode a single batch of records
decode :: ByteArray -> Either ArrowParser.Error NamedColumns
decode contents = do
  footer <- ArrowParser.decodeFooterFromArrowContents contents
  let batches = footer.recordBatches
  case PM.sizeofPrimArray batches of
    0 -> Left ArrowParser.ZeroBatches
    1 -> pure ()
    _ -> Left ArrowParser.MoreThanOneBatch
  let !block0 = PM.indexPrimArray batches 0
  let !bodyStart = roundUp8 (i64ToI block0.offset + 8 + i32ToI block0.metaDataLength)
  let !bodyEnd = bodyStart + i64ToI block0.bodyLength
  case footer.schema.endianness of
    0 -> pure ()
    _ -> Left ArrowParser.BigEndianBuffersNotSupported
  -- TODO: check that body end is actually in bounds
  metadata <- ArrowParser.extractMetadata contents block0
  case metadata.header of
    MessageHeaderRecordBatch batch -> do
      -- Note: the batch length is the number of elements, not bytes
      when (batch.length < 0) (Left ArrowParser.NegativeBatchLength)
      when (PM.sizeofPrimArray batch.nodes /= PM.sizeofSmallArray footer.schema.fields) (Left ArrowParser.SchemaFieldCountDoesNotMatchNodeCount)
      let !bufferCount = PM.sizeofPrimArray batch.buffers
      -- Left (ArrowParser.Placeholder (show metadata))
      Nat.with# (case batch.length of {I64# i -> Exts.int64ToInt# i}) $ \n -> do
        let defaultValidity = Bit.replicate n True#
        (_, finalBldr) <- C.foldlZipWithM'
          (\(bufIx, bldr) node field -> case field.type_ of
            -- Currently ignoring the time zone. Fix this.
            Timestamp TableTimestamp{unit=Second} -> do
              (trueOffElems, trueContents) <- primitiveColumnExtraction bodyStart bodyEnd contents n bufferCount bufIx field 8 batch
              let !col = NamedColumn field.name defaultValidity (TimestampUtcSecond (Int64.cloneFromByteArray trueOffElems n trueContents))
              let !bldr' = col : bldr
              pure (bufIx + 2, bldr')
            Int TableInt{bitWidth,isSigned} -> do
              byteWidth <- case bitWidth of
                8 -> Right 1
                16 -> Right 2
                32 -> Right 4
                64 -> Right 8
                _ -> Left ArrowParser.UnsupportedBitWidth
              (trueOffElems, trueContents) <- primitiveColumnExtraction bodyStart bodyEnd contents n bufferCount bufIx field byteWidth batch
              !col <-
                if | 1 <- byteWidth, True <- isSigned ->
                       Right $! NamedColumn field.name defaultValidity (PrimitiveInt8 (Int8.cloneFromByteArray trueOffElems n trueContents))
                   | 4 <- byteWidth, True <- isSigned ->
                       Right $! NamedColumn field.name defaultValidity (PrimitiveInt32 (Int32.cloneFromByteArray trueOffElems n trueContents))
                   | 8 <- byteWidth, True <- isSigned ->
                       Right $! NamedColumn field.name defaultValidity (PrimitiveInt64 (Int64.cloneFromByteArray trueOffElems n trueContents))
                   | 1 <- byteWidth, False <- isSigned ->
                       Right $! NamedColumn field.name defaultValidity (PrimitiveWord8 (Word8.cloneFromByteArray trueOffElems n trueContents))
                   | 2 <- byteWidth, False <- isSigned ->
                       Right $! NamedColumn field.name defaultValidity (PrimitiveWord16 (Word16.cloneFromByteArray trueOffElems n trueContents))
                   | 4 <- byteWidth, False <- isSigned ->
                       Right $! NamedColumn field.name defaultValidity (PrimitiveWord32 (Word32.cloneFromByteArray trueOffElems n trueContents))
                   | 8 <- byteWidth, False <- isSigned ->
                       Right $! NamedColumn field.name defaultValidity (PrimitiveWord64 (Word64.cloneFromByteArray trueOffElems n trueContents))
                   | otherwise -> Left ArrowParser.UnsupportedCombinationOfBitWidthAndSign
              let !bldr' = col : bldr
              pure (bufIx + 2, bldr')
          ) (0 :: Int, []) batch.nodes footer.schema.fields
        pure (NamedColumns n (Exts.fromList (List.reverse finalBldr)))
    _ -> error "whoops, can't handle this stuff yet"

-- Returns a "true offset" and a "true contents". The true contents might be the original array,
-- or they might be a decompression of an LZ4 block.
primitiveColumnExtraction ::
     Int -- body start
  -> Int -- body end
  -> ByteArray -- contents
  -> Nat# n
  -> Int -- buffer count
  -> Int -- buffer index
  -> Field
  -> Int -- byte width
  -> RecordBatch
  -> Either ArrowParser.Error (Int, ByteArray)
primitiveColumnExtraction !bodyStart !bodyEnd !contents n !bufferCount !bufIx !field !byteWidth !batch = do
  when (bufIx + 1 >= bufferCount) $ Left ArrowParser.RanOutOfBuffers
  -- Skipping the validity bitmap for now. TODO: stop doing that.
  let buf = PM.indexPrimArray batch.buffers (bufIx + 1)
  let off = fromIntegral @Int64 @Int buf.offset
  let len = fromIntegral @Int64 @Int buf.length
  let !bufDataStartOff = off + bodyStart - 8
  when (off + len > bodyEnd) $ Left ArrowParser.BatchDataOutOfRange
  -- Invariants:
  -- * trueOff is suitably aligned
  -- * trueLen is nonnegative
  (trueOff, trueLen, trueContents) <- case batch.compression of
    Nothing -> do
      when (rem off byteWidth /= 0) $ Left (ArrowParser.MisalignedOffsetForIntBatch field.name byteWidth off)
      pure (bufDataStartOff, len, contents)
    Just (BodyCompression Lz4Frame) -> do
      when (rem bufDataStartOff 8 /= 0) $ Left ArrowParser.CompressedBufferMisaligned
      let (decompressedSize :: Int64) = LE.indexByteArray contents (quot bufDataStartOff 8)
      when (decompressedSize >= 0xFFFF_FFFF) $ Left ArrowParser.CannotDecompressToGiganticArray
      case compare decompressedSize (-1) of
        EQ -> Left ArrowParser.DisabledDecompressionNotSupported
        LT -> Left ArrowParser.NegativeDecompressedSize
        GT -> do
          let decompressedSizeI = fromIntegral decompressedSize :: Int
          decompressed <- maybe (Left ArrowParser.Lz4DecompressionFailure) Right (Lz4.Frame.decompressU decompressedSizeI (Bytes contents (bufDataStartOff + 8) (len - 8)))
          pure (0, decompressedSizeI, decompressed)
  when (trueLen < I# (Nat.demote# n) * byteWidth) $ Left (ArrowParser.ColumnByteLengthDisagreesWithBatchLength field.name trueLen (I# (Nat.demote# n)) byteWidth)
  let !trueOffElems = quot trueOff byteWidth
  Right $! (trueOffElems, trueContents)

i64ToI :: Int64 -> Int
i64ToI = fromIntegral

i32ToI :: Int32 -> Int
i32ToI = fromIntegral

roundUp8 :: Int -> Int
{-# inline roundUp8 #-}
roundUp8 x = 8 * div (7 + x) 8

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
  , buffers = C.convert buffers
  , compression = marshallCompression cmpr
  }

