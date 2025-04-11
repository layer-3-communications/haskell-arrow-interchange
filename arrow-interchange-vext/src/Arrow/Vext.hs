{-# language DataKinds #-}
{-# language UnboxedTuples #-}
{-# language MultiWayIf #-}
{-# language LambdaCase #-}
{-# language MagicHash #-}
{-# language DuplicateRecordFields #-}
{-# language PatternSynonyms #-}
{-# language TypeApplications #-}
{-# language TypeOperators #-}
{-# language OverloadedRecordDot #-}
{-# language PatternSynonyms #-}

module Arrow.Vext
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
    -- * Variable Binary Helper
  , shortTextVectorToVariableBinary
  ) where

import Arrow.Builder.Raw

import Data.Int

import Arithmetic.Nat (pattern N0#, pattern N1#)
import Arithmetic.Nat ((<=?#),(<?#))
import Arithmetic.Types (Fin(Fin),Fin#)
import Arithmetic.Types (Fin32#,Nat#,(:=:#))
import Arithmetic.Unsafe (Fin32#(Fin32#)) -- todo: get rid of this
import Arithmetic.Unsafe (Fin#(Fin#)) -- todo: get rid of this
import Control.Monad (when)
import Control.Monad.ST (runST)
import Data.Bytes.Types (ByteArrayN(ByteArrayN),Bytes(Bytes))
import Data.Foldable (foldlM)
import Data.Maybe (fromMaybe)
import Data.Maybe.Void (pattern JustVoid#)
import Data.Primitive (SmallArray,ByteArray(ByteArray),PrimArray)
import Data.Primitive.Unlifted.Array (UnliftedArray)
import Data.Text (Text)
import Data.Unlifted (Bool#, pattern True#, ShortText#)
import Data.Unlifted (PrimArray#(PrimArray#))
import Data.Word (Word32)
import GHC.Exts ((+#))
import GHC.Exts (Int8#,Int16#,Int32#,Int64#,Word64#,Word32#,Word16#,Word8#)
import GHC.Int (Int64(I64#),Int(I#))
import GHC.TypeNats (Nat, type (+))

import qualified Arithmetic.Fin as Fin
import qualified Arithmetic.Lt as Lt
import qualified Arithmetic.Lte as Lte
import qualified Arithmetic.Nat as Nat
import qualified Arithmetic.Types as Arithmetic
import qualified ArrowParser
import qualified Data.Builder.Catenable.Bytes as Catenable
import qualified Data.ByteString.Short as SBS
import qualified Data.Bytes as Bytes
import qualified Data.Bytes.Indexed
import qualified Data.List as List
import qualified Data.Primitive as PM
import qualified Data.Primitive.ByteArray.LittleEndian as LE
import qualified Data.Primitive.Contiguous as C
import qualified Data.Text as T
import qualified Data.Text.Short as TS
import qualified Data.Text.Short.Unlifted
import qualified Flatbuffers.Builder as B
import qualified GHC.Exts as Exts
import qualified GHC.TypeNats as GHC
import qualified Lz4.Frame
import qualified Vector.Bit as Bit
import qualified Vector.Int16 as Int16
import qualified Vector.Int32 as Int32
import qualified Vector.Int64 as Int64
import qualified Vector.Int8 as Int8
import qualified Vector.Unlifted as Unlifted
import qualified Vector.Unlifted.ShortText
import qualified Vector.Word16 as Word16
import qualified Vector.Word32 as Word32
import qualified Vector.Word64 as Word64
import qualified Vector.Word8 as Word8

data Column n
  = PrimitiveInt8
      !(Int8.Vector n Int8#)
  | PrimitiveInt16
      !(Int16.Vector n Int16#)
  | PrimitiveInt32
      !(Int32.Vector n Int32#)
  | PrimitiveInt64
      !(Int64.Vector n Int64#)
  | PrimitiveWord8
      !(Word8.Vector n Word8#)
  | PrimitiveWord16
      !(Word16.Vector n Word16#)
  | PrimitiveWord32
      !(Word32.Vector n Word32#)
  | PrimitiveWord64
      !(Word64.Vector n Word64#)
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

appendColumn :: Nat# n -> Nat# m -> Column n -> Column m -> Maybe (Column (n + m))
appendColumn n m (PrimitiveInt8 a) (PrimitiveInt8 b) = Just $! PrimitiveInt8 $! Int8.append n m a b
appendColumn _ _ (PrimitiveInt8 _) _ = Nothing
appendColumn n m (PrimitiveInt16 a) (PrimitiveInt16 b) = Just $! PrimitiveInt16 $! Int16.append n m a b
appendColumn _ _ (PrimitiveInt16 _) _ = Nothing
appendColumn n m (PrimitiveInt32 a) (PrimitiveInt32 b) = Just $! PrimitiveInt32 $! Int32.append n m a b
appendColumn _ _ (PrimitiveInt32 _) _ = Nothing
appendColumn n m (PrimitiveInt64 a) (PrimitiveInt64 b) = Just $! PrimitiveInt64 $! Int64.append n m a b
appendColumn _ _ (PrimitiveInt64 _) _ = Nothing
appendColumn n m (PrimitiveWord8 a) (PrimitiveWord8 b) = Just $! PrimitiveWord8 $! Word8.append n m a b
appendColumn _ _ (PrimitiveWord8 _) _ = Nothing
appendColumn n m (PrimitiveWord16 a) (PrimitiveWord16 b) = Just $! PrimitiveWord16 $! Word16.append n m a b
appendColumn _ _ (PrimitiveWord16 _) _ = Nothing
appendColumn n m (PrimitiveWord32 a) (PrimitiveWord32 b) = Just $! PrimitiveWord32 $! Word32.append n m a b
appendColumn _ _ (PrimitiveWord32 _) _ = Nothing
appendColumn n m (PrimitiveWord64 a) (PrimitiveWord64 b) = Just $! PrimitiveWord64 $! Word64.append n m a b
appendColumn _ _ (PrimitiveWord64 _) _ = Nothing
appendColumn n m (TimestampUtcSecond a) (TimestampUtcSecond b) = Just $! TimestampUtcSecond $! Int64.append n m a b
appendColumn _ _ (TimestampUtcSecond _) _ = Nothing
appendColumn n m (TimestampUtcMillisecond a) (TimestampUtcMillisecond b) = Just $! TimestampUtcMillisecond $! Int64.append n m a b
appendColumn _ _ (TimestampUtcMillisecond _) _ = Nothing
appendColumn n m (Date64 a) (Date64 b) = Just $! Date64 $! Int64.append n m a b
appendColumn _ _ (Date64 _) _ = Nothing
appendColumn n m (VariableBinaryUtf8 a) (VariableBinaryUtf8 b) = Just $! VariableBinaryUtf8 $! appendVariableBinary n m a b
appendColumn _ _ (VariableBinaryUtf8 _) _ = Nothing

showColumn :: Arithmetic.Nat# n -> Column n -> String
showColumn n (PrimitiveInt32 x) = Int32.show n x
showColumn n (PrimitiveInt64 x) = Int64.show n x
showColumn n (PrimitiveWord8 x) = Word8.show n x
showColumn n (PrimitiveWord16 x) = Word16.show n x
showColumn n (PrimitiveWord32 x) = Word32.show n x
showColumn n (PrimitiveWord64 x) = Word64.show n x

-- Only used by the test suite
showNamedColumn :: Nat# n -> NamedColumn n -> String
showNamedColumn n (NamedColumn name _ column) = "NamedColumn " ++ show name ++ " " ++ "???" ++ " " ++ showColumn n column

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
  go (PrimitiveInt8 a) (PrimitiveInt8 b) = Int8.equals n a (Int8.substitute eq b)
  go (PrimitiveInt16 a) (PrimitiveInt16 b) = Int16.equals n a (Int16.substitute eq b)
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

appendVariableBinary :: forall (m :: Nat) (n :: Nat).
     Nat# m
  -> Nat# n
  -> VariableBinary m -> VariableBinary n -> VariableBinary (m + n)
appendVariableBinary m n (VariableBinary payloadA ixsA) (VariableBinary payloadB ixsB) =
  VariableBinary
    (Data.Bytes.Indexed.append payloadA payloadB)
    (appendVarBinIxs m n (Data.Bytes.Indexed.length# payloadA) (Data.Bytes.Indexed.length# payloadB) ixsA ixsB)

appendVarBinIxs :: forall (m :: Nat) (n :: Nat) (k :: Nat) (j :: Nat).
     Nat# m
  -> Nat# n
  -> Nat# k
  -> Nat# j
  -> Int32.Vector (m + 1) (Fin32# (k + 1))
  -> Int32.Vector (n + 1) (Fin32# (j + 1))
  -> Int32.Vector ((m + n) + 1) (Fin32# ((k + j) + 1))
appendVarBinIxs m n k j ixsA ixsB = runST $ do
  dst <- Int32.initialized (Nat.succ# (Nat.plus# m n)) (Fin32# (Exts.intToInt32# 0#) :: Fin32# ((k + j) + 1))
  -- Strategy:
  -- for i in [0:m]: (inclusive upper bound)
  --   dst[i] = src[i]
  -- for i in [0:n): (exclusive upper bound)
  --   dst[i+m+1] = src[i+1] + k
  Int32.copySlice (Lte.incrementR# @1 (Lte.weakenR# @n (Lte.reflexive# @m (# #)))) (Lte.reflexive# (# #)) dst N0#
    (Int32.weakenFins (Lte.incrementR# @1 (Lte.weakenR# @j (Lte.reflexive# @k (# #)))) ixsA)
    N0# (Nat.succ# m)
  let !bound = Nat.succ# (Nat.plus# k j)
  case bound <=?# Nat.constant# @2147483648 (# #) of
    JustVoid# boundLte -> do
      Fin.ascendM_# n $ \(srcIxPred :: Fin# n) -> do
        let !srcIx = Fin.incrementR# N1# srcIxPred
        let !old = Int32.index ixsB srcIx
        let !dstIx = Fin.incrementR# N1# (Fin.incrementL# m srcIxPred) :: Fin# ((m + n) + 1)
        let !new@(Fin# newInner) = Fin.incrementL# k (Fin.nativeFrom32# old) :: Fin# (k + (j + 1))
        -- TODO: Figure out a way to use the associativity of addition instead
        -- of cheating here. I need to add stuff to natural-arithmetic.
        let !new' = Fin# newInner :: Fin# ((k + j) + 1)
        Int32.write dst dstIx (Fin.nativeTo32# boundLte new')
      Int32.unsafeFreeze dst
    _ -> errorWithoutStackTrace "appendVarBinIxs: bound got too big when appending"

shortTextVectorToVariableBinary :: forall n. Nat# n -> Unlifted.Vector n ShortText# -> VariableBinary n
shortTextVectorToVariableBinary n texts = runST $ do
  dst <- Int32.initialized (Nat.succ# n) (Exts.intToInt32# 0# )
  I# total# <- Fin.ascendM (Nat.lift n) (0 :: Int) $ \fin@(Fin ix lt) !offset@(I# offset#) -> do
    let val = Unlifted.index texts (Fin.unlift fin)
    let fin' = Fin ix (Lt.weakenR @1 lt)
    Int32.write dst (Fin.unlift fin') (Exts.intToInt32# offset#)
    pure (offset + SBS.length (TS.toShortByteString (Data.Text.Short.Unlifted.lift val)))
  Int32.write dst
    (Fin.unlift (Fin (Nat.lift n) (Lt.incrementL @n Lt.zero)))
    (Exts.intToInt32# total#)
  dst' <- Int32.unsafeFreeze dst
  let !concatenation = Vector.Unlifted.ShortText.concat n texts
  case TS.toShortByteString (Data.Text.Short.Unlifted.lift concatenation) of
    SBS.SBS arr -> Bytes.withLengthU (ByteArray arr) $ \m contents ->
      let !m# = Nat.unlift m in
      case Int32.toFins (Nat.succ# m#) (Nat.succ# n) dst' of
        Nothing -> errorWithoutStackTrace "shortTextVectorToVariableBinary: implementation mistake"
        Just fins -> pure (VariableBinary @n contents fins)

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
            PrimitiveInt8 v -> case Int8.expose v of
              PrimArray# b ->
                let b' = ByteArray b
                 in finishPrimitive b'
            PrimitiveInt16 v -> case Int16.expose v of
              PrimArray# b ->
                let b' = ByteArray b
                 in finishPrimitive b'
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
  PrimitiveInt8{} -> Int TableInt{bitWidth=8,isSigned=True}
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

emptyByteArrayN :: ByteArrayN 0
emptyByteArrayN = ByteArrayN mempty

makeEmptyNamedColumns :: Schema -> Either ArrowParser.Error NamedColumns
makeEmptyNamedColumns schema = do
  let emptyValidity = Bit.empty
  finalBldr <- C.foldlM'
    (\bldr field -> case field.type_ of
      Timestamp TableTimestamp{unit=Second} -> do
        let !col = NamedColumn field.name emptyValidity (TimestampUtcSecond Int64.empty)
        let !bldr' = col : bldr
        pure bldr'
      Utf8 -> do
        let !col = NamedColumn field.name emptyValidity (VariableBinaryUtf8 (VariableBinary emptyByteArrayN (Int32.replicate N1# (Fin32# (Exts.intToInt32# 0#)))))
        let !bldr' = col : bldr
        pure bldr'
      Int TableInt{bitWidth,isSigned} -> do
        !col <-
          if | 8 <- bitWidth, True <- isSigned ->
                 Right $! NamedColumn field.name emptyValidity (PrimitiveInt8 Int8.empty)
             | 32 <- bitWidth, True <- isSigned ->
                 Right $! NamedColumn field.name emptyValidity (PrimitiveInt32 Int32.empty)
             | 64 <- bitWidth, True <- isSigned ->
                 Right $! NamedColumn field.name emptyValidity (PrimitiveInt64 Int64.empty)
             | 8 <- bitWidth, False <- isSigned ->
                 Right $! NamedColumn field.name emptyValidity (PrimitiveWord8 Word8.empty)
             | 16 <- bitWidth, False <- isSigned ->
                 Right $! NamedColumn field.name emptyValidity (PrimitiveWord16 Word16.empty)
             | 32 <- bitWidth, False <- isSigned ->
                 Right $! NamedColumn field.name emptyValidity (PrimitiveWord32 Word32.empty)
             | 64 <- bitWidth, False <- isSigned ->
                 Right $! NamedColumn field.name emptyValidity (PrimitiveWord64 Word64.empty)
             | otherwise -> Left ArrowParser.UnsupportedCombinationOfBitWidthAndSign
        let !bldr' = col : bldr
        pure bldr'
    ) [] schema.fields
  pure (NamedColumns Nat.N0# (Exts.fromList (List.reverse finalBldr)))

-- | Decode a single batch of records
decode :: ByteArray -> Either ArrowParser.Error NamedColumns
decode contents = do
  footer <- ArrowParser.decodeFooterFromArrowContents contents
  let batches = footer.recordBatches
  case footer.schema.endianness of
    0 -> pure ()
    _ -> Left ArrowParser.BigEndianBuffersNotSupported
  case PM.sizeofPrimArray batches of
    0 -> makeEmptyNamedColumns footer.schema
    1 -> do
      let !block0 = PM.indexPrimArray batches 0
      handleOneBlock contents footer block0
    _ -> handleManyBlocks contents footer batches

-- Both of these values are relative to the beginning of the record batch. They
-- are not absolute positions in the file.
data BodyBounds = BodyBounds
  { bodyStart :: !Int 
  , bodyEnd :: !Int 
  }

computeBodyBounds :: Block -> BodyBounds
computeBodyBounds block =
  let !bodyStart = roundUp8 (i64ToI block.offset + 8 + i32ToI block.metaDataLength)
      !bodyEnd = bodyStart + i64ToI block.bodyLength
   in BodyBounds{bodyStart,bodyEnd}

handleManyBlocks :: ByteArray -> Footer -> PrimArray Block -> Either ArrowParser.Error NamedColumns
handleManyBlocks !contents footer blocks = do
  pairs :: SmallArray (Block,RecordBatch) <- C.traverse
    (\block -> do
      metadata <- ArrowParser.extractMetadata contents block
      case metadata.header of
        MessageHeaderRecordBatch batch -> pure (block, batch)
        _ -> Left ArrowParser.OnlyRecordBatchesAreSupported
    ) blocks
  -- totalElemLen <- C.foldlM'
  --   (\acc batch -> do
  --     let !len = batch.length
  --     when (len < 0) (Left ArrowParser.NegativeBatchLength)
  --     pure (acc + fromIntegral len)
  --   ) (0 :: Int) batches
  namedColsList <- traverse (\(block,batch) -> handleOneBatch contents footer block batch) pairs
  case Exts.toList namedColsList of
    [] -> makeEmptyNamedColumns footer.schema
    nc0 : ncs -> do
      foldlM
        (\acc nc -> case acc of
          NamedColumns accSize accColumns -> case nc of
            NamedColumns ncSize ncColumns -> do
              let resultColumns = C.zipWith
                    (\NamedColumn{name,mask=accMask,column=accCol} NamedColumn{mask=ncMask,column=ncCol} -> NamedColumn
                      { name
                      , mask = Bit.append accSize ncSize accMask ncMask
                      , column = fromMaybe (error "handleManyBlocks: column type mismatch") (appendColumn accSize ncSize accCol ncCol)
                      }
                    ) accColumns ncColumns
              pure (NamedColumns (Nat.plus# accSize ncSize) resultColumns)
        ) nc0 ncs

handleOneBlock :: ByteArray -> Footer -> Block -> Either ArrowParser.Error NamedColumns
handleOneBlock !contents footer block = do
  -- TODO: check that body end is actually in bounds
  metadata <- ArrowParser.extractMetadata contents block
  batch <- case metadata.header of
    MessageHeaderRecordBatch batch -> pure batch
    _ -> Left ArrowParser.OnlyRecordBatchesAreSupported
  handleOneBatch contents footer block batch

handleOneBatch :: ByteArray -> Footer -> Block -> RecordBatch -> Either ArrowParser.Error NamedColumns
handleOneBatch !contents footer block batch = do
  let bodyBounds@BodyBounds{bodyStart,bodyEnd} = computeBodyBounds block
  -- Note: the batch length is the number of elements, not bytes
  when (batch.length < 0) (Left ArrowParser.NegativeBatchLength)
  when (PM.sizeofPrimArray batch.nodes /= PM.sizeofSmallArray footer.schema.fields) (Left ArrowParser.SchemaFieldCountDoesNotMatchNodeCount)
  let !bufferCount = PM.sizeofPrimArray batch.buffers
  -- Left (ArrowParser.Placeholder (show metadata))
  Nat.with# (case batch.length of {I64# i -> Exts.int64ToInt# i}) $ \n -> do
    let defaultValidity = Bit.replicate n True#
    (_, finalBldr) <- C.foldlZipWithM'
      (\(bufIx, bldr) _ field -> case field.type_ of
        -- We do not need to use node unless we support Arrow lists
        -- Currently ignoring the time zone. Fix this.
        Timestamp TableTimestamp{unit=Second} -> do
          (trueOffElems, trueContents) <- primitiveColumnExtraction bodyBounds contents n bufferCount bufIx field 8 batch
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
          (trueOffElems, trueContents) <- primitiveColumnExtraction bodyBounds contents n bufferCount bufIx field byteWidth batch
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
        Utf8 -> do
          when (bufIx + 2 >= bufferCount) $ Left ArrowParser.RanOutOfBuffers
          -- Skipping the validity bitmap for now. TODO: stop doing that.
          let bufOffsets = PM.indexPrimArray batch.buffers (bufIx + 1)
          let bufData = PM.indexPrimArray batch.buffers (bufIx + 2)
          let dataStart = fromIntegral @Int64 @Int bufData.offset + (bodyStart - 8)
          let dataLen = fromIntegral @Int64 @Int bufData.length
          when (dataStart + dataLen > bodyEnd) $ Left ArrowParser.BatchDataOutOfRange
          let offsetArrayStart = fromIntegral @Int64 @Int bufOffsets.offset + (bodyStart - 8)
          let offsetArrayLen = fromIntegral @Int64 @Int bufOffsets.length
          when (offsetArrayStart + offsetArrayLen > bodyEnd) $ Left ArrowParser.BatchDataOutOfRange
          (dataOffTrue, dataLenTrue, dataContents) <- decompressBufferIfNeeded batch.compression contents bodyBounds bufData
          (offsOffTrue, offsLenTrue, offsContents) <- decompressBufferIfNeeded batch.compression contents bodyBounds bufOffsets
          when (rem offsOffTrue 4 /= 0) $ Left (ArrowParser.MisalignedOffsetForIntBatch 4 offsOffTrue)
          when (offsLenTrue < (1 + I# (Nat.demote# n)) * 4) $ Left (ArrowParser.ColumnByteLengthDisagreesWithBatchLength field.name offsLenTrue (1 + (I# (Nat.demote# n))) 4)
          let dataContents' = if dataOffTrue == 0 && PM.sizeofByteArray dataContents == dataLenTrue
                then dataContents
                else Bytes.toByteArrayClone (Bytes dataContents dataOffTrue dataLenTrue)
          Bytes.withLengthU dataContents' $ \m dataContentsLenIxed -> case Int32.toFins (Nat.succ# (Nat.unlift m)) (Nat.succ# n) (Int32.cloneFromByteArray (quot offsOffTrue 4) (Nat.succ# n) offsContents) of
            Nothing -> Left ArrowParser.VariableBinaryIndicesBad
            Just ixs -> do
              let !col = NamedColumn field.name defaultValidity (VariableBinaryUtf8 (VariableBinary dataContentsLenIxed ixs))
              let !bldr' = col : bldr
              pure (bufIx + 3, bldr')
        ty -> Left (ArrowParser.CannotUnmarshalColumnWithType ty)
      ) (0 :: Int, []) batch.nodes footer.schema.fields
    pure (NamedColumns n (Exts.fromList (List.reverse finalBldr)))


-- Returns a "true offset" and a "true contents". The true contents might be the original array,
-- or they might be a decompression of an LZ4 block.
primitiveColumnExtraction ::
     BodyBounds
  -> ByteArray -- contents
  -> Nat# n
  -> Int -- buffer count
  -> Int -- buffer index
  -> Field
  -> Int -- byte width
  -> RecordBatch
  -> Either ArrowParser.Error (Int, ByteArray)
primitiveColumnExtraction bodyBounds@BodyBounds{bodyEnd} !contents n !bufferCount !bufIx !field !byteWidth !batch = do
  when (bufIx + 1 >= bufferCount) $ Left ArrowParser.RanOutOfBuffers
  -- Skipping the validity bitmap for now. TODO: stop doing that.
  let buf = PM.indexPrimArray batch.buffers (bufIx + 1)
  let off = fromIntegral @Int64 @Int buf.offset
  let len = fromIntegral @Int64 @Int buf.length
  when (off + len > bodyEnd) $ Left ArrowParser.BatchDataOutOfRange
  -- Invariants:
  -- * trueOff is suitably aligned
  -- * trueLen is nonnegative
  (trueOff, trueLen, trueContents) <- decompressBufferIfNeeded batch.compression contents bodyBounds buf
  when (rem trueOff byteWidth /= 0) $ Left (ArrowParser.MisalignedOffsetForIntBatch byteWidth trueOff)
  when (trueLen < I# (Nat.demote# n) * byteWidth) $ Left (ArrowParser.ColumnByteLengthDisagreesWithBatchLength field.name trueLen (I# (Nat.demote# n)) byteWidth)
  let !trueOffElems = quot trueOff byteWidth
  Right $! (trueOffElems, trueContents)

-- Return values: true offset, true length, buffer that offset and len apply to.
-- Return the true offset to the beginning of the decompressed buffer, in bytes.
decompressBufferIfNeeded :: Maybe BodyCompression -> ByteArray -> BodyBounds -> Buffer -> Either ArrowParser.Error (Int, Int, ByteArray)
decompressBufferIfNeeded mc !contents BodyBounds{bodyStart,bodyEnd} buf = do
  let off = fromIntegral @Int64 @Int buf.offset
  let len = fromIntegral @Int64 @Int buf.length
  let !bufDataStartOff = off + bodyStart - 8
  when (off + len > bodyEnd) $ Left ArrowParser.BatchDataOutOfRange
  case mc of
    Nothing -> do
      pure (bufDataStartOff, len, contents)
    Just (BodyCompression Lz4Frame) -> do
      when (rem bufDataStartOff 8 /= 0) $ Left ArrowParser.CompressedBufferMisaligned
      let (decompressedSize :: Int64) = LE.indexByteArray contents (quot bufDataStartOff 8)
      when (decompressedSize >= 0xFFFF_FFFF) $ Left (ArrowParser.CannotDecompressToGiganticArray decompressedSize)
      case compare decompressedSize (-1) of
        EQ -> Left ArrowParser.DisabledDecompressionNotSupported
        LT -> Left ArrowParser.NegativeDecompressedSize
        GT -> do
          let decompressedSizeI = fromIntegral decompressedSize :: Int
          decompressed <- maybe (Left ArrowParser.Lz4DecompressionFailure) Right (Lz4.Frame.decompressU decompressedSizeI (Bytes contents (bufDataStartOff + 8) (len - 8)))
          pure (0, decompressedSizeI, decompressed)

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

