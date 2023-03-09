{-# language DataKinds #-}
{-# language LambdaCase #-}
{-# language MagicHash #-}
{-# language DuplicateRecordFields #-}
{-# language TypeApplications #-}
{-# language OverloadedRecordDot #-}

module ArrowBuilder
  ( Column(..)
  , NamedColumn(..)
  , encode
  ) where

import ArrowFile
import ArrowSchema
import ArrowMessage

import Data.Word
import Data.Int

import Data.Text.Short (ShortText)
import Data.Primitive (SmallArray,ByteArray)
import Data.Text (Text)
import Arithmetic.Types (Fin(Fin))

import qualified Arithmetic.Fin as Fin
import qualified Arithmetic.Types as Arithmetic
import qualified Arithmetic.Nat as Nat
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
import qualified Vector.Int64.Internal as Int64
import qualified Vector.Bool.Internal as Bool

data Column n
  = PrimitiveWord8 !(Word8.Vector n)
  | PrimitiveWord16 !(Word16.Vector n)
  | PrimitiveWord32 !(Word32.Vector n)
  | PrimitiveWord64 !(Word64.Vector n)
  | PrimitiveInt64 !(Int64.Vector n)
  -- x | VariableBinaryShortText !(Unlifted.Vector ('Known n) ShortText#)

data NamedColumn n = NamedColumn
  { name :: !Text
  , mask :: !(Bool.Vector n)
  , column :: !(Column n)
  }

data BufferWithPayload = BufferWithPayload
  { buffer :: !Buffer
  , payload :: !ByteArray
  , padding :: !Int -- must be [0,63], this is just how many null bytes we need to tack onto the end
  }

computePadding64 :: Int -> Int
computePadding64 !n = (if n == 0 then 0 else 64 - mod n 64) :: Int

makeBuffers :: Arithmetic.Nat n -> SmallArray (NamedColumn n) -> [BufferWithPayload]
makeBuffers !n !cols = go 0 [] 0
  where
  -- 64 must divide pos evenly
  go :: Int -> [BufferWithPayload] -> Int -> [BufferWithPayload]
  go !ix !acc !pos = if ix < PM.sizeofSmallArray cols
    then
      let NamedColumn{mask,column} = PM.indexSmallArray cols ix
          finishPrimitive !exposed = 
            let rawSize = PM.sizeofByteArray exposed
                exposedMask = Bool.expose mask
                rawMaskSize = PM.sizeofByteArray exposedMask
                elementPadding = computePadding64 rawSize
                maskPadding = computePadding64 rawMaskSize
                acc' = consPrimitive pos rawSize rawMaskSize elementPadding maskPadding exposed exposedMask acc
             in go (ix + 1) acc' (pos + rawSize + elementPadding + rawMaskSize + maskPadding)
       in case column of
            PrimitiveWord8 v -> finishPrimitive (Word8.expose v)
            PrimitiveWord16 v -> finishPrimitive (Word16.expose v)
            PrimitiveWord32 v -> finishPrimitive (Word32.expose v)
            PrimitiveWord64 v -> finishPrimitive (Word64.expose v)
            PrimitiveInt64 v -> finishPrimitive (Int64.expose v)
    else List.reverse acc

consPrimitive :: Int -> Int -> Int -> Int -> Int -> ByteArray -> ByteArray -> [BufferWithPayload] -> [BufferWithPayload]
consPrimitive !pos !rawSize !rawMaskSize !elementPadding !maskPadding !exposed !exposedMask !acc =
    BufferWithPayload {buffer=Buffer{offset=fromIntegral (pos + rawMaskSize + maskPadding),length=fromIntegral @Int @Int64 rawSize},payload=exposed,padding=elementPadding}
  : BufferWithPayload {buffer=Buffer{offset=fromIntegral pos,length=fromIntegral @Int @Int64 rawMaskSize},payload=exposedMask,padding=maskPadding}
  : acc

columnToType :: Column n -> Type
columnToType = \case
  PrimitiveWord8{} -> Int TableInt{bitWidth=8,isSigned=False}
  PrimitiveWord16{} -> Int TableInt{bitWidth=16,isSigned=False}
  PrimitiveWord32{} -> Int TableInt{bitWidth=32,isSigned=False}
  PrimitiveWord64{} -> Int TableInt{bitWidth=64,isSigned=False}
  PrimitiveInt64{} -> Int TableInt{bitWidth=64,isSigned=True}

namedColumnToField :: NamedColumn n -> Field
namedColumnToField NamedColumn{name,column} = Field
  { name = name
  , nullable = True
  , type_ = columnToType column
  , dictionary = ()
  , children = mempty
  }

makeSchema :: SmallArray (NamedColumn n) -> Schema
makeSchema !namedColumns = Schema
  { endianness = 1
  , fields = fmap namedColumnToField namedColumns
  }

encode :: Arithmetic.Nat n -> SmallArray (NamedColumn n) -> Catenable.Builder
encode !n !namedColumns =
  let partA = 
        asciiArrow1 -- len 6
        <>
        Catenable.bytes (Bytes.replicate 2 0x00) -- len 2
        <>
        continuation -- len 4
        <>
        Catenable.byteArray (Bounded.run (Nat.constant @4) (Bounded.word32LE (fromIntegral (encodedSchemaMessageLength + encodedSchemaMessagePadding)))) -- len 4
        <>
        Catenable.bytes encodedSchemaMessage <> Catenable.bytes (Bytes.replicate encodedSchemaMessagePadding 0x00) -- 8 divides this evenly
      body = foldMap
        (\BufferWithPayload{payload,padding} ->
          Catenable.bytes (Bytes.fromByteArray payload)
          <> 
          Catenable.bytes (Bytes.replicate padding 0x00)
        ) buffersWithPayload
      bodyLength = Catenable.length body
      recordBatch = makeRecordBatch n buffersWithPayload namedColumns
      encodedRecordBatch = B.encode (encodeMessage (Message{header=MessageHeaderRecordBatch recordBatch,bodyLength=fromIntegral bodyLength}))
      encodedRecordBatchLength = Catenable.length encodedRecordBatch
      partALen = Catenable.length partA -- 8 divides this length evenly
      -- We can figure out the length of partB before we build it.
      -- It's just the length of the encoded record batch, plus 4 bytes
      -- for the continuation, plus 4 more bytes for the padded record batch length
      partBLen = 8 + Catenable.length encodedRecordBatch -- unknown length, we must pad part B
      partABLen = partALen + partBLen
      partBPadding = computePadding64 partABLen
      partB =
        continuation -- len 4
        <>
        Catenable.byteArray (Bounded.run (Nat.constant @4) (Bounded.word32LE (fromIntegral (encodedRecordBatchLength + partBPadding)))) -- len 4
        <>
        encodedRecordBatch -- len unknown
      partBLenWithPadding = partBLen + partBPadding
      encodedFooter = B.encode (encodeFooter (makeFooter partALen partBLenWithPadding bodyLength schema))
      encodedFooterLength = Catenable.length encodedFooter
   in partA
      <>
      partB
      <>
      Catenable.bytes (Bytes.replicate partBPadding 0x00) -- after this, we are 64-byte aligned
      <>
      body
      <>
      continuation
      <>
      eos
      <>
      encodedFooter 
      <>
      Catenable.bytes (Bytes.fromByteArray (Bounded.run (Nat.constant @4) (Bounded.word32LE (fromIntegral encodedFooterLength)))) -- len 4
      <>
      asciiArrow1
  where
  schema = makeSchema namedColumns
  schemaMessage = Message{header=MessageHeaderSchema schema,bodyLength=0}
  encodedSchemaMessage = Chunks.concat (Catenable.run (B.encode (encodeMessage schemaMessage)))
  encodedSchemaMessageLength = Bytes.length encodedSchemaMessage
  encodedSchemaMessagePadding = 8 - mod (Bytes.length encodedSchemaMessage) 8
  buffersWithPayload = Exts.fromList (makeBuffers n namedColumns) :: SmallArray BufferWithPayload

continuation :: Catenable.Builder
continuation = Catenable.bytes (Bytes.replicate 4 0xFF)

eos :: Catenable.Builder
eos = Catenable.bytes (Bytes.replicate 4 0x00)

asciiArrow1 :: Catenable.Builder
asciiArrow1 = Catenable.bytes (Exts.fromList [0x41 :: Word8,0x52,0x52,0x4f,0x57,0x31])

-- TODO: Replace this. It is slow and awful.
naivePopCount :: Arithmetic.Nat n -> Bool.Vector n -> Int
naivePopCount !n !v = Fin.ascend' n 0 $ \(Fin ix lt) acc -> acc + (if Bool.index lt v ix then 1 else 0)

makeRecordBatch :: Arithmetic.Nat n -> SmallArray BufferWithPayload -> SmallArray (NamedColumn n) -> RecordBatch
makeRecordBatch !n buffersWithPayload !cols = RecordBatch
  { length = fromIntegral (Nat.demote n)
  , nodes = C.map'
      (\NamedColumn{mask} -> FieldNode
        { length=fromIntegral (Nat.demote n)
        , nullCount=fromIntegral @Int @Int64 (Nat.demote n - naivePopCount n mask)
        }
      ) cols
  , buffers = fmap (\x -> x.buffer) buffersWithPayload
  }

makeFooter :: Int -> Int -> Int -> Schema -> Footer
makeFooter !offset !metaDataLength !bodyLength !schema = Footer
  { schema = schema
  , dictionaries = mempty
  , recordBatches = C.singleton Block
    { offset = fromIntegral @Int @Int64 offset
    , metaDataLength = fromIntegral @Int @Int32 metaDataLength
    , bodyLength = fromIntegral @Int @Int64 bodyLength
    }
  }
