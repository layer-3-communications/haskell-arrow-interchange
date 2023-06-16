{-# language DataKinds #-}
{-# language LambdaCase #-}
{-# language MagicHash #-}
{-# language DuplicateRecordFields #-}
{-# language TypeApplications #-}
{-# language TypeOperators #-}
{-# language OverloadedRecordDot #-}

module ArrowBuilderNoColumn
  ( BufferWithPayload(..)
  , Compression(..)
  , makeFooter
  , encodePreludeAndSchema
  , encodeFooterAndEpilogue
  , consPrimitive
  , consVariableBinary
  , asciiArrow1
  , eos
  , continuation
  , computePadding64
  , makeFooter
  , encodeBuffersWithPayload
  , encodePartB
  , marshallCompression
  ) where

import ArrowFile
import ArrowSchema
import ArrowMessage

import Data.Word
import Data.Int

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

data BufferWithPayload = BufferWithPayload
  { buffer :: !Buffer
  , payload :: !ByteArray
  , padding :: !Int -- must be [0,63], this is just how many null bytes we need to tack onto the end
  }

data Compression
  = None
  | Lz4

computePadding64 :: Int -> Int
computePadding64 !n = (if n == 0 then 0 else 64 - mod n 64) :: Int

-- We cons these backwards since the list gets reversed at the end.
consPrimitive :: Int -> Int -> Int -> Int -> Int -> ByteArray -> ByteArray -> [BufferWithPayload] -> [BufferWithPayload]
consPrimitive !pos !rawSize !rawMaskSize !elementPadding !maskPadding !exposed !exposedMask !acc =
    BufferWithPayload
      { buffer=Buffer{offset=fromIntegral (pos + rawMaskSize + maskPadding)
      , length=fromIntegral @Int @Int64 rawSize},payload=exposed,padding=elementPadding
      }
  : BufferWithPayload
      { buffer=Buffer{offset=fromIntegral pos,length=fromIntegral @Int @Int64 rawMaskSize}
      , payload=exposedMask,padding=maskPadding
      }
  : acc

consVariableBinary :: Int -> Int -> Int -> Int -> Int -> Int -> Int -> ByteArray -> ByteArray -> ByteArray -> [BufferWithPayload] -> [BufferWithPayload]
consVariableBinary !pos !rawSize !rawMaskSize !rawOffsetsSize !elementPadding !maskPadding !offsetsPadding !exposed !exposedMask !exposedOffsets !acc =
    BufferWithPayload
      { buffer=Buffer
        { offset=fromIntegral (pos + rawMaskSize + maskPadding + rawOffsetsSize + offsetsPadding)
        , length=fromIntegral @Int @Int64 rawSize
        }
      , payload=exposed
      , padding=elementPadding
      }
  : BufferWithPayload {buffer=Buffer{offset=fromIntegral (pos + rawMaskSize + maskPadding),length=fromIntegral @Int @Int64 rawOffsetsSize},payload=exposedOffsets,padding=offsetsPadding}
  : BufferWithPayload {buffer=Buffer{offset=fromIntegral pos,length=fromIntegral @Int @Int64 rawMaskSize},payload=exposedMask,padding=maskPadding}
  : acc

-- | Encodes the schema and the header that comes before it.
-- Length is always a multiple of 64 bytes.
encodePreludeAndSchema :: Schema -> Catenable.Builder
encodePreludeAndSchema !schema =
  asciiArrow1 -- len 6
  <>
  Catenable.bytes (Bytes.replicate 2 0x00) -- len 2
  <>
  continuation -- len 4
  <>
  Catenable.byteArray (Bounded.run (Nat.constant @4) (Bounded.word32LE (fromIntegral (encodedSchemaMessageLength + encodedSchemaMessagePadding)))) -- len 4
  <>
  Catenable.bytes encodedSchemaMessage
  <>
  Catenable.bytes (Bytes.replicate encodedSchemaMessagePadding 0x00)
  where
  schemaMessage = Message{header=MessageHeaderSchema schema,bodyLength=0}
  encodedSchemaMessage = Chunks.concat (Catenable.run (B.encode (encodeMessage schemaMessage)))
  encodedSchemaMessageLength = Bytes.length encodedSchemaMessage
  encodedSchemaMessagePadding = computePadding64 encodedSchemaMessageLength

-- | Encode the footer and the epilogue.
encodeFooterAndEpilogue :: Schema -> SmallArray Block -> Catenable.Builder
encodeFooterAndEpilogue !schema !blocks = 
  let encodedFooter = B.encode (encodeFooter (makeFooter blocks schema))
      encodedFooterLength = Catenable.length encodedFooter
   in continuation
      <>
      eos
      <>
      encodedFooter 
      <>
      Catenable.bytes (Bytes.fromByteArray (Bounded.run (Nat.constant @4) (Bounded.word32LE (fromIntegral encodedFooterLength)))) -- len 4
      <>
      asciiArrow1
 
continuation :: Catenable.Builder
continuation = Catenable.bytes (Bytes.replicate 4 0xFF)

eos :: Catenable.Builder
eos = Catenable.bytes (Bytes.replicate 4 0x00)

asciiArrow1 :: Catenable.Builder
asciiArrow1 = Catenable.bytes (Exts.fromList [0x41 :: Word8,0x52,0x52,0x4f,0x57,0x31])

makeFooter :: SmallArray Block -> Schema -> Footer
makeFooter !blocks !schema = Footer
  { schema = schema
  , dictionaries = mempty
  , recordBatches = blocks
  }

encodeBuffersWithPayload :: SmallArray BufferWithPayload -> Catenable.Builder
encodeBuffersWithPayload = foldMap
  (\BufferWithPayload{payload,padding} ->
    Catenable.bytes (Bytes.fromByteArray payload)
    <> 
    Catenable.bytes (Bytes.replicate padding 0x00)
  )

-- Argument: the encoded record batch;
-- Output, the encoded record batch prefixed by
-- * the continuation byte sequence
-- * the length
-- And suffixed by
-- * padding bytes (NUL) to 64-byte pad the length to a multiple of 64
encodePartB :: Catenable.Builder -> Catenable.Builder
encodePartB encodedRecordBatch =
  let encodedRecordBatchLength = Catenable.length encodedRecordBatch
      partBLen = 8 + encodedRecordBatchLength
      partBPadding = computePadding64 partBLen
      partBPaddedLength = partBLen + partBPadding
   in continuation -- len 4
      <>
      Catenable.byteArray (Bounded.run (Nat.constant @4) (Bounded.word32LE (fromIntegral (partBPaddedLength - 8)))) -- len 4
      <>
      encodedRecordBatch -- len unknown
      <>
      Catenable.bytes (Bytes.replicate partBPadding 0x00) -- after this, we are 64-byte aligned

marshallCompression :: Compression -> Maybe BodyCompression
marshallCompression = \case
  None -> Nothing
  Lz4 -> Just BodyCompression{codec=Lz4Frame}
