{-# language DataKinds #-}
{-# language DuplicateRecordFields #-}
{-# language LambdaCase #-}
{-# language MagicHash #-}
{-# language OverloadedRecordDot #-}
{-# language PatternSynonyms #-}
{-# language TypeApplications #-}
{-# language TypeOperators #-}

module Arrow.Builder.Raw
  ( -- * Types
    Payloads(..)
  , Compression(..)
    -- * Flatbuffer Types And Encoders
    -- ** File
  , Footer(..)
  , Block(..)
  , encodeFooter
  , parseFooter
    -- ** Schema
  , Field(..)
  , Type(..)
  , Schema(..)
  , TableInt(..)
  , TableFixedSizeBinary(..)
  , TableFixedSizeList(..)
  , TableTimestamp(..)
  , TableDate(..)
  , Buffer(..)
  , TimeUnit(..)
  , DateUnit(..)
  , DictionaryEncoding(..)
  , pattern Second
  , pattern Millisecond
  , pattern Microsecond
  , pattern Nanosecond
  , pattern Day
  , pattern DateMillisecond
  , encodeSchema
    -- ** Message
  , Message(..)
  , FieldNode(..)
  , RecordBatch(..)
  , MessageHeader(..)
  , CompressionType(..)
  , BodyCompression(..)
  , encodeMessage
  , parseMessage
  , encodeRecordBatch

    -- * Encoding Helpers
  , makeFooter
  , encodePreludeAndSchema
  , encodeFooterAndEpilogue
  , payloadsToArray
  , consPrimitive
  , consValidityMask
  , consVariableBinary
  , consListMetadata
  , asciiArrow1
  , eos
  , continuation
  , computePadding64
  , encodePayloadsUncompressed
  , encodePayloadsLz4
  , encodePartB
  , marshallCompression
  ) where

import ArrowFile
import ArrowSchema
import ArrowMessage

import Data.Word

import Data.Primitive (SmallArray,ByteArray,PrimArray)
import Data.Primitive.Unlifted.Array (UnliftedArray, sizeofUnliftedArray)
import Data.Primitive.Unlifted.Array (writeUnliftedArray, unsafeFreezeUnliftedArray, newUnliftedArray)
import Control.Monad.ST.Run (runUnliftedArrayST)

import qualified Arithmetic.Nat as Nat
import qualified Lz4.Frame as Lz4
import qualified Data.Bytes.Encode.LittleEndian as LittleEndian
import qualified Data.Primitive.Contiguous as C
import qualified Data.Primitive as PM
import qualified GHC.Exts as Exts
import qualified Data.Bytes.Builder.Bounded as Bounded
import qualified Data.Bytes as Bytes
import qualified Data.Bytes.Chunks as Chunks
import qualified Flatbuffers.Builder as B
import qualified Data.Builder.Catenable.Bytes as Catenable
import qualified Data.Primitive.Contiguous as Contiguous

data Payloads
  = PayloadsCons !ByteArray !Payloads
  | PayloadsNil

data Compression
  = None
  | Lz4

computePadding64 :: Int -> Int
{-# inline computePadding64 #-}
computePadding64 !n = (if mod n 64 == 0 then 0 else 64 - mod n 64) :: Int

computePadding8 :: Int -> Int
{-# inline computePadding8 #-}
computePadding8 !n = (if mod n 8 == 0 then 0 else 8 - mod n 8) :: Int

-- Used for validity masks for structs
consValidityMask :: ByteArray -> Payloads -> Payloads
consValidityMask !exposedMask !acc =
    PayloadsCons exposedMask
  $ acc

-- We cons these backwards since the list gets reversed at the end.
consPrimitive :: ByteArray -> ByteArray -> Payloads -> Payloads
consPrimitive !exposed !exposedMask !acc =
    PayloadsCons exposed
  $ PayloadsCons exposedMask
  $ acc

consVariableBinary :: ByteArray -> ByteArray -> ByteArray -> Payloads -> Payloads
consVariableBinary !exposed !exposedMask !exposedOffsets !acc =
    PayloadsCons exposed
  $ PayloadsCons exposedOffsets
  $ PayloadsCons exposedMask
  $ acc

consListMetadata :: ByteArray -> ByteArray -> Payloads -> Payloads
consListMetadata !exposedMask !exposedOffsets !acc =
    PayloadsCons exposedOffsets
  $ PayloadsCons exposedMask
  $ acc

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
  , recordBatches = Contiguous.convert blocks
  }

makeFooterPrim :: PrimArray Block -> Schema -> Footer
makeFooterPrim !blocks !schema = Footer
  { schema = schema
  , dictionaries = mempty
  , recordBatches = blocks
  }

-- Concatenates all the payloads and computes the positions for
-- the Arrow Buffers. The array of buffers has the same length
-- as the array of payloads.
encodePayloadsUncompressed ::
     UnliftedArray ByteArray
  -> ( Catenable.Builder
     , SmallArray Buffer
     )
encodePayloadsUncompressed payloads =
  let EncodePayloadState{builder=builderZ,buffers=buffersZ} = C.foldl'
        (\EncodePayloadState{position,builder,buffers} payload ->
          let !unpaddedLen = PM.sizeofByteArray payload
              !padding = computePadding64 unpaddedLen
              builder' =
                builder
                <>
                Catenable.bytes (Bytes.fromByteArray payload)
                <>
                Catenable.bytes (Bytes.replicate padding 0x00)
              !paddedLen = padding + unpaddedLen
              buffers' =
                Buffer{offset=fromIntegral position, length=fromIntegral unpaddedLen} : buffers
              position' = position + paddedLen
           in EncodePayloadState
                { position = position'
                , builder = builder'
                , buffers = buffers'
                }
        ) EncodePayloadState{builder=mempty,position=0,buffers=[]} payloads
   in (builderZ, C.unsafeFromListReverseN (sizeofUnliftedArray payloads) buffersZ)

-- Note: Payloads under 64 bytes are never compressed. LZ4 frames have 15 bytes of
-- overhead, so it is unlikely that the compression would be beneficial. An
-- LZ4 frame cannot have compressed blocks whose uncompressed contents are
-- larger than 4MiB, so we also disable compression on these large payloads.
encodePayloadsLz4 ::
     UnliftedArray ByteArray
  -> ( Catenable.Builder
     , SmallArray Buffer
     )
encodePayloadsLz4 payloads =
  let EncodePayloadState{builder=builderZ,buffers=buffersZ} = C.foldl'
        (\EncodePayloadState{position,builder,buffers} payload ->
          let payloadSize = PM.sizeofByteArray payload
           in if payloadSize <= 64
                then
                  -- We pad to 8-byte alignment rather than 64-byte alignment.
                  -- Anyone working with compressed Arrow buffers should not expecting
                  -- to be able to memory map the contents anyway.
                  let padding = computePadding8 (PM.sizeofByteArray payload)
                      builder' =
                        builder
                        <>
                        Catenable.bytes (LittleEndian.int64 (-1))
                        <>
                        Catenable.bytes (Bytes.fromByteArray payload)
                        <>
                        Catenable.bytes (Bytes.replicate padding 0x00)
                      unpaddedLen = 8 + PM.sizeofByteArray payload
                      paddedLen = padding + unpaddedLen
                      buffers' =
                        Buffer{offset=fromIntegral position, length=fromIntegral unpaddedLen} : buffers
                      position' = position + paddedLen
                   in EncodePayloadState
                        { position = position'
                        , builder = builder'
                        , buffers = buffers'
                        }
                else
                  let compressed = Lz4.compressHighlyU 9 (Bytes.fromByteArray payload)
                      padding = computePadding8 (PM.sizeofByteArray compressed)
                      builder' =
                        builder
                        <>
                        Catenable.bytes (LittleEndian.int64 (fromIntegral payloadSize))
                        <>
                        Catenable.bytes (Bytes.fromByteArray compressed)
                        <>
                        Catenable.bytes (Bytes.replicate padding 0x00)
                      unpaddedLen = 8 + PM.sizeofByteArray compressed
                      paddedLen = unpaddedLen + padding
                      buffers' =
                        Buffer{offset=fromIntegral position, length=fromIntegral unpaddedLen} : buffers
                      position' = position + paddedLen
                   in EncodePayloadState
                        { position = position'
                        , builder = builder'
                        , buffers = buffers'
                        }
        ) EncodePayloadState{builder=mempty,position=0,buffers=[]} payloads
   in (builderZ, C.unsafeFromListReverseN (sizeofUnliftedArray payloads) buffersZ)

data EncodePayloadState = EncodePayloadState
  { builder :: !Catenable.Builder
  , position :: !Int
  , buffers :: ![Buffer]
    -- ^ This is built in reverse order
  }

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

payloadsToArray :: Payloads -> UnliftedArray ByteArray
{-# noinline payloadsToArray #-}
payloadsToArray p0 = step1 p0 0
  where
  step1 :: Payloads -> Int -> UnliftedArray ByteArray
  step1 PayloadsNil !n = runUnliftedArrayST $ do
    dst <- newUnliftedArray n (mempty :: ByteArray)
    let go !ix p = case p of
          PayloadsNil -> if ix == (-1)
            then unsafeFreezeUnliftedArray dst
            else errorWithoutStackTrace "arrow-interchange.payloadsToArray: impl mistake"
          PayloadsCons p' ps' -> do
            writeUnliftedArray dst ix p'
            go (ix - 1) ps'
    go (n - 1) p0
  step1 (PayloadsCons _ xs) !acc = step1 xs (acc + 1)
