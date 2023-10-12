{-# language DuplicateRecordFields #-}
{-# language LambdaCase #-}

module ArrowMessage
  ( Message(..)
  , FieldNode(..)
  , RecordBatch(..)
  , MessageHeader(..)
  , CompressionType(..)
  , BodyCompression(..)
  , encodeMessage
  , encodeRecordBatch
  , decodeMessage
  ) where

import Prelude hiding (length)

import ArrowSchema (Schema,Buffer(Buffer))
import ArrowSchema (encodeSchema,decodeSchemaInternal)
import ArrowSchema (decodeBufferInternal)
import Data.Primitive (SmallArray,ByteArray)
import Control.Monad.ST.Run (runByteArrayST)
import Data.Int
import Data.Bytes (Bytes)

import qualified Data.Primitive.Contiguous as Contiguous
import qualified Flatbuffers.Builder as B
import qualified Data.Primitive as PM
import qualified GHC.Exts as Exts
import qualified Data.Primitive.ByteArray.LittleEndian as LE
import qualified ArrowSchema
import qualified FlatBuffers as FB
import qualified FlatBuffers.Vector as FBV
import qualified Data.Bytes as Bytes
import qualified ArrowTemplateHaskell as G
import qualified Data.ByteString.Lazy as LBS

-- The @version@ field is implied.
data Message = Message
  { header :: !MessageHeader
  , bodyLength :: !Int64
  }

-- This is a flatbuffers struct
data FieldNode = FieldNode
  { length :: !Int64
  , nullCount :: !Int64
  }

decodeMessage :: Bytes -> Either String Message
decodeMessage !b = do
  theMessage <- FB.decode (LBS.fromStrict (Bytes.toByteString b))
  decodeMessageInternal theMessage

decodeMessageInternal :: FB.Table G.Message -> Either String Message
decodeMessageInternal x = do
  hdr <- G.messageHeader x >>= \case
    FB.UnionNone -> Left "Got None when looking for message header union"
    FB.UnionUnknown{} -> Left "Got unrecognized tag when looking for message header union"
    FB.Union h -> case h of
      G.MessageHeaderSchema s ->
        MessageHeaderSchema <$> decodeSchemaInternal s
      G.MessageHeaderDictionaryBatch{} -> Left "decodeSchemaInternal: cannot yet handle dictionary batch message"
      G.MessageHeaderRecordBatch r ->
        MessageHeaderRecordBatch <$> decodeRecordBatch r
      G.MessageHeaderTensor{} -> Left "Sorry: tensor is not supported"
      G.MessageHeaderSparseTensor{} -> Left "Sorry: sparse tensor is not supported"
  bodyLength <- G.messageBodyLength x
  pure Message{header=hdr,bodyLength}

decodeFieldNode :: FB.Struct G.FieldNode -> Either String FieldNode
decodeFieldNode x = do
  length <- G.fieldNodeLength x
  nullCount <- G.fieldNodeNullCount x
  pure FieldNode{length,nullCount}

decodeRecordBatch :: FB.Table G.RecordBatch -> Either String RecordBatch
decodeRecordBatch x = do
  len <- G.recordBatchLength x
  nodes <- G.recordBatchNodes x >>= \case
    Nothing -> pure mempty
    Just ys -> do
      zs <- FBV.toList ys >>= traverse decodeFieldNode
      pure (Exts.fromList zs)
  buffers <- G.recordBatchBuffers x >>= \case
    Nothing -> pure mempty
    Just ys -> do
      zs <- FBV.toList ys >>= traverse decodeBufferInternal
      pure (Exts.fromList zs)
  cmpr <- G.recordBatchCompression x >>= \case
    Nothing -> Right Nothing 
    Just z -> do
      ty <- G.bodyCompressionCodec z
      case ty of
        0 -> Right (Just (BodyCompression Lz4Frame))
        1 -> Left "Sorry, we do not support zstd yet"
        _ -> Left "Unrecognized compression codec"
  pure RecordBatch
    { length = len
    , nodes = nodes
    , buffers = buffers
    , compression = cmpr
    }

data RecordBatch = RecordBatch
  { length :: Int64
  , nodes :: !(SmallArray FieldNode)
  , buffers :: !(SmallArray Buffer)
  , compression :: !(Maybe BodyCompression)
  }

-- We omit the method field since the only value for BodyCompressionMethod
-- is BUFFER.
newtype BodyCompression = BodyCompression
  { codec :: CompressionType
  }

-- | We should also add zstd, but we do not have bindings for it available,
-- so I am leaving it out for now.
data CompressionType
  = Lz4Frame

data MessageHeader
  = MessageHeaderSchema !Schema
  | MessageHeaderDictionaryBatch
  | MessageHeaderRecordBatch !RecordBatch

serializeNodes :: SmallArray FieldNode -> ByteArray
serializeNodes !nodes = runByteArrayST $ do
  let len = PM.sizeofSmallArray nodes
  dst <- PM.newByteArray (16 * len)
  let go !ix = if ix < len
        then do
          let FieldNode{length,nullCount} = PM.indexSmallArray nodes ix
          LE.writeByteArray dst (ix * 2) length
          LE.writeByteArray dst (ix * 2 + 1) nullCount
          go (ix + 1)
        else PM.unsafeFreezeByteArray dst
  go 0

serializeBuffers :: SmallArray Buffer -> ByteArray
serializeBuffers !nodes = runByteArrayST $ do
  let len = PM.sizeofSmallArray nodes
  dst <- PM.newByteArray (16 * len)
  let go !ix = if ix < len
        then do
          let Buffer{offset,length} = PM.indexSmallArray nodes ix
          LE.writeByteArray dst (ix * 2) offset
          LE.writeByteArray dst (ix * 2 + 1) length
          go (ix + 1)
        else PM.unsafeFreezeByteArray dst
  go 0

encodeRecordBatch :: RecordBatch -> B.Object
encodeRecordBatch RecordBatch{length,nodes,buffers,compression} =
  case compression of
    Nothing -> B.Object (Contiguous.tripleton v0 v1 v2)
    Just (BodyCompression Lz4Frame) ->
      let v3 = B.FieldObject (B.Object mempty)
       in B.Object (Contiguous.quadrupleton v0 v1 v2 v3)
  where
  v0 = B.signed64 length
  v1 = B.structs (PM.sizeofSmallArray nodes) (serializeNodes nodes)
  v2 = B.structs (PM.sizeofSmallArray buffers) (serializeBuffers buffers)

encodeMessageHeader :: MessageHeader -> B.Union
encodeMessageHeader = \case
  MessageHeaderSchema s -> B.Union{tag=1,object=encodeSchema s}
  MessageHeaderRecordBatch b -> B.Union{tag=3,object=encodeRecordBatch b}

encodeMessage :: Message -> B.Object
encodeMessage Message{header,bodyLength} = B.Object $ Exts.fromList
  [ B.unsigned16 4
  , B.union (encodeMessageHeader header)
  , B.signed64 bodyLength
  ]
