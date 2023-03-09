{-# language DuplicateRecordFields #-}
{-# language LambdaCase #-}

module ArrowMessage
  ( Message(..)
  , FieldNode(..)
  , RecordBatch(..)
  , MessageHeader(..)
  , encodeMessage
  , encodeRecordBatch
  ) where

import Prelude hiding (length)

import ArrowSchema (Schema,Buffer(Buffer))
import ArrowSchema (encodeSchema)
import Data.Primitive (SmallArray,ByteArray)
import Control.Monad.ST.Run (runByteArrayST)
import Data.Int

import qualified Flatbuffers.Builder as B
import qualified Data.Primitive as PM
import qualified GHC.Exts as Exts
import qualified Data.Primitive.ByteArray.LittleEndian as LE
import qualified ArrowSchema

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

data RecordBatch = RecordBatch
  { length :: Int64
  , nodes :: !(SmallArray FieldNode)
  , buffers :: !(SmallArray Buffer)
  }

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
encodeRecordBatch RecordBatch{length,nodes,buffers} = B.Object $ Exts.fromList
  [ B.signed64 length
  , B.structs (PM.sizeofSmallArray nodes) (serializeNodes nodes)
  , B.structs (PM.sizeofSmallArray buffers) (serializeBuffers buffers)
  ]

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
