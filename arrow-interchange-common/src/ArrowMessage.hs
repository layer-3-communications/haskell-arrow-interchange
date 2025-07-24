{-# language DuplicateRecordFields #-}
{-# language MagicHash #-}
{-# language UnboxedTuples #-}
{-# language LambdaCase #-}

module ArrowMessage
  ( Message(..)
  , FieldNode(..)
  , RecordBatch(..)
  , DictionaryBatch(..)
  , MessageHeader(..)
  , CompressionType(..)
  , BodyCompression(..)
  , encodeMessage
  , parseMessage
  , encodeRecordBatch
  ) where

import Prelude hiding (length)

import ArrowSchema (Schema,Buffer(Buffer))
import ArrowSchema (encodeSchema, parseSchema)
import Data.Primitive (SmallArray,PrimArray,ByteArray)
import Control.Monad.ST.Run (runByteArrayST)
import Data.Int
import Data.Functor (($>))
import GHC.Exts ((+#),(*#))
import Data.Primitive.Types (indexByteArray#,writeByteArray#,readByteArray#)

import qualified Data.Primitive.Contiguous as Contiguous
import qualified Flatbuffers.Builder as B
import qualified Flatbuffers.Parser as P
import qualified Data.Primitive as PM
import qualified GHC.Exts as Exts
import qualified Data.Primitive.ByteArray.LittleEndian as LE
import qualified ArrowSchema

-- The @version@ field is implied.
data Message = Message
  { header :: !MessageHeader
  , bodyLength :: !Int64
  } deriving (Show)

-- This is a flatbuffers struct
data FieldNode = FieldNode
  { length :: !Int64
  , nullCount :: !Int64
  } deriving (Show)

instance PM.Prim FieldNode where
  sizeOf# _ = 16#
  alignment# _ = 8#
  writeByteArray# arr# i# (FieldNode a b) =
    \s0 -> case writeByteArray# arr# (2# *# i#) a s0 of
       s1 -> writeByteArray# arr# ((2# *# i#) +# 1# ) b s1
  readByteArray# arr# i# s0 = case readByteArray# arr# (2# *# i#) s0 of
    (# s1, (length :: Int64) #) -> case readByteArray# arr# ((2# *# i#) +# 1# ) s1 of
      (# s2, (nullCount :: Int64) #) -> (# s2, FieldNode{length,nullCount} #)
  indexByteArray# arr# i# = FieldNode
    (indexByteArray# arr# (i# *# 2#))
    (indexByteArray# arr# ((i# *# 2#) +# 1#))
  setByteArray# = PM.defaultSetByteArray#

data RecordBatch = RecordBatch
  { length :: Int64
  , nodes :: !(PrimArray FieldNode)
  , buffers :: !(PrimArray Buffer)
  , compression :: !(Maybe BodyCompression)
  } deriving (Show)

data DictionaryBatch = DictionaryBatch
  { id :: !Int64 -- the dictionary id
  , data_ :: !RecordBatch
  , isDelta :: !Bool
  } deriving (Show)

-- We omit the method field since the only value for BodyCompressionMethod
-- is BUFFER.
newtype BodyCompression = BodyCompression
  { codec :: CompressionType
  } deriving (Show)

-- | We should also add zstd, but we do not have bindings for it available,
-- so I am leaving it out for now.
data CompressionType
  = Lz4Frame
  deriving (Show)

data MessageHeader
  = MessageHeaderSchema !Schema
  | MessageHeaderDictionaryBatch !DictionaryBatch
  | MessageHeaderRecordBatch !RecordBatch
  deriving (Show)

serializeNodes :: PrimArray FieldNode -> ByteArray
serializeNodes !nodes = runByteArrayST $ do
  let len = PM.sizeofPrimArray nodes
  dst <- PM.newByteArray (16 * len)
  let go !ix = if ix < len
        then do
          let FieldNode{length,nullCount} = PM.indexPrimArray nodes ix
          LE.writeByteArray dst (ix * 2) length
          LE.writeByteArray dst (ix * 2 + 1) nullCount
          go (ix + 1)
        else PM.unsafeFreezeByteArray dst
  go 0

serializeBuffers :: PrimArray Buffer -> ByteArray
serializeBuffers !nodes = runByteArrayST $ do
  let len = PM.sizeofPrimArray nodes
  dst <- PM.newByteArray (16 * len)
  let go !ix = if ix < len
        then do
          let Buffer{offset,length} = PM.indexPrimArray nodes ix
          LE.writeByteArray dst (ix * 2) offset
          LE.writeByteArray dst (ix * 2 + 1) length
          go (ix + 1)
        else PM.unsafeFreezeByteArray dst
  go 0

parseRecordBatch :: P.TableParser RecordBatch
parseRecordBatch = RecordBatch
  <$> P.int64
  <*> P.structs
  <*> P.structs
  <*> P.optTable parseBodyCompression

parseDictionaryBatch :: P.TableParser DictionaryBatch
parseDictionaryBatch = DictionaryBatch
  <$> P.int64
  <*> P.table parseRecordBatch
  <*> P.boolean

parseBodyCompression :: P.TableParser BodyCompression
parseBodyCompression = BodyCompression
  <$> (P.word8Eq 0 $> Lz4Frame)

encodeDictionaryBatch :: DictionaryBatch -> B.Object
encodeDictionaryBatch DictionaryBatch{id,data_,isDelta} =
  B.Object $ Contiguous.tripleton
    (B.signed64 id)
    (B.FieldObject (encodeRecordBatch data_))
    (B.boolean isDelta)

encodeRecordBatch :: RecordBatch -> B.Object
encodeRecordBatch RecordBatch{length,nodes,buffers,compression} =
  case compression of
    Nothing -> B.Object (Contiguous.tripleton v0 v1 v2)
    Just (BodyCompression Lz4Frame) ->
      let v3 = B.FieldObject (B.Object mempty)
       in B.Object (Contiguous.quadrupleton v0 v1 v2 v3)
  where
  v0 = B.signed64 length
  v1 = B.structs (PM.sizeofPrimArray nodes) (serializeNodes nodes)
  v2 = B.structs (PM.sizeofPrimArray buffers) (serializeBuffers buffers)

encodeMessageHeader :: MessageHeader -> B.Union
encodeMessageHeader = \case
  MessageHeaderSchema s -> B.Union{tag=1,object=encodeSchema s}
  MessageHeaderRecordBatch b -> B.Union{tag=3,object=encodeRecordBatch b}
  MessageHeaderDictionaryBatch b -> B.Union{tag=2,object=encodeDictionaryBatch b}

encodeMessage :: Message -> B.Object
encodeMessage Message{header,bodyLength} = B.Object $ Exts.fromList
  [ B.unsigned16 4
  , B.union (encodeMessageHeader header)
  , B.signed64 bodyLength
  ]

parseMessageHeader :: P.UnionParser MessageHeader
parseMessageHeader = P.constructUnion3
  (MessageHeaderSchema <$> parseSchema)
  (MessageHeaderDictionaryBatch <$> parseDictionaryBatch)
  (MessageHeaderRecordBatch <$> parseRecordBatch)

parseMessage :: P.TableParser Message
parseMessage = Message
  <$  P.word16Eq 4
  <*> P.union parseMessageHeader
  <*> P.int64
