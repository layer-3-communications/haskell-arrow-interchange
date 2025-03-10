{-# language DuplicateRecordFields #-}
{-# language LambdaCase #-}

module ArrowFile
  ( Footer(..)
  , Block(..)
  , encodeFooter
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

-- | The @version@ field is implied.
data Footer = Footer
  { schema :: !Schema
  , dictionaries :: !(SmallArray Block)
  , recordBatches :: !(SmallArray Block)
  }

-- | This is a flatbuffers struct
data Block = Block
  { offset :: !Int64
  , metaDataLength :: !Int32 -- we pad this when we encode it
  , bodyLength :: !Int64
  }

serializeBlocks :: SmallArray Block -> ByteArray
serializeBlocks !blocks = runByteArrayST $ do
  let len = PM.sizeofSmallArray blocks
  dst <- PM.newByteArray (24 * len)
  let go !ix = if ix < len
        then do
          let Block{offset,metaDataLength,bodyLength} = PM.indexSmallArray blocks ix
          LE.writeByteArray dst (ix * 3) offset
          LE.writeByteArray dst (ix * 6 + 2) metaDataLength
          LE.writeByteArray dst (ix * 3 + 2) bodyLength
          go (ix + 1)
        else PM.unsafeFreezeByteArray dst
  go 0

encodeFooter :: Footer -> B.Object
encodeFooter Footer{schema,dictionaries,recordBatches} = B.Object $ Exts.fromList
  [ B.unsigned16 4
  , B.FieldObject (encodeSchema schema)
  , B.structs (PM.sizeofSmallArray dictionaries) (serializeBlocks dictionaries)
  , B.structs (PM.sizeofSmallArray recordBatches) (serializeBlocks recordBatches)
  ]

