{-# language DuplicateRecordFields #-}
{-# language UnboxedTuples #-}
{-# language MagicHash #-}
{-# language LambdaCase #-}

module ArrowFile
  ( Footer(..)
  , Block(..)
  , encodeFooter
  , parseFooter
  ) where

import Prelude hiding (length)

import ArrowSchema (Schema,Buffer(Buffer))
import ArrowSchema (encodeSchema)
import Data.Primitive (SmallArray,ByteArray,PrimArray)
import Control.Monad.ST.Run (runByteArrayST)
import Data.Int
import GHC.Exts ((+#),(*#),quotInt#)
import Data.Primitive.Types (writeByteArray#,indexByteArray#,readByteArray#)

import qualified Flatbuffers.Builder as B
import qualified Flatbuffers.Parser as P
import qualified Data.Primitive as PM
import qualified GHC.Exts as Exts
import qualified Data.Primitive.ByteArray.LittleEndian as LE
import qualified ArrowSchema

-- | The @version@ field is implied.
data Footer = Footer
  { schema :: !Schema
  , dictionaries :: !(PrimArray Block)
  , recordBatches :: !(PrimArray Block)
  }

-- | This is a flatbuffers struct
data Block = Block
  { offset :: !Int64
  , metaDataLength :: !Int32 -- we pad this when we encode it
  , bodyLength :: !Int64
  }

-- | This instance is only correct for little-endian architectures.
-- If I ever need to support a big-endian architecture, it will need to
-- be rewritten.
-- Also, this is currently missing all the methods for reading/writing
-- to/from Addr#.
instance PM.Prim Block where
  sizeOf# _ = 24#
  alignment# _ = 8#
  writeByteArray# arr# i# (Block a b c) =
    \s0 -> case writeByteArray# arr# (3# *# i#) a s0 of
       s1 -> case writeByteArray# arr# ((3# *# i#) +# 1#) (i32ToI64 b) s1 of
         s2 -> case writeByteArray# arr# ((3# *# i#) +# 2# ) c s2 of
           s3 -> s3
  readByteArray# arr# i# s0 = case readByteArray# arr# (3# *# i#) s0 of
    (# s1, (offset :: Int64) #) -> case readByteArray# arr# (2# +# (i# *# 6#)) s1 of
      (# s2, (metaDataLength :: Int32) #) -> case readByteArray# arr# ((3# *# i#) +# 2# ) s2 of
        (# s3, (bodyLength :: Int64) #) -> (# s3, Block{offset,metaDataLength,bodyLength} #)
  indexByteArray# arr# i# = Block
    (indexByteArray# arr# (i# *# 3#))
    (indexByteArray# arr# (2# +# (i# *# 6#)))
    (indexByteArray# arr# ((i# *# 3#) +# 2#))
  setByteArray# = PM.defaultSetByteArray#

i32ToI64 :: Int32 -> Int64
{-# inline i32ToI64 #-}
i32ToI64 = fromIntegral

serializeBlocks :: PrimArray Block -> ByteArray
serializeBlocks !blocks = runByteArrayST $ do
  let len = PM.sizeofPrimArray blocks
  dst <- PM.newByteArray (24 * len)
  let go !ix = if ix < len
        then do
          let Block{offset,metaDataLength,bodyLength} = PM.indexPrimArray blocks ix
          -- The positions are effectively:
          -- n, n+1, n+2
          -- but it is obscured by the alignment trick
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
  , B.structs (PM.sizeofPrimArray dictionaries) (serializeBlocks dictionaries)
  , B.structs (PM.sizeofPrimArray recordBatches) (serializeBlocks recordBatches)
  ]

parseFooter :: P.TableParser Footer
parseFooter = Footer
  <$  P.word16Eq 4
  <*> P.table ArrowSchema.parseSchema
  <*> P.structs
  <*> P.structs
