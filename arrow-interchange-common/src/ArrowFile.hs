{-# language DuplicateRecordFields #-}
{-# language LambdaCase #-}

module ArrowFile
  ( Footer(..)
  , Block(..)
  , encodeFooter
  , decodeFooter
  ) where

import Prelude hiding (length)

import ArrowSchema (Schema)
import ArrowSchema (encodeSchema,decodeSchemaInternal)
import Data.Primitive (SmallArray,ByteArray)
import Control.Monad.ST.Run (runByteArrayST)
import Data.Int
import Data.Maybe (maybe)
import Data.Bytes (Bytes)

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
data Footer = Footer
  { schema :: !Schema
  , dictionaries :: !(SmallArray Block)
  , recordBatches :: !(SmallArray Block)
  }

decodeFooter :: Bytes -> Either String Footer
decodeFooter !b = do
  theSchema <- FB.decode (LBS.fromStrict (Bytes.toByteString b))
  decodeFooterInternal theSchema

decodeFooterInternal :: FB.Table G.Footer -> Either String Footer
decodeFooterInternal x = do
  theSchema <- G.footerSchema x
    >>= maybe (Left "Expected schema in footer") Right
    >>= decodeSchemaInternal
  dicts <- G.footerDictionaries x >>= \case
    Nothing -> pure mempty
    Just ys -> do
      zs <- FBV.toList ys >>= traverse decodeBlock
      pure (Exts.fromList zs)
  recs <- G.footerRecordBatches x >>= \case
    Nothing -> pure mempty
    Just ys -> do
      zs <- FBV.toList ys >>= traverse decodeBlock
      pure (Exts.fromList zs)
  pure Footer
    { schema = theSchema
    , dictionaries = dicts
    , recordBatches = recs
    }

decodeBlock :: FB.Struct G.Block -> Either String Block
decodeBlock x = do
  offset <- G.blockOffset x
  metaDataLength <- G.blockMetaDataLength x
  bodyLength <- G.blockBodyLength x
  pure Block{offset,metaDataLength,bodyLength}

-- This is a flatbuffers struct
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

