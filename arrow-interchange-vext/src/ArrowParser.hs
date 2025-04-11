{-# language DuplicateRecordFields #-}

module ArrowParser
  ( Error(..)
  , decodeFooterFromArrowContents
  , extractMetadata
  ) where

import Arrow.Builder.Raw (Footer,Block(Block),Message,parseFooter,parseMessage)
import Arrow.Builder.Raw (Type)
import Control.Monad (when)
import Control.Monad.ST (runST)
import Data.Bifunctor (first)
import Data.Char (chr)
import Data.Primitive (ByteArray)
import Data.Primitive (sizeofByteArray,indexByteArray)
import Data.Text (Text)
import Data.Word (Word32,Word8)
import Data.Int (Int64)

import qualified Arrow.Builder.Raw
import qualified Data.Primitive as PM
import qualified Flatbuffers.Parser as P
import qualified Data.Primitive.ByteArray.LittleEndian as LE

data Error
  = MissingMagicTrailer
  | NegativeBatchLength
  | Lz4DecompressionFailure !Int !Int
  | CompressedBufferMisaligned
  | CannotDecompressToGiganticArray !Int64
  | NegativeDecompressedSize
  | DisabledDecompressionNotSupported
  | ContentsTooSmall
  | ColumnByteLengthDisagreesWithBatchLength !Text !Int !Int !Int
  | FooterOutOfBounds
  | FooterSizeMisaligned
  | FooterFlatbuffersDecodeError !P.Error
  | MessageFlatbuffersDecodeError !P.Error
  | MoreThanOneBatch
  | ZeroBatches
  | OffsetToMetadataTooSmall
  | OffsetToMetadataTooLarge
  | MissingContinuationByte
  | MetadataLengthsDoNotAgree
  | OffsetToMetadataMisaligned !Int
  | ExpectedRecordBatch
  | Placeholder !String
  | ImplementationLimit
  | MisalignedByteLengthFor32BitIntBatch
  | BatchDataOutOfRange
  | RanOutOfBuffers
  | MisalignedOffsetForIntBatch !Int !Int
  | SchemaFieldCountDoesNotMatchNodeCount
  | BigEndianBuffersNotSupported
  | I32ColumnStartsMisaligned
  | UnsupportedBitWidth
  | UnsupportedCombinationOfBitWidthAndSign
  | DecompressionNotSupported
  | OnlyRecordBatchesAreSupported
  | VariableBinaryIndicesBad
  | CannotUnmarshalColumnWithType !Type
  deriving (Show)

extractMetadata :: ByteArray -> Block -> Either Error Message
extractMetadata !contents Block{offset,metaDataLength} = do
  when (offset < 8) (Left OffsetToMetadataTooSmall)
  let !offsetI = fromIntegral offset :: Int
  when (rem offsetI 8 /= 0) (Left (OffsetToMetadataMisaligned offsetI))
  let !metaDataLengthI = fromIntegral metaDataLength :: Int
  when (PM.indexByteArray contents (offsetI + 0) /= (0xFF :: Word8)) $ Left MissingContinuationByte
  when (PM.indexByteArray contents (offsetI + 1) /= (0xFF :: Word8)) $ Left MissingContinuationByte
  when (PM.indexByteArray contents (offsetI + 2) /= (0xFF :: Word8)) $ Left MissingContinuationByte
  when (PM.indexByteArray contents (offsetI + 3) /= (0xFF :: Word8)) $ Left MissingContinuationByte
  when (offsetI + metaDataLengthI > sizeofByteArray contents - 8) (Left OffsetToMetadataTooLarge)
  -- We have two copies of the metadata length for some reason when we decode
  -- starting at the end of the file. Here, we make sure that they agree as an
  -- extra validation step.
  let (metadataSizeExtraW :: Word32) = LE.indexByteArray contents (1 + quot offsetI 4)
  let (metadataSizeExtra :: Int) = fromIntegral metadataSizeExtraW
  when (metadataSizeExtra > metaDataLengthI - 8) $ Left MetadataLengthsDoNotAgree
  let encodedMessage = runST $ do
        dst <- PM.newByteArray metadataSizeExtra
        PM.copyByteArray dst 0 contents (offsetI + 8) metadataSizeExtra
        PM.unsafeFreezeByteArray dst
  first MessageFlatbuffersDecodeError (P.run parseMessage encodedMessage)

decodeFooterFromArrowContents :: ByteArray -> Either Error Footer
decodeFooterFromArrowContents !contents = do
  let sz = sizeofByteArray contents
  when (sz < 10) $ Left ContentsTooSmall
  when (indexChar contents (sz - 6) /= 'A') $ Left MissingMagicTrailer
  when (indexChar contents (sz - 5) /= 'R') $ Left MissingMagicTrailer
  when (indexChar contents (sz - 4) /= 'R') $ Left MissingMagicTrailer
  when (indexChar contents (sz - 3) /= 'O') $ Left MissingMagicTrailer
  when (indexChar contents (sz - 2) /= 'W') $ Left MissingMagicTrailer
  when (indexChar contents (sz - 1) /= '1') $ Left MissingMagicTrailer
  let ixFooterSize = sz - 10
  when (rem ixFooterSize 4 /= 0) $ Left FooterSizeMisaligned
  let (footerSizeW :: Word32) = LE.indexByteArray contents (quot ixFooterSize 4)
  let footerSize = fromIntegral @Word32 @Int footerSizeW
  when (footerSize + 10 > sz) $ Left FooterOutOfBounds
  -- Note: It doesn't really make any sense to perform a copy
  -- here, but the library used for decoding flatbuffers does not
  -- support operating on a slice. I could fix this later.
  let encodedFooter = runST $ do
        encodeFooterMut <- PM.newByteArray footerSize
        PM.copyByteArray encodeFooterMut 0 contents (sz - (10 + footerSize)) footerSize
        PM.unsafeFreezeByteArray encodeFooterMut
  first FooterFlatbuffersDecodeError (P.run parseFooter encodedFooter)

indexChar :: ByteArray -> Int -> Char
{-# inline indexChar #-}
indexChar b i = chr (fromIntegral (indexByteArray b i :: Word8))

