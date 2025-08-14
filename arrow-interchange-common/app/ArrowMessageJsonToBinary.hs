{-# language LambdaCase #-}
{-# language TemplateHaskell #-}

import Arrow.Builder.Raw
import Control.Exception (throwIO)
import Control.Monad (when)
import Data.Aeson (eitherDecodeFileStrict)
import Data.Aeson.TH (defaultOptions,deriveFromJSON)
import System.Environment (getArgs)

import qualified Data.Builder.Catenable.Bytes
import qualified Data.Bytes.Chunks as Chunks
import qualified Data.List as List
import qualified Flatbuffers.Builder as B
import qualified System.IO as IO

$(deriveFromJSON defaultOptions ''CompressionType)
$(deriveFromJSON defaultOptions ''BodyCompression)
$(deriveFromJSON defaultOptions ''Buffer)
$(deriveFromJSON defaultOptions ''DateUnit)
$(deriveFromJSON defaultOptions ''TableDate)
$(deriveFromJSON defaultOptions ''TableInt)
$(deriveFromJSON defaultOptions ''DictionaryEncoding)
$(deriveFromJSON defaultOptions ''TimeUnit)
$(deriveFromJSON defaultOptions ''TableMap)
$(deriveFromJSON defaultOptions ''TableTimestamp)
$(deriveFromJSON defaultOptions ''TableFixedSizeBinary)
$(deriveFromJSON defaultOptions ''TableFixedSizeList)
$(deriveFromJSON defaultOptions ''Type)
$(deriveFromJSON defaultOptions ''Field)
$(deriveFromJSON defaultOptions ''Schema)
$(deriveFromJSON defaultOptions ''FieldNode)
$(deriveFromJSON defaultOptions ''RecordBatch)
$(deriveFromJSON defaultOptions ''DictionaryBatch)
$(deriveFromJSON defaultOptions ''MessageHeader)
$(deriveFromJSON defaultOptions ''Message)

main :: IO ()
main = getArgs >>= \case
  [input,output] -> do
    when (not (List.isSuffixOf ".json" input)) $ fail "input file must end with .json"
    when (not (List.isSuffixOf ".bin" output)) $ fail "input file must end with .bin"
    eitherDecodeFileStrict input >>= \case
      Left e -> fail e
      Right (msg :: Message) -> IO.withFile output IO.WriteMode $ \h -> do
        Chunks.hPut h (Data.Builder.Catenable.Bytes.run (B.encode (encodeMessage msg)))
  _ -> fail "expected two arguments: input file name and output file name"
