{-# language LambdaCase #-}
{-# language TemplateHaskell #-}

import ArrowFile
import ArrowSchema
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

$(deriveFromJSON defaultOptions ''Buffer)
$(deriveFromJSON defaultOptions ''DateUnit)
$(deriveFromJSON defaultOptions ''TableDate)
$(deriveFromJSON defaultOptions ''TableInt)
$(deriveFromJSON defaultOptions ''TimeUnit)
$(deriveFromJSON defaultOptions ''TableTimestamp)
$(deriveFromJSON defaultOptions ''TableFixedSizeBinary)
$(deriveFromJSON defaultOptions ''Type)
$(deriveFromJSON defaultOptions ''Field)
$(deriveFromJSON defaultOptions ''Schema)
$(deriveFromJSON defaultOptions ''Block)
$(deriveFromJSON defaultOptions ''Footer)

main :: IO ()
main = getArgs >>= \case
  [input,output] -> do
    when (not (List.isSuffixOf ".json" input)) $ fail "input file must end with .json"
    when (not (List.isSuffixOf ".bin" output)) $ fail "input file must end with .bin"
    eitherDecodeFileStrict input >>= \case
      Left e -> fail e
      Right (msg :: Footer) -> IO.withFile output IO.WriteMode $ \h -> do
        Chunks.hPut h (Data.Builder.Catenable.Bytes.run (B.encode (encodeFooter msg)))
  _ -> fail "expected two arguments: input file name and output file name"

