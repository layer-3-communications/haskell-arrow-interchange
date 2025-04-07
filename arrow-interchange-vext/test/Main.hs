{-# language MagicHash #-}
{-# language OverloadedStrings #-}
{-# language PatternSynonyms #-}

import Arrow.Vext (NamedColumns(..), NamedColumn(..), Column(..), encodeNamedColumns, decode)
import Arrow.Vext (Compression(Lz4, None))
import GHC.Exts (Int#, Int32#, Word16#)
import Data.Unlifted (pattern True#)
import Arithmetic.Nat (pattern N3#, pattern N4#, pattern N5#)
import Control.Monad (when)
import GHC.Word (Word16(W16#), Word32(W32#))
import Data.Text (Text)
import Data.Maybe (fromMaybe)
import System.Environment (lookupEnv)

import qualified Data.Builder.Catenable.Bytes as Catenable
import qualified Data.Bytes as Bytes
import qualified Data.Bytes.Chunks as Chunks
import qualified Data.Primitive.Contiguous as Contiguous
import qualified Data.Text as T
import qualified GHC.Exts as Exts
import qualified System.IO as IO
import qualified Vector.Bit as Bit
import qualified Vector.Int32 as Int32
import qualified Vector.Word16 as Word16
import qualified Vector.Word32 as Word32

main :: IO ()
main = do
  IO.hPutStrLn IO.stderr "Performing roundtrip tests"
  selector <- fromMaybe "ALL" <$> lookupEnv "ONETEST"
  when (selector == "ALL" || selector == "01") $ testRoundtrip "Example 01" example01
  when (selector == "ALL" || selector == "02") $ testRoundtrip "Example 02" example02
  when (selector == "ALL" || selector == "03") $ testRoundtrip "Example 03" example03
  when (selector == "ALL" || selector == "04") $ testRoundtrip "Example 04" example04
  IO.hPutStrLn IO.stderr "Testing decoding from files"
  when (selector == "ALL" || selector == "firewall") $ do
    NamedColumns{size, columns} <- testFile "Clickhouse Firewall Dump" "examples/firewall.arrow"
    PrimitiveWord32 ips <- findColumnOrFail "source_ip" columns
    when (not (Word32.all (\w -> W32# w == 0x0a010815) size ips)) $ do
      IO.hPutStrLn IO.stderr "Expected all source IPs to be 0x0a010815 (10.1.8.21). Got this instead:"
      IO.hPutStrLn IO.stderr (Word32.show size ips)
      fail "Exiting"
    PrimitiveWord16 ports <- findColumnOrFail "destination_port" columns
    when (not (Word16.all (\w -> W16# w == 443) size ports)) $ do
      IO.hPutStrLn IO.stderr "Expected all destination ports to be 443. Got this instead:"
      IO.hPutStrLn IO.stderr (Word16.show size ports)
      fail "Exiting"
  when (selector == "ALL" || selector == "traffic-01") $ do
    NamedColumns{columns} <- testFile "Clickhouse Traffic 01 Dump" "examples/traffic-01.arrow"
    PrimitiveWord32 _ <- findColumnOrFail "source_ip" columns
    PrimitiveWord16 _ <- findColumnOrFail "destination_port" columns
    pure ()
  when (selector == "ALL" || selector == "traffic-02") $ do
    NamedColumns{columns} <- testFile "Clickhouse Traffic 02 Dump" "examples/traffic-02.arrow"
    PrimitiveWord32 _ <- findColumnOrFail "source_ip" columns
    PrimitiveWord16 _ <- findColumnOrFail "destination_port" columns
    VariableBinaryUtf8 _ <- findColumnOrFail "application" columns
    pure ()
  IO.hPutStrLn IO.stderr "All tests succeeded"

testFile :: String -> String -> IO NamedColumns
testFile name fileName = do
  contents <- Bytes.toByteArray <$> Bytes.readFile fileName
  case decode contents of
    Left err -> fail (name ++ " failed with: " ++ show err)
    Right x -> do
      IO.hPutStrLn IO.stderr (name ++ " decoded successfully")
      pure x

testRoundtrip :: String -> NamedColumns -> IO ()
testRoundtrip name ex = do
  case decode (Chunks.concatU (Catenable.run (encodeNamedColumns None ex))) of
    Left err -> fail (name ++ " failed with: " ++ show err)
    Right x -> when (x /= ex) $ do
      IO.hPutStrLn IO.stderr (name ++ " does not roundtrip")
      IO.hPutStrLn IO.stderr "Expected:"
      IO.hPutStrLn IO.stderr (show ex)
      IO.hPutStrLn IO.stderr "Got:"
      IO.hPutStrLn IO.stderr (show x)
      fail "Exiting"
  IO.hPutStrLn IO.stderr (name ++ " success")

example01 :: NamedColumns
example01 = NamedColumns N4# $ Contiguous.construct1
  ( NamedColumn "height" (Bit.replicate N4# True#)
    (PrimitiveInt32 $ Int32.construct4 (i32 42#) (i32 1089#) (i32 753205#) (i32 (-55#)))
  )

example02 :: NamedColumns
example02 = NamedColumns N5# $ Contiguous.construct1
  ( NamedColumn "megahertz" (Bit.replicate N5# True#)
    (PrimitiveInt32 $ Int32.construct5 (i32 65071347#) (i32 (-57259449#)) (i32 476453#) (i32 (-11#)) (i32 (-269#)))
  )

example03 :: NamedColumns
example03 = NamedColumns N3# $ Contiguous.construct2
  ( NamedColumn "latitude" (Bit.replicate N3# True#)
    (PrimitiveInt32 $ Int32.construct3 (i32 63#) (i32 17#) (i32 89#))
  )
  ( NamedColumn "longitude" (Bit.replicate N3# True#)
    (PrimitiveInt32 $ Int32.construct3 (i32 14#) (i32 12#) (i32 71#))
  )

example04 :: NamedColumns
example04 = NamedColumns N3# $ Contiguous.construct2
  ( NamedColumn "protocol" (Bit.replicate N3# True#)
    (PrimitiveInt32 $ Int32.construct3 (i32 6#) (i32 6#) (i32 17#))
  )
  ( NamedColumn "port" (Bit.replicate N3# True#)
    (PrimitiveWord16 $ Word16.construct3 (u16 443#) (u16 443#) (u16 53#))
  )

i32 :: Int# -> Int32#
i32 = Exts.intToInt32#

u16 :: Int# -> Word16#
u16 i = Exts.wordToWord16# (Exts.int2Word# i)

findColumnOrFail :: Text -> Contiguous.SmallArray (NamedColumn n) -> IO (Column n)
findColumnOrFail x xs = case Contiguous.find (\NamedColumn{name} -> name == x) xs of
  Nothing -> fail ("No column named: " ++ T.unpack x)
  Just NamedColumn{column} -> pure column
