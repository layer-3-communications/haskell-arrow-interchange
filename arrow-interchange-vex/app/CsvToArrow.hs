{-# language DuplicateRecordFields #-}
{-# language LambdaCase #-}
{-# language OverloadedStrings #-}

import Arithmetic.Types (Fin(Fin))
import Arrow.Builder.Vex (NamedColumn(..),Compression(..),MaskedColumn(..),Contents(..))
import Control.Monad (when)
import Control.Monad.ST (stToIO)
import Data.Bytes (Bytes)
import Data.Int
import Data.Text (Text)
import Data.Text.Short (ShortText)
import Data.Word
import GHC.Exts (RealWorld)
import Net.Types (IPv4(IPv4))
import Text.Read (readMaybe)
import System.Environment (getArgs)

import qualified Arithmetic.Fin as Fin
import qualified Arithmetic.Lte as Lte
import qualified Arithmetic.Nat as Nat
import qualified Arithmetic.Types as Arithmetic
import qualified Arrow.Builder.Vex
import qualified Data.Builder.Catenable.Bytes as Catenable
import qualified Data.Bytes as Bytes
import qualified Data.Bytes.Chunks as Chunks
import qualified Data.Bytes.Text.Latin1 as Latin1
import qualified Data.List as List
import qualified Data.Primitive as PM
import qualified Data.Text as T
import qualified Data.Text.Short as TS
import qualified GHC.Exts as Exts
import qualified Net.IPv4 as IPv4
import qualified System.IO as IO
import qualified Vector.Bool.Internal as Bool
import qualified Vector.Boxed as Boxed
import qualified Vector.Int64.Internal as Int64
import qualified Vector.ShortText.Internal as ShortText
import qualified Vector.ShortTexts.Internal as ShortTexts
import qualified Vector.Word16.Internal as Word16
import qualified Vector.Word32.Internal as Word32
import qualified Vector.Word64.Internal as Word64
import qualified Vector.Word8.Internal as Word8

-- The header row of the CSV needs to look like this:
--
-- > age:s32,height:s64,first_name:string
main :: IO ()
main = do
  compression <- getArgs >>= \case
    [] -> pure None
    ["--lz4"] -> pure Lz4
    ["--uncompressed"] -> pure None
    _ -> fail "Bad flags, expected --lz4 or --uncompressed"
  contents <- Chunks.hGetContents IO.stdin
  let bytes = Chunks.concat contents
  let rows = List.filter (not . Bytes.null) (Bytes.split 0x0A bytes)
  case rows of
    [] -> fail "expected at least one row (the header)"
    header : dataRows -> do
      let rawNames = Bytes.split 0x2c header
          len = List.length rawNames
          rowsLen = List.length dataRows
      decodedNames <- mapM decodeName (map (T.pack . Latin1.toString) rawNames)
      dataRows' <- traverse
        (\row -> do
          let row' = Bytes.split 0x2c row
          when (List.length row' /= len) (fail "Encountered row with bad length")
          pure row'
        ) dataRows
      let dataColumns = List.transpose dataRows'
      Nat.with rowsLen $ \n -> do
        cols <- decodeColumns n (zip decodedNames dataColumns)
        IO.withFile "output.arrow" IO.WriteMode $ \h -> do
          Chunks.hPut h (Catenable.run (Arrow.Builder.Vex.encode n compression (Exts.fromList cols)))

decodeColumns :: Arithmetic.Nat n -> [(DecodedName,[Bytes])] -> IO [NamedColumn n]
decodeColumns !n = traverse (uncurry (decodeColumn n))

decodeColumn :: Arithmetic.Nat n -> DecodedName -> [Bytes] -> IO (NamedColumn n)
decodeColumn !n DecodedName{name,ty} xs = do
  xs' <- maybe (fail "wrong number of cells in column") pure (Boxed.fromListN n xs)
  case ty of
    Unsigned8 -> do
      dst <- stToIO (Word8.uninitialized n)
      mask <- stToIO (Bool.uninitialized n)
      zeroBoolArray mask
      Fin.ascendM_ n $ \(Fin ix lt) -> do
        let str = Boxed.index lt xs' ix
        if Bytes.null str
          then pure ()
          else case readMaybe (Latin1.toString str) of
            Nothing -> fail "failed to decode U8 value"
            Just (w :: Word8) -> stToIO $ do
              Bool.write lt mask ix True
              Word8.write lt dst ix w
      dst' <- stToIO (Word8.unsafeFreeze dst)
      mask' <- stToIO (Bool.unsafeFreeze mask)
      pure NamedColumn{name,contents=Values MaskedColumn{mask=mask',column=Arrow.Builder.Vex.PrimitiveWord8 dst'}}
    Unsigned16 -> do
      dst <- stToIO (Word16.uninitialized n)
      mask <- stToIO (Bool.uninitialized n)
      zeroBoolArray mask
      Fin.ascendM_ n $ \(Fin ix lt) -> do
        let str = Boxed.index lt xs' ix
        if Bytes.null str
          then pure ()
          else case readMaybe (Latin1.toString str) of
            Nothing -> fail "failed to decode U16 value"
            Just (w :: Word16) -> stToIO $ do
              Bool.write lt mask ix True
              Word16.write lt dst ix w
      dst' <- stToIO (Word16.unsafeFreeze dst)
      mask' <- stToIO (Bool.unsafeFreeze mask)
      pure NamedColumn{name,contents=Values MaskedColumn{mask=mask',column=Arrow.Builder.Vex.PrimitiveWord16 dst'}}
    Unsigned32 -> do
      dst <- stToIO (Word32.uninitialized n)
      mask <- stToIO (Bool.uninitialized n)
      zeroBoolArray mask
      Fin.ascendM_ n $ \(Fin ix lt) -> do
        let str = Boxed.index lt xs' ix
        if Bytes.null str
          then pure ()
          else case readMaybe (Latin1.toString str) of
            Nothing -> case IPv4.decodeUtf8Bytes str of
              Nothing -> fail "failed to decode U32 value"
              Just (IPv4 w) -> stToIO $ do
                Bool.write lt mask ix True
                Word32.write lt dst ix w
            Just (w :: Word32) -> stToIO $ do
              Bool.write lt mask ix True
              Word32.write lt dst ix w
      dst' <- stToIO (Word32.unsafeFreeze dst)
      mask' <- stToIO (Bool.unsafeFreeze mask)
      pure NamedColumn{name,contents=Values MaskedColumn{mask=mask',column=Arrow.Builder.Vex.PrimitiveWord32 dst'}}
    Unsigned64 -> do
      dst <- stToIO (Word64.uninitialized n)
      mask <- stToIO (Bool.uninitialized n)
      zeroBoolArray mask
      Fin.ascendM_ n $ \(Fin ix lt) -> do
        let str = Boxed.index lt xs' ix
        if Bytes.null str
          then pure ()
          else case readMaybe (Latin1.toString str) of
            Nothing -> fail "failed to decode U64 value"
            Just (w :: Word64) -> stToIO $ do
              Bool.write lt mask ix True
              Word64.write lt dst ix w
      dst' <- stToIO (Word64.unsafeFreeze dst)
      mask' <- stToIO (Bool.unsafeFreeze mask)
      pure NamedColumn{name,contents=Values MaskedColumn{mask=mask',column=Arrow.Builder.Vex.PrimitiveWord64 dst'}}
    Signed64 -> do
      dst <- stToIO (Int64.uninitialized n)
      mask <- stToIO (Bool.uninitialized n)
      zeroBoolArray mask
      Fin.ascendM_ n $ \(Fin ix lt) -> do
        let str = Boxed.index lt xs' ix
        if Bytes.null str
          then pure ()
          else case readMaybe (Latin1.toString str) of
            Nothing -> fail "failed to decode S64 value"
            Just (w :: Int64) -> stToIO $ do
              Bool.write lt mask ix True
              Int64.write lt dst ix w
      dst' <- stToIO (Int64.unsafeFreeze dst)
      mask' <- stToIO (Bool.unsafeFreeze mask)
      pure NamedColumn{name,contents=Values MaskedColumn{mask=mask',column=Arrow.Builder.Vex.PrimitiveInt64 dst'}}
    String -> do
      dst <- stToIO (ShortText.uninitialized n)
      stToIO (ShortText.set Lte.reflexive dst Nat.zero n mempty)
      mask <- stToIO (Bool.uninitialized n)
      zeroBoolArray mask
      Fin.ascendM_ n $ \(Fin ix lt) -> do
        let str = Boxed.index lt xs' ix
        if Bytes.null str
          then pure ()
          else case TS.fromShortByteString (Bytes.toShortByteString str) of
            Nothing -> fail "failed to decode utf-8 value"
            Just (s :: ShortText) -> stToIO $ do
              Bool.write lt mask ix True
              ShortText.write lt dst ix s
      dst' <- stToIO (ShortText.unsafeFreeze dst)
      mask' <- stToIO (Bool.unsafeFreeze mask)
      pure NamedColumn{name,contents=Values MaskedColumn{mask=mask',column=Arrow.Builder.Vex.VariableBinaryUtf8 dst'}}
    Strings -> do
      dst <- stToIO (ShortTexts.uninitialized n)
      stToIO (ShortTexts.set Lte.reflexive dst Nat.zero n mempty)
      mask <- stToIO (Bool.uninitialized n)
      zeroBoolArray mask
      Fin.ascendM_ n $ \(Fin ix lt) -> do
        let str = Boxed.index lt xs' ix
        if Bytes.null str
          then pure ()
          else case traverse (TS.fromShortByteString . Bytes.toShortByteString) (Bytes.split 0x3B str) of
            Nothing -> fail "failed to decode utf-8 value in strs list"
            Just (s :: [ShortText]) -> stToIO $ do
              Bool.write lt mask ix True
              ShortTexts.write lt dst ix (Exts.fromList s)
      dst' <- stToIO (ShortTexts.unsafeFreeze dst)
      mask' <- stToIO (Bool.unsafeFreeze mask)
      pure NamedColumn{name,contents=Values MaskedColumn{mask=mask',column=Arrow.Builder.Vex.ListVariableBinaryUtf8 dst'}}

zeroBoolArray :: Bool.MutableVector RealWorld n -> IO ()
zeroBoolArray v = do
  let m = Bool.exposeMutable v
  n <- PM.getSizeofMutableByteArray m
  PM.setByteArray m 0 n (0 :: Word8)

decodeName :: Text -> IO DecodedName
decodeName x = case T.splitOn (T.singleton ':') x of
  [name,tyStr] -> do
    ty <- case tyStr of
      "u8" -> pure Unsigned8
      "u16" -> pure Unsigned16
      "u32" -> pure Unsigned32
      "u64" -> pure Unsigned64
      "s64" -> pure Signed64
      "str" -> pure String
      "strs" -> pure Strings
      _ -> fail ("unrecognized type: " ++ T.unpack tyStr)
    pure DecodedName{name,ty}
  _ -> fail ("failed to decode header: " ++ T.unpack x)

data DecodedName = DecodedName
  { name :: !Text
  , ty :: !Ty
  }

data Ty
  = Unsigned8
  | Unsigned16
  | Unsigned32
  | Unsigned64
  | Signed64
  | String
  | Strings
