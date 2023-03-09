{-# language DuplicateRecordFields #-}
{-# language OverloadedStrings #-}

import Data.Text (Text)
import Data.Bytes (Bytes)
import ArrowBuilder (NamedColumn(..))
import Data.Int
import Data.Word
import Arithmetic.Types (Fin(Fin))
import Text.Read (readMaybe)
import Data.Maybe (fromMaybe)
import Control.Monad.ST (stToIO)
import Control.Monad (when)
import GHC.Exts (RealWorld)
import Net.Types (IPv4(IPv4))

import qualified Arithmetic.Fin as Fin
import qualified Arithmetic.Nat as Nat
import qualified Arithmetic.Types as Arithmetic
import qualified Data.Builder.Catenable.Bytes as Catenable
import qualified Data.Bytes as Bytes
import qualified Data.Bytes.Chunks as Chunks
import qualified Data.Bytes.Text.Latin1 as Latin1
import qualified Data.List as List
import qualified Data.Primitive as PM
import qualified Data.Text as T
import qualified GHC.Exts as Exts
import qualified Net.IPv4 as IPv4
import qualified ArrowBuilder
import qualified System.IO as IO
import qualified Vector.Boxed as Boxed
import qualified Vector.Word8.Internal as Word8
import qualified Vector.Word16.Internal as Word16
import qualified Vector.Word32.Internal as Word32
import qualified Vector.Word64.Internal as Word64
import qualified Vector.Int64.Internal as Int64
import qualified Vector.Bool.Internal as Bool

-- The header row of the CSV needs to look like this:
--
-- > age:s32,height:s64,first_name:string
main :: IO ()
main = do
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
          Chunks.hPut h (Catenable.run (ArrowBuilder.encode n (Exts.fromList cols)))

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
      pure NamedColumn{name,mask=mask',column=ArrowBuilder.PrimitiveWord8 dst'}
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
      pure NamedColumn{name,mask=mask',column=ArrowBuilder.PrimitiveWord16 dst'}
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
      pure NamedColumn{name,mask=mask',column=ArrowBuilder.PrimitiveWord32 dst'}
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
      pure NamedColumn{name,mask=mask',column=ArrowBuilder.PrimitiveWord64 dst'}
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
      pure NamedColumn{name,mask=mask',column=ArrowBuilder.PrimitiveInt64 dst'}

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
