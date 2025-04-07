{-# language TypeApplications #-}
{-# language TypeOperators #-}
{-# language DataKinds #-}
{-# language MagicHash #-}

module Arrow.Vext.VariableBinaryUtf8
  ( VariableBinary(..)
  , index
  ) where

import Arrow.Vext (VariableBinary(..))

import Arithmetic.Types (Fin#,Fin32#)
import Data.Bytes.Types (ByteArrayN(ByteArrayN))
import Data.Text (Text)
import GHC.TypeNats (type (+))

import qualified Arithmetic.Fin as Fin
import qualified Arithmetic.Nat as Nat
import qualified Data.Text.Internal
import qualified GHC.Exts as Exts
import qualified Vector.Int32 as Int32


-- | Index into a VarBinary block that is known to be UTF-8 encoded text.
-- This does not verify that the contents are UTF-8 encoded text. If the
-- slice would have a negative length, this returns the empty text instead.
index :: VariableBinary n -> Fin# n -> Text
{-# inline index #-}
index (VariableBinary contents ixs) fin =
  let !ixStart = Int32.index ixs (Fin.weakenR# @_ @1 fin)
      !ixEnd = Int32.index ixs (Fin.incrementR# Nat.N1# fin)
   in sliceByteArrayToText contents ixStart ixEnd

sliceByteArrayToText :: ByteArrayN n -> Fin32# (n + 1) -> Fin32# (n + 1) -> Text
{-# inline sliceByteArrayToText #-}
sliceByteArrayToText (ByteArrayN arr) a b =
  let a' = Exts.I# (Exts.int32ToInt# (Fin.demote32# a))
      b' = Exts.I# (Exts.int32ToInt# (Fin.demote32# b))
   in if a' >= b' then Data.Text.Internal.Text arr 0 0 else Data.Text.Internal.Text arr a' (b' - a')

