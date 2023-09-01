{-# language DuplicateRecordFields #-}
{-# language LambdaCase #-}
{-# language PatternSynonyms #-}

module ArrowSchema
  ( Field(..)
  , Type(..)
  , Schema(..)
  , TableInt(..)
  , TableFixedSizeBinary(..)
  , TableTimestamp(..)
  , TableDate(..)
  , Buffer(..)
  , TimeUnit(..)
  , DateUnit(..)
  , DictionaryEncoding(..)
  , encodeSchema
    -- * Time Units
  , pattern Second
  , pattern Millisecond
  , pattern Microsecond
  , pattern Nanosecond
    -- * Time Units
  , pattern Day
  , pattern DateMillisecond
  ) where

import Data.Word (Word16)
import Data.Int (Int32)
import Data.Primitive (SmallArray)
import Data.Text (Text)
import Data.Int

import qualified Flatbuffers.Builder as B
import qualified GHC.Exts as Exts

newtype TimeUnit = TimeUnit Word16

pattern Second :: TimeUnit
pattern Second = TimeUnit 0

pattern Millisecond :: TimeUnit
pattern Millisecond = TimeUnit 1

pattern Microsecond :: TimeUnit
pattern Microsecond = TimeUnit 2

pattern Nanosecond :: TimeUnit
pattern Nanosecond = TimeUnit 3

newtype DateUnit = DateUnit Word16

pattern Day :: DateUnit
pattern Day = DateUnit 0

pattern DateMillisecond :: DateUnit
pattern DateMillisecond = DateUnit 1

-- | Corresponding schema at @schema/arrow-schema.fbs@:
data Schema = Schema
  { endianness :: !Word16
  , fields :: !(SmallArray Field)
  }

-- This is a flatbuffers struct
data Buffer = Buffer
  { offset :: !Int64
  , length :: !Int64
  }

-- Note, the dictionaryKind field is omitted since it only has a single
-- inhabitant.
data DictionaryEncoding = DictionaryEncoding
  { id :: !Int64
  , indexType :: !TableInt
  , isOrdered :: !Bool
  }

data Field = Field
  { name :: !Text
  , nullable :: !Bool
  , type_ :: Type
  , dictionary :: !(Maybe DictionaryEncoding)
  , children :: !(SmallArray Field)
  }

data Type
  = Null
  | Int TableInt
  | FixedSizeBinary !TableFixedSizeBinary
  | Utf8
  | Bool
  | Timestamp !TableTimestamp
  | Date !TableDate
  | Duration !TimeUnit
  | Struct
  | List

newtype TableFixedSizeBinary = TableFixedSizeBinary
  { byteWidth :: Int32
  }

data TableInt = TableInt
  { bitWidth :: !Int32
  , isSigned :: !Bool
  }

data TableTimestamp = TableTimestamp
  { unit :: !TimeUnit
  , timezone :: !Text
  }

data TableDate = TableDate
  { unit :: !DateUnit
  }

encodeSchema :: Schema -> B.Object
encodeSchema Schema{endianness,fields} = B.Object $ Exts.fromList
  [ B.unsigned16 endianness
  , B.objects (fmap encodeField fields)
  ]

encodeField :: Field -> B.Object
encodeField Field{name,nullable,type_,dictionary,children} = B.Object $ Exts.fromList
  [ B.text name
  , B.boolean nullable
  , B.union (encodeType type_)
  , case dictionary of {Nothing -> B.absent; Just d -> B.FieldObject (encodeDictionaryEncoding d)}
  , B.objects (fmap encodeField children)
  ]

encodeDictionaryEncoding :: DictionaryEncoding -> B.Object
encodeDictionaryEncoding DictionaryEncoding{id,indexType,isOrdered} = B.Object $ Exts.fromList
  [ B.signed64 id
  , B.FieldObject (encodeTableInt indexType)
  , B.boolean isOrdered
  , B.unsigned16 0 -- the dictionary kind: DenseArray
  ]

encodeTableInt :: TableInt -> B.Object
encodeTableInt TableInt{bitWidth,isSigned} = B.Object $ Exts.fromList
  [ B.signed32 bitWidth
  , B.boolean isSigned
  ]

encodeTableTimestamp :: TableTimestamp -> B.Object
encodeTableTimestamp TableTimestamp{unit=TimeUnit w,timezone} =
  B.Object $ Exts.fromList
    [ B.unsigned16 w
    , B.text timezone
    ]

encodeTableDate :: TableDate -> B.Object
encodeTableDate TableDate{unit=DateUnit w} =
  B.Object $ Exts.fromList
    [ B.unsigned16 w
    ]

encodeTableBinary :: TableFixedSizeBinary -> B.Object
encodeTableBinary TableFixedSizeBinary{byteWidth} = B.Object $ Exts.fromList
  [ B.signed32 byteWidth
  ]

encodeType :: Type -> B.Union
encodeType = \case
  Null -> B.Union{tag=1,object=B.Object mempty}
  Int table -> B.Union{tag=2,object=encodeTableInt table}
  Utf8 -> B.Union{tag=5,object=B.Object mempty}
  Bool -> B.Union{tag=6,object=B.Object mempty}
  FixedSizeBinary table -> B.Union{tag=15,object=encodeTableBinary table}
  Date table -> B.Union{tag=8,object=encodeTableDate table}
  Timestamp table -> B.Union{tag=10,object=encodeTableTimestamp table}
  Duration (TimeUnit w) -> B.Union{tag=18,object=B.Object $ Exts.fromList [B.unsigned16 w]}
  List -> B.Union{tag=12,object=B.Object mempty}
  Struct -> B.Union{tag=13,object=B.Object mempty}
