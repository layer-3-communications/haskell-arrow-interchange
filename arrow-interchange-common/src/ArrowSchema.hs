{-# language DuplicateRecordFields #-}
{-# language MagicHash #-}
{-# language UnboxedTuples #-}
{-# language LambdaCase #-}
{-# language PatternSynonyms #-}

module ArrowSchema
  ( Field(..)
  , Type(..)
  , Schema(..)
  , TableInt(..)
  , TableFixedSizeBinary(..)
  , TableFixedSizeList(..)
  , TableTimestamp(..)
  , TableDate(..)
  , TableMap(..)
  , Buffer(..)
  , TimeUnit(..)
  , DateUnit(..)
  , DictionaryEncoding(..)
  , encodeSchema
  , parseSchema
    -- * Time Units
  , pattern Second
  , pattern Millisecond
  , pattern Microsecond
  , pattern Nanosecond
    -- * Time Units
  , pattern Day
  , pattern DateMillisecond
  ) where

import Prelude hiding (id)

import Data.Word (Word16)
import Data.Primitive (SmallArray)
import Data.Text (Text)
import Data.Functor (($>))
import Data.Int
import GHC.Exts ((+#),(*#))
import Data.Primitive.Types (indexByteArray#,writeByteArray#,readByteArray#)

import qualified Data.Primitive as PM
import qualified Flatbuffers.Builder as B
import qualified Flatbuffers.Parser as P
import qualified GHC.Exts as Exts

newtype TimeUnit = TimeUnit Word16
  deriving Show

pattern Second :: TimeUnit
pattern Second = TimeUnit 0

pattern Millisecond :: TimeUnit
pattern Millisecond = TimeUnit 1

pattern Microsecond :: TimeUnit
pattern Microsecond = TimeUnit 2

pattern Nanosecond :: TimeUnit
pattern Nanosecond = TimeUnit 3

newtype DateUnit = DateUnit Word16
  deriving Show

pattern Day :: DateUnit
pattern Day = DateUnit 0

pattern DateMillisecond :: DateUnit
pattern DateMillisecond = DateUnit 1

-- | Corresponding schema at @schema/arrow-schema.fbs@:
data Schema = Schema
  { endianness :: !Word16
  , fields :: !(SmallArray Field)
    -- Think about adding the features field at some point. This might
    -- not actually be used by any clients though.
  } deriving (Show)

-- This is a flatbuffers struct
data Buffer = Buffer
  { offset :: !Int64
  , length :: !Int64
  } deriving (Show)

instance PM.Prim Buffer where
  sizeOf# _ = 16#
  alignment# _ = 8#
  writeByteArray# arr# i# (Buffer a b) =
    \s0 -> case writeByteArray# arr# (2# *# i#) a s0 of
       s1 -> writeByteArray# arr# ((2# *# i#) +# 1# ) b s1
  readByteArray# arr# i# s0 = case readByteArray# arr# (2# *# i#) s0 of
    (# s1, (offset :: Int64) #) -> case readByteArray# arr# ((2# *# i#) +# 1# ) s1 of
      (# s2, (len :: Int64) #) -> (# s2, Buffer{offset,length=len} #)
  indexByteArray# arr# i# = Buffer
    (indexByteArray# arr# (i# *# 2#))
    (indexByteArray# arr# ((i# *# 2#) +# 1#))
  setByteArray# = PM.defaultSetByteArray#

-- Note, the dictionaryKind field is omitted since it only has a single
-- inhabitant.
data DictionaryEncoding = DictionaryEncoding
  { id :: !Int64
  , indexType :: !TableInt
  , isOrdered :: !Bool
  } deriving Show

data Field = Field
  { name :: !Text
  , nullable :: !Bool
  , type_ :: Type
  , dictionary :: !(Maybe DictionaryEncoding)
  , children :: !(SmallArray Field)
  } deriving (Show)

data Type
  = Null
  | Int TableInt
  | FixedSizeBinary !TableFixedSizeBinary
  | FixedSizeList !TableFixedSizeList
  | Utf8
  | Bool
  | Timestamp !TableTimestamp
  | Date !TableDate
  | Duration !TimeUnit
  | Struct
  | List -- tag 12
  | Map !TableMap -- tag 17
  deriving (Show)

newtype TableFixedSizeBinary = TableFixedSizeBinary
  { byteWidth :: Int32
  } deriving (Show)

newtype TableFixedSizeList = TableFixedSizeList
  { listSize :: Int32
  } deriving (Show)

newtype TableMap = TableMap
  { keysSorted :: Bool
  } deriving (Show)

data TableInt = TableInt
  { bitWidth :: !Int32
  , isSigned :: !Bool
  } deriving (Show)

data TableTimestamp = TableTimestamp
  { unit :: !TimeUnit
  , timezone :: !Text
  } deriving (Show)

data TableDate = TableDate
  { unit :: !DateUnit
  } deriving (Show)

encodeSchema :: Schema -> B.Object
encodeSchema Schema{endianness,fields} = B.Object $ Exts.fromList
  [ B.unsigned16 endianness
  , B.objects (fmap encodeField fields)
  ]

parseSchema :: P.TableParser Schema
parseSchema = Schema
  <$> P.word16
  <*> P.array parseField

encodeField :: Field -> B.Object
encodeField Field{name,nullable,type_,dictionary,children} = B.Object $ Exts.fromList
  [ B.text name
  , B.boolean nullable
  , B.union (encodeType type_)
  , case dictionary of {Nothing -> B.absent; Just d -> B.FieldObject (encodeDictionaryEncoding d)}
  , B.objects (fmap encodeField children)
  ]

parseField :: P.TableParser Field
parseField = Field
  <$> P.string
  <*> P.boolean
  <*> P.union parseType
  <*> (P.ignore $> Nothing)
  <*> (P.ignore $> mempty)

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

encodeTableMap :: TableMap -> B.Object
encodeTableMap TableMap{keysSorted} = B.Object $ Exts.fromList
  [ B.boolean keysSorted
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

encodeTableFixedSizeList :: TableFixedSizeList -> B.Object
encodeTableFixedSizeList TableFixedSizeList{listSize} = B.Object $ Exts.fromList
  [ B.signed32 listSize
  ]

encodeType :: Type -> B.Union
encodeType = \case
  Null -> B.Union{tag=1,object=B.Object mempty}
  Int table -> B.Union{tag=2,object=encodeTableInt table}
  Utf8 -> B.Union{tag=5,object=B.Object mempty}
  Bool -> B.Union{tag=6,object=B.Object mempty}
  FixedSizeBinary table -> B.Union{tag=15,object=encodeTableBinary table}
  FixedSizeList table -> B.Union{tag=16,object=encodeTableFixedSizeList table}
  Date table -> B.Union{tag=8,object=encodeTableDate table}
  Timestamp table -> B.Union{tag=10,object=encodeTableTimestamp table}
  Duration (TimeUnit w) -> B.Union{tag=18,object=B.Object $ Exts.fromList [B.unsigned16 w]}
  List -> B.Union{tag=12,object=B.Object mempty}
  Struct -> B.Union{tag=13,object=B.Object mempty}
  Map table -> B.Union{tag=17,object=encodeTableMap table}

parseType :: P.UnionParser Type
parseType = P.constructUnionFromList
  [ pure Null
  , Int <$> (TableInt <$> P.int32 <*> P.boolean)
  , P.tableParserThrow (P.UnsupportedUnionTag 3)
  , P.tableParserThrow (P.UnsupportedUnionTag 4)
  , pure Utf8
  , P.tableParserThrow (P.UnsupportedUnionTag 6)
  , P.tableParserThrow (P.UnsupportedUnionTag 7)
  , P.tableParserThrow (P.UnsupportedUnionTag 8)
  , P.tableParserThrow (P.UnsupportedUnionTag 9)
  , Timestamp <$> (TableTimestamp <$> fmap TimeUnit P.word16 <*> P.string)
  ]
