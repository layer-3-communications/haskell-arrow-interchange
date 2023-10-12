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
  , encodeSchema
  , decodeSchema
  , decodeSchemaInternal
  , decodeBufferInternal
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
import Data.Bytes (Bytes)

import qualified Flatbuffers.Builder as B
import qualified GHC.Exts as Exts
import qualified ArrowTemplateHaskell as G
import qualified Data.ByteString.Lazy as LBS
import qualified Data.Bytes as Bytes
import qualified FlatBuffers as FB
import qualified FlatBuffers.Vector as FBV

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

decodeSchema :: Bytes -> Either String Schema
decodeSchema !b = do
  theSchema <- FB.decode (LBS.fromStrict (Bytes.toByteString b))
  decodeSchemaInternal theSchema

decodeSchemaInternal :: FB.Table G.Schema -> Either String Schema
decodeSchemaInternal theSchema = do
  theEndianness <- G.schemaEndianness theSchema
  theFields <- G.schemaFields theSchema >>= \case
    Nothing -> pure mempty
    Just theFields -> do
      theDecodedFields <- FBV.toList theFields >>= traverse decodeField
      pure (Exts.fromList theDecodedFields)
  pure Schema
    { endianness = fromIntegral theEndianness
    , fields = theFields
    }

decodeType :: FB.Union G.Type -> Either String Type
decodeType theType = case theType of
  FB.UnionNone -> Left "Got None when looking for Type union"
  FB.UnionUnknown{} -> Left "Got unrecognized tag when looking for Type union"
  FB.Union ty -> case ty of
    G.TypeUtf8 _ -> Right Utf8
    G.TypeTInt t -> Int <$> decodeTableInt t
    _ -> Left "decodeType: finish mapping these"

decodeTableInt :: FB.Table G.TInt -> Either String TableInt
decodeTableInt x = do
  isSigned <- G.tIntIsSigned x
  bitWidth <- G.tIntBitWidth x
  pure TableInt{isSigned,bitWidth}

decodeField :: FB.Table G.Field -> Either String Field
decodeField theField = do
  name <- G.fieldName theField >>= \case
    Nothing -> Left "Field name was missing"
    Just name -> pure name
  nullable <- G.fieldNullable theField
  typ <- G.fieldTyp theField >>= decodeType
  children <- G.fieldChildren theField >>= \case
    Nothing -> pure mempty
    Just theFields -> do
      theDecodedFields <- FBV.toList theFields >>= traverse decodeField
      pure (Exts.fromList theDecodedFields)
  pure Field
    { name = name
    , nullable = nullable
    , dictionary = ()
    , type_ = typ
    , children = children
    }

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

decodeBufferInternal :: FB.Struct G.Buffer -> Either String Buffer
decodeBufferInternal x = do
  offset <- G.bufferOffset x
  len <- G.bufferLength x
  pure Buffer{offset,length=len}

data Field = Field
  { name :: !Text
  , nullable :: !Bool
  , type_ :: !Type
  , dictionary :: !() -- omitting this for now. add it later
  , children :: !(SmallArray Field)
  }

data Type
  = Null
  | Int TableInt
  | FixedSizeBinary !TableFixedSizeBinary
  | Binary
  | Utf8
  | Bool
  | Timestamp !TableTimestamp
  | Date !TableDate
  | Duration !TimeUnit

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
encodeField Field{name,nullable,type_,children} = B.Object $ Exts.fromList
  [ B.text name
  , B.boolean nullable
  , B.union (encodeType type_)
  , B.absent
  , B.objects (fmap encodeField children)
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
  Binary -> B.Union{tag=4,object=B.Object mempty}
  Utf8 -> B.Union{tag=5,object=B.Object mempty}
  Bool -> B.Union{tag=6,object=B.Object mempty}
  FixedSizeBinary table -> B.Union{tag=15,object=encodeTableBinary table}
  Date table -> B.Union{tag=8,object=encodeTableDate table}
  Timestamp table -> B.Union{tag=10,object=encodeTableTimestamp table}
  Duration (TimeUnit w) -> B.Union{tag=18,object=B.Object $ Exts.fromList [B.unsigned16 w]}
