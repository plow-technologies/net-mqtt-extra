{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveFoldable #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveTraversable #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Network.MQTT.Connection.FilterTree
  ( FilterTree,
    empty,
    insertWith,
    insert,
    match,
    update,
    delete,
    null,
    toList,
    fromList,
    fromListWith,
    elems,
    keys,
    filter,
    mapMaybe,
    member,
  )
where

import Control.DeepSeq (NFData (..))
import Control.Monad ((>=>))
import qualified Data.Foldable as Foldable
import qualified Data.HashMap.Strict as M
import Data.Hashable (Hashable (..))
import Data.List (foldl')
import Data.Maybe (fromMaybe, isJust)
import Data.Text (intercalate, isPrefixOf)
import GHC.Generics (Generic)
import Network.MQTT.Topic as Filter
  ( Filter,
    Topic,
    mkFilter,
    split,
    toFilter,
    unFilter,
    unTopic,
  )
import Prelude hiding (filter, null)

-- | A (Filter, a) Map that allows efficient matching of 'Filter's according
-- to MQTT filter semantics. 'match' is logaritmic on the number of
-- entries in the FilterTree
data FilterTree a
  = FilterTree
      !(Maybe a)
      -- ^ # matches
      !(Maybe (FilterTree a))
      -- ^ + matches
      !(Maybe a)
      -- ^ leaf matches
      !(M.HashMap Filter (FilterTree a))
  deriving (Eq, Ord, Show, Generic, Functor, NFData, Foldable, Traversable)

instance NFData Filter where
  rnf x = rnf (unFilter x)

instance Hashable Filter where
  hashWithSalt s = hashWithSalt s . unFilter

-- | Returns all the 'Filter's and the elems in the 'FilterTree' in
-- no particular order
toList :: FilterTree a -> [(Filter, a)]
toList = go []
  where
    go parts (FilterTree mx my mz m) =
      maybe [] (\x -> [(joinParts ("#" : parts), x)]) mx
        <> maybe [] (go ("+" : parts)) my
        <> maybe [] (\x -> [(joinParts parts, x)]) mz
        <> concatMap (\(t, fs) -> go (t : parts) fs) (M.toList m)

    joinParts :: [Filter] -> Filter
    joinParts =
      fromMaybe cantHappen
        . mkFilter
        . intercalate "/"
        . map unFilter
        . reverse

    cantHappen = error "FilterTree.toList: this can't happen"

-- | Create a FilterTree from a list of (Filter, a) pairs with a custom combime
-- function
fromListWith :: (a -> a -> a) -> [(Filter, a)] -> FilterTree a
fromListWith combine = foldl' go empty
  where
    go !m (f, v) = insertWith combine f v m

-- | Create a FilterTree from a list of (Filter, a) pairs
fromList :: [(Filter, a)] -> FilterTree a
fromList = fromListWith (const id)

-- | Returns all the 'Filter's in the 'FilterTree'
keys :: FilterTree a -> [Filter]
keys = map fst . toList

-- | Returns all the elements in the 'FilterTree'
elems :: FilterTree a -> [a]
elems = Foldable.toList

-- | Returns a list of all the elems that match a 'Topic'
match :: Topic -> FilterTree a -> [a]
match topic = go (Filter.split topic)
  where
    go [] !(FilterTree x _ z _) = maybeToList x <> maybeToList z
    go (f' : rest) !(FilterTree x y _ xs)
      | "$" `isPrefixOf` unTopic f' =
          let t = Filter.toFilter f'
           in concatMap (go rest) (lookupL t xs)
      | otherwise =
          let t = Filter.toFilter f'
           in maybeToList x
                <> concatMap (go rest) (lookupL t xs)
                <> concatMap (go rest) (maybeToList y)

    lookupL x = maybeToList . M.lookup x
    maybeToList = maybe [] (: [])

-- | An empty 'FilterTree'
empty :: forall a. FilterTree a
empty = FilterTree Nothing Nothing Nothing mempty

-- | Does the 'Filter' belong to the 'FilterSet'?
member :: forall a. Filter -> FilterTree a -> Bool
member f = go (Filter.split f)
  where
    go [] !(FilterTree _ _ z _) = isJust z
    go ("#" : _) !(FilterTree x _ _ _) = isJust x
    go ("+" : rest) !(FilterTree _ y _ _) = maybe False (go rest) y
    go (f' : rest) !(FilterTree _ _ _ m) = maybe False (go rest) (M.lookup f' m)

-- | Insert a (Filter, a) pair in the 'FilterTree'. If a value already exists for
-- a 'Filter' it will be replaced
insert :: forall a. Filter -> a -> FilterTree a -> FilterTree a
insert = insertWith (const id)

-- | Insert a (Filter, a) pair in the 'FilterTree'. If a value already exists for
-- a 'Filter' it will be combined with the new value by the given function. If
-- there's no value for that 'Filter' the given default value will be used
insertWith :: forall a. (a -> a -> a) -> Filter -> a -> FilterTree a -> FilterTree a
insertWith combine f o = go (Filter.split f)
  where
    go [] !(FilterTree x y Nothing m) = FilterTree x y (Just o) m
    go [] !(FilterTree x y (Just o') m) = FilterTree x y (Just (o' `combine` o)) m
    -- A # can only go in the last topic-parts position so silently ignore
    -- invalid Filters so they don't match spuriously
    go ("#" : []) !fs = fs `combineFs` FilterTree (Just o) Nothing Nothing mempty
    go ("#" : _) !fs = fs
    go ("+" : rest) !(FilterTree x y z m) = FilterTree x y' z m
      where
        y' = Just $ go rest $ fromMaybe empty y
    go (f' : rest) !(FilterTree x y z m) = FilterTree x y z m'
      where
        m' = M.alter (Just . go rest . fromMaybe empty) f' m

    combineMaybe :: Maybe a -> Maybe a -> Maybe a
    combineMaybe Nothing Nothing = Nothing
    combineMaybe (Just x) Nothing = Just x
    combineMaybe Nothing (Just x) = Just x
    combineMaybe (Just x) (Just y) = Just (x `combine` y)

    combineFs :: FilterTree a -> FilterTree a -> FilterTree a
    combineFs (FilterTree x Nothing z m) (FilterTree x' Nothing z' m') =
      FilterTree
        (x `combineMaybe` x')
        Nothing
        (z `combineMaybe` z')
        (M.unionWith combineFs m m')
    combineFs (FilterTree x y z m) (FilterTree x' y' z' m') =
      FilterTree
        (x `combineMaybe` x')
        (Just (combineFs (fromMaybe empty y) (fromMaybe empty y')))
        (z `combineMaybe` z')
        (M.unionWith combineFs m m')

-- | Update or delete the value associated to a given 'Filter'. If the 'Filter'
-- does not exist in the set the original 'FilterTree' will be returned as-is
update :: forall a. (a -> Maybe a) -> Filter -> FilterTree a -> FilterTree a
update upd f = go (Filter.split f)
  where
    go [] !(FilterTree x y z m) = FilterTree x y (upd =<< z) m
    go ("#" : []) !(FilterTree x y z m) = FilterTree (upd =<< x) y z m
    go ("#" : _) !fs = fs
    go ("+" : rest) !(FilterTree x y z m) =
      FilterTree
        x
        (goM rest y)
        z
        m
    go (f' : rest) !(FilterTree x y z m) =
      FilterTree
        x
        y
        z
        (M.alter (goM rest) f' m)

    goM rest = fmap (go rest) >=> trim
    trim m
      | null m = Nothing
      | otherwise = Just m

-- | Is the given 'FilterTree' empty?
null :: FilterTree a -> Bool
null (FilterTree Nothing Nothing Nothing m) = M.null m
null _ = False

-- | Delete the value associaed to a given 'Filter' If the 'Filter' does not
-- belong to the set the set will be returned as-is
delete :: Filter -> FilterTree a -> FilterTree a
delete = update (const Nothing)

-- | Remove all entries from the 'FilterTree' whose element does not match
-- the given predicate
filter :: (a -> Bool) -> FilterTree a -> FilterTree a
filter f = mapMaybe (\x -> if f x then Just x else Nothing)

-- | Apply a function over all elements and remove those where
-- the function returns a Nothing
mapMaybe :: (a -> Maybe a) -> FilterTree a -> FilterTree a
mapMaybe f = fromMaybe empty . go
  where
    go !(FilterTree x y z m)
      | null fs' = Nothing
      | otherwise = Just fs'
      where
        fs' = FilterTree x' y' z' m'
        x' = x >>= f
        y' = y >>= go
        z' = z >>= f
        m' = M.mapMaybe go m
