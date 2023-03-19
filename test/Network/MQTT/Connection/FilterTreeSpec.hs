{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Network.MQTT.Connection.FilterTreeSpec (spec) where

import Control.Monad (forM_)
import Data.Containers.ListUtils (nubOrd)
import qualified Data.Set as Set
import Network.MQTT.Arbitrary (MatchingTopic (..))
import qualified Network.MQTT.Connection.FilterTree as FT
import Network.MQTT.Topic (Filter, unFilter)
import qualified Network.MQTT.Topic as MQTT
import Test.Hspec (Spec, describe, it, shouldBe, shouldMatchList)
import Test.Hspec.QuickCheck (modifyMaxDiscardRatio, prop)
import Test.QuickCheck ((==>))

spec :: Spec
spec = describe "Network.MQTT.Connection.FilterTreeSpec" $ modifyMaxDiscardRatio (* 100) $ do
  prop "# matches every topic" $ \topic -> do
    let fs = add "#" FT.empty
    FT.match topic fs `shouldBe` [["#"]]

  prop "matches only every topic it should" $ \(MatchingTopic (topic, filts), nonMatching) ->
    all (not . flip MQTT.match topic) nonMatching ==> do
      let fs = foldr add FT.empty (filts <> nonMatching)
      concat (FT.match topic fs) `shouldMatchList` filts

  prop "member returns True for existing members" $ \(filts :: [Filter], filts2 :: [Filter]) ->
    all (`notElem` filts) filts2 ==> do
      let fs = foldr add FT.empty filts
      all (flip FT.member fs) filts `shouldBe` True
      all (not . flip FT.member fs) filts2 `shouldBe` True

  prop "toList returns correct result" $ \filts ->
    length (Set.fromList filts)
      == length filts
      ==> let fs = foldr add FT.empty filts
           in FT.toList fs `shouldMatchList` map (\x -> (x, [x])) filts

  prop "keys returns correct result" $ \filts ->
    length (Set.fromList filts)
      == length filts
      ==> let fs = foldr add FT.empty filts
           in FT.keys fs `shouldMatchList` filts

  prop "elems returns correct result" $ \filts ->
    length (Set.fromList filts)
      == length filts
      ==> let fs = foldr add FT.empty filts
           in FT.elems fs `shouldMatchList` map (: []) filts

  prop "can delete filter" $ \(MatchingTopic (topic, filts)) -> do
    let fs = foldr add FT.empty filts
        fs' = foldr FT.delete fs filts
    FT.match topic fs' `shouldBe` []

  prop "can replace filter" $ \(MatchingTopic (topic, filts')) -> do
    let filts = nubOrd filts'
    let fs = foldr add FT.empty filts
    concat (FT.match topic fs) `shouldMatchList` filts
    let fs' = foldr (\t -> FT.insert t ["replaced"]) fs filts
    concat (FT.match topic fs') `shouldBe` replicate (length filts) "replaced"

  it "matches topics with + in start" $ do
    let fs = add "+/b/c" $ add "a/b/c" FT.empty
    FT.match "b/b/c" fs `shouldBe` [["+/b/c"]]
    FT.match "a/b/c" fs `shouldMatchList` [["+/b/c"], ["a/b/c"]]

  it "matches topics with + in middle" $ do
    let fs = add "a/+/c" $ add "a/b/c" FT.empty
    FT.match "a/a/c" fs `shouldBe` [["a/+/c"]]
    FT.match "a/b/c" fs `shouldMatchList` [["a/+/c"], ["a/b/c"]]

  it "matches topics with + in end" $ do
    let fs = add "a/b/+" $ add "a/b/c" FT.empty
    FT.match "a/b/b" fs `shouldBe` [["a/b/+"]]
    FT.match "a/b/c" fs `shouldMatchList` [["a/b/+"], ["a/b/c"]]

  let allTopics =
        [ "a",
          "a/b",
          "a/b/c/d",
          "b/a/c/d",
          "$SYS/a/b",
          "a/$SYS/b"
        ]
      tsts =
        [ ("a", ["a"]),
          ("a/#", ["a", "a/b", "a/b/c/d"]),
          ("+/b", ["a/b"]),
          ("+/+/c/+", ["a/b/c/d", "b/a/c/d"]),
          ("+/+/b", []),
          ("+/$SYS/b", ["a/$SYS/b"]),
          ("$SYS/#", ["$SYS/a/b"]),
          ("+/$SYS/+", ["a/$SYS/b"]),
          ("#/b", [])
        ]
  forM_ tsts $ \(p, want) ->
    it ("handles " <> show (unFilter p)) $
      let fs = add p FT.empty
          match t = not $ null $ FT.match t fs
       in filter match allTopics `shouldBe` want

add :: Filter -> FT.FilterTree [Filter] -> FT.FilterTree [Filter]
add f = FT.insertWith (<>) f [f]
