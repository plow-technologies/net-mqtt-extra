module Main (main, spec) where

import qualified Network.MQTT.Connection.FilterTreeSpec as FilterTreeSpec (spec)
import Test.Hspec (Spec, hspec)

main :: IO ()
main = hspec spec

spec :: Spec
spec = do
  FilterTreeSpec.spec
