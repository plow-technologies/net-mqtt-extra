{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module Main (main) where

import Data.Maybe
import Network.MQTT.Connection
import System.Environment (getArgs)
import System.Exit (die)

main :: IO ()
main = do
  args <- getArgs
  case args of
    ["server"] -> do
      server
    ["client"] -> client
    _ -> die "Usage: net-mqtt-extra-demo (server | client)"

server :: IO ()
server = do
  let uri = fromJust $ parseURI "mqtt://localhost:56789#server"
  conn <-
    newConnection
      ( (defaultSettings uri)
          { connectTimeout = 1_000_000
          }
      )
  onReconnect conn $ putStrLn "re-connected"
  subscribe conn "testing/foo" print
  closeConnection conn
  putStrLn "closed connection"

client :: IO ()
client = do
  let uri = fromJust $ parseURI "mqtt://localhost:56789#server"
  conn <- newConnection ((defaultSettings uri) {tracer = printConnectionTrace})
  publish conn "testing/foo" "hello!" False QoS0 []
