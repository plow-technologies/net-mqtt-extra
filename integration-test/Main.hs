{-# LANGUAGE CPP #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}

-- This is a test so don't be so linty
{- HLINT ignore "Avoid restricted function" -}
{- HLINT ignore "Avoid restricted identifiers" -}

module Main (main, spec) where

import Control.Concurrent
import Control.Concurrent.Async
import Control.Concurrent.STM
import qualified Control.Concurrent.STM.TChan as TChan
import Control.Exception (IOException, throwIO)
import Control.Monad
import Control.Monad.Catch
#if !MIN_VERSION_base(4,18,0)
import Control.Monad.Cont
#endif
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Retry
import qualified Data.ByteString.Lazy.Char8 as LBS
import Data.Functor.Contravariant (contramap)
import Data.IORef
import qualified Data.Map.Strict as Map
import qualified Data.Text as T
import qualified Network.MQTT.Connection as MQTT
import Network.Socket.Free (getFreePort)
import Network.Socket.Wait as Wait (wait)
import Plow.Logging (IOTracer (..), traceWith)
import Plow.Logging.Async (withAsyncHandleTracer)
import System.FilePath ((</>))
import System.IO
import System.IO.Temp
import System.IO.Unsafe (unsafePerformIO)
import System.Mem (performGC)
import System.Posix.Signals (sigTERM, signalProcess)
import System.Process (getPid)
import System.Process.Typed
import System.Random
import System.Timeout (timeout)
import Test.HUnit.Lang (HUnitFailure)
import Test.Hspec

mosquittoPort :: Int
mosquittoPort = unsafePerformIO getFreePort
{-# NOINLINE mosquittoPort #-}

mosquittoExe :: FilePath
mosquittoExe = "mosquitto"

mqttBrokerUri :: MQTT.URI
mqttBrokerUri = case MQTT.parseURI ("mqtt://localhost:" <> show mosquittoPort) of
  Just x -> x
  Nothing -> error "shouldn't happen"

main :: IO ()
main =
  withAsyncHandleTracer stdout 100 $ \tracer -> do
    let mosquittoTracer = contramap T.pack tracer
    hspec (around (withMosquitto mosquittoTracer mosquittoPort . (\f m -> f (m, tracer))) spec)

spec :: SpecWith (MosquittoHandle, IOTracer T.Text)
spec = do
  it "can publish and subscribe" $ \(_, tracer) ->
    withTestConnection tracer $ \subConn ->
      withTestConnection tracer $ \pubConn -> do
        var <- newEmptyMVar
        mReq <- MQTT.withAsyncSubscribe subConn "test/topic" (putMVar var) $ do
          MQTT.publish pubConn "not-subscribed" "foo" False MQTT.QoS0 []
          MQTT.publish pubConn "test/topic" "hello" False MQTT.QoS0 []
          timeout 1_000_000 $ takeMVar var
        case mReq of
          Just req -> do
            MQTT._pubTopic req `shouldBe` "test/topic"
          Nothing -> expectationFailure "subscriber didn't get message"

  it "messages are only processed once" $ \(_, tracer) ->
    withTestConnection tracer $ \subConn ->
      withTestConnection tracer $ \pubConn -> do
        var <- newIORef (0 :: Int)
        let fset = Map.singleton "test/topic" (MQTT.subOptions, [])
        MQTT.withAsyncSubscribeWith subConn 5 fset Nothing (const (atomicModifyIORef' var (\c -> (c + 1, ())))) $ do
          MQTT.publish pubConn "test/topic" "hello" False MQTT.QoS0 []
          eventually $
            readIORef var `shouldReturn` 1
          eventually $
            readIORef var `shouldReturn` 1
          eventually $
            readIORef var `shouldReturn` 1

  it "can update subscriptions" $ \(_, tracer) ->
    withTestConnection tracer $ \subConn ->
      withTestConnection tracer $ \pubConn -> do
        var <- newEmptyMVar
        chan <- TChan.newTChanIO
        let fset = Map.singleton "test/topic" (MQTT.subOptions, [])
        mReq <- MQTT.withAsyncSubscribeWith2 subConn 1 fset (Just chan) (putMVar var) $ do
          MQTT.publish pubConn "test/topic" "hello" False MQTT.QoS0 []
          r1 <- timeout 1_000_000 $ takeMVar var
          let onReset = MQTT.publish pubConn "test/topic2" "hello" False MQTT.QoS0 []
          atomically $
            TChan.writeTChan chan $
              MQTT.ResetSubscriptions (Map.singleton "test/topic2" (MQTT.subOptions, [])) onReset
          r2 <- timeout 1_000_000 $ takeMVar var
          pure ((,) <$> r1 <*> r2)
        case mReq of
          Just (req1, req2) -> do
            MQTT._pubTopic req1 `shouldBe` "test/topic"
            MQTT._pubTopic req2 `shouldBe` "test/topic2"
          Nothing -> expectationFailure "subscriber didn't get message"

  it "can update subscriptions v2" $ \(_, tracer) ->
    withTestConnection tracer $ \subConn ->
      withTestConnection tracer $ \pubConn -> do
        var <- newEmptyMVar
        chan <- TChan.newTChanIO
        let fset = Map.singleton "test/topic" (MQTT.subOptions, [])
        mReq <- MQTT.withAsyncSubscribeWith2 subConn 1 fset (Just chan) (putMVar var) $ do
          MQTT.publish pubConn "test/topic" "hello" False MQTT.QoS0 []
          r1 <- timeout 1_000_000 $ takeMVar var
          let onSubscribed = MQTT.publish pubConn "test/topic2" "hello" False MQTT.QoS0 []
          atomically $ TChan.writeTChan chan $ MQTT.Subscribe "test/topic2" MQTT.subOptions [] onSubscribed
          r2 <- timeout 1_000_000 $ takeMVar var
          pure ((,) <$> r1 <*> r2)
        case mReq of
          Just (req1, req2) -> do
            MQTT._pubTopic req1 `shouldBe` "test/topic"
            MQTT._pubTopic req2 `shouldBe` "test/topic2"
          Nothing -> expectationFailure "subscriber didn't get message"

  it "can subscribe to multiple overlapping topics" $ \(_, tracer) ->
    withTestConnection tracer $ \subConn ->
      withTestConnection tracer $ \pubConn -> do
        var1 <- newEmptyMVar
        var2 <- newEmptyMVar
        mReqs <-
          MQTT.withAsyncSubscribe subConn "test/topic" (putMVar var1) $
            MQTT.withAsyncSubscribe subConn "test/+" (putMVar var2) $ do
              MQTT.publish pubConn "not-subscribed" "foo" False MQTT.QoS0 []
              MQTT.publish pubConn "test/topic" "hello" False MQTT.QoS0 []
              timeout 5_000_000 $ (,) <$> takeMVar var1 <*> takeMVar var2
        case mReqs of
          Just (req1, req2) -> do
            MQTT._pubTopic req1 `shouldBe` "test/topic"
            MQTT._pubTopic req2 `shouldBe` "test/topic"
          Nothing -> expectationFailure "subscribers didn't get message"

  it "it unsubscribes when subscriber is garbage-collected" $ \(_, tracer) -> do
    ((), subConnTrace) <- traceTestConnection tracer $ \subConn -> do
      -- We need to GC here so we can see the unsuscribes, otherwise we exit
      -- the traceTestConnection continuation causing a disconnect before
      -- having a chance to unsubscribe
      withTestConnection tracer $ \pubConn -> do
        mReqs <- do
          var1 <- newEmptyMVar
          var2 <- newEmptyMVar
          var3 <- newEmptyMVar
          MQTT.withAsyncSubscribe subConn "test/+" (putMVar var1 . MQTT._pubTopic) $ do
            MQTT.withAsyncSubscribe subConn "test/topic" (putMVar var3 . MQTT._pubTopic) $ do
              req1and2 <- MQTT.withAsyncSubscribe subConn "test/topic" (putMVar var2 . MQTT._pubTopic) $ do
                MQTT.publish pubConn "not-subscribed" "foo" False MQTT.QoS0 []
                MQTT.publish pubConn "test/topic" "hello" False MQTT.QoS0 []
                timeout 5_000_000 $ (,) <$> takeMVar var1 <*> takeMVar var2
              MQTT.publish pubConn "test/topic" "hello" False MQTT.QoS0 []
              req3 <- timeout 5_000_000 $ takeMVar var3
              pure (req1and2, req3)
        case mReqs of
          (Just (req1, req2), Just req3) -> do
            req1 `shouldBe` "test/topic"
            req2 `shouldBe` "test/topic"
            req3 `shouldBe` "test/topic"
          _ -> expectationFailure "subscribers didn't get message"
        replicateM_ 10 (threadDelay 1000 >> performGC)
    length (filter (\case MQTT.Unsubscribing{} -> True; _ -> False) subConnTrace) `shouldBe` 2
    length (filter (\case MQTT.Unsubscribing _ t -> "test/topic" `elem` t; _ -> False) subConnTrace) `shouldBe` 1
    length (filter (\case MQTT.Unsubscribing _ t -> "test/+" `elem` t; _ -> False) subConnTrace) `shouldBe` 1

  it "it re-connects when broker goes down" $ \(mosquitto, tracer) -> do
    reconnectCountRef <- newTVarIO (0 :: Int)
    (mReqs, subConnTrace) <- traceTestConnection tracer $ \subConn ->
      withTestConnection tracer $ \pubConn -> do
        MQTT.onReconnect pubConn (atomically (modifyTVar reconnectCountRef (+ 1)))
        performGC
        var1 <- newEmptyMVar
        var2 <- newEmptyMVar
        MQTT.withAsyncSubscribe subConn "test/topic" (putMVar var1) $
          MQTT.withAsyncSubscribe subConn "test/+" (putMVar var2) $ do
            -- Kill mosquitto here, after subscribing, so we can test that after
            -- reconnecting it re-subscribes to existing subscriptions
            killMosquitto mosquitto
            -- block until both connections are down before restrating
            -- mosquitto (for predictability in tests)
            atomically ((||) <$> MQTT.isConnectedSTM pubConn <*> MQTT.isConnectedSTM subConn >>= check . not)
            withMosquitto (contramap T.pack tracer) mosquittoPort $ \_mosquitto2 -> do
              -- block until connections are back up and
              -- resubcsription occurs, otherwise published messages may get lost
              atomically ((&&) <$> MQTT.isConnectedSTM pubConn <*> MQTT.isConnectedSTM subConn >>= check)
              traceWith tracer "re-connected to second mosquitto"
              MQTT.publish pubConn "not-subscribed" "foo" False MQTT.QoS0 []
              MQTT.publish pubConn "test/topic" "hello" False MQTT.QoS0 []
              ret <- timeout 1_000_0000 $ (,) <$> takeMVar var1 <*> takeMVar var2
              -- Close connections here so no more re-connections are attempted
              -- after second mosquitto is closed. Otherwise we get
              -- un-deterministic re-connection and re-subscribe attempt counts.
              -- It also indirectly checks that closeConnection is idempotent
              MQTT.closeConnection pubConn
              MQTT.closeConnection subConn
              pure ret
    case mReqs of
      Just (req1, req2) -> do
        MQTT._pubTopic req1 `shouldBe` "test/topic"
        MQTT._pubTopic req2 `shouldBe` "test/topic"
      Nothing -> expectationFailure "subscribers didn't get message"
    length (filter (\case MQTT.Subscribed _ f _ -> "test/topic" `elem` f; _ -> False) subConnTrace) `shouldBe` 2
    readTVarIO reconnectCountRef `shouldReturn` 1

  it "cannot publish on closed connection" $ \(_, tracer) -> do
    conn <- withTestConnection tracer pure
    MQTT.publish conn "test/topic" "hello" False MQTT.QoS1 []
      `shouldThrow` \case MQTT.ConnectionIsClosed{} -> True; _ -> False

  it "pubSubTimeout is honored" $ \(mosquitto, tracer) -> do
    killMosquitto mosquitto
    MQTT.withConnection
      (MQTT.defaultSettings mqttBrokerUri)
        { MQTT.tracer = traceWith (contramap MQTT.showConnectionTrace tracer)
        , MQTT.connectRetryPolicy = constantDelay 30_000_000
        , MQTT.failFast = False
        , MQTT.pubSubTimeout = Just 50
        }
      $ \conn ->
        MQTT.publish conn "test/topic" "testing" False MQTT.QoS1 []
          `shouldThrow` \case MQTT.PubSubTimeout{} -> True; _ -> False

  it "can publish concurrently" $ \(_, tracer) ->
    withTestConnection tracer $ \subConn ->
      MQTT.withConnection
        (MQTT.defaultSettings mqttBrokerUri)
          { MQTT.tracer = traceWith (contramap MQTT.showConnectionTrace tracer)
          , MQTT.connectRetryPolicy = constantDelay 30_000_000
          , MQTT.failFast = False
          , MQTT.pubSubTimeout = Just 50
          }
        $ \pubConn -> do
          var <- newIORef (0 :: Int)
          MQTT.withAsyncSubscribe subConn "test/topic" (atomicModifyIORef' var . (\n x -> (x + n, ())) . read . LBS.unpack . MQTT._pubBody) $ do
            let nums = [0 .. 5000] :: [Int]
            forConcurrently_ nums $ \x -> do
              randomIO >>= \case
                True -> threadDelay =<< randomRIO (0, 10_000)
                False -> pure ()
              qos <-
                randomRIO @Int (1, 2) >>= \case
                  1 -> pure MQTT.QoS1
                  _ -> pure MQTT.QoS2
              retryingTimeout $
                MQTT.publish pubConn "test/topic" (LBS.pack (show x)) False qos []
            eventually $
              readIORef var `shouldReturn` sum nums

  it "publishes last-will when withConnection's continuation throws" $ \(_, tracer) ->
    withTestConnection tracer $ \subConn -> do
      var <- newIORef False
      MQTT.withAsyncSubscribe subConn "lwt-topic" (const (writeIORef var True)) $ do
        void $ async $ do
          MQTT.withConnection
            (MQTT.defaultSettings mqttBrokerUri)
              { MQTT.tracer = traceWith (contramap MQTT.showConnectionTrace tracer)
              , MQTT.connectRetryPolicy = constantDelay 30_000_000
              , MQTT.failFast = True
              , MQTT.pubSubTimeout = Just 50
              , MQTT.lwt =
                  pure $
                    Just $
                      MQTT.LastWill
                        { MQTT._willRetain = False
                        , MQTT._willQoS = MQTT.QoS0
                        , MQTT._willTopic = "lwt-topic"
                        , MQTT._willMsg = "my last of wills shall be honored"
                        , MQTT._willProps = []
                        }
              }
            $ \_ -> throwIO $ userError "et tu, Brute?"
        eventually $
          readIORef var `shouldReturn` True

  it "it sends DISCONNECT when withConnection's continuation exits gracefully" $ \(_, tracer) -> do
    ((), connTrace) <- traceTestConnection tracer $ const (pure ())
    length (filter (\case MQTT.ConnectionClosed _ (Just MQTT.DiscoNormalDisconnection) -> True; _ -> False) connTrace)
      `shouldBe` 1

withTestConnection ::
  (MonadIO m, MonadMask m) => IOTracer T.Text -> (MQTT.Connection -> m a) -> m a
withTestConnection rawTracer =
  MQTT.withConnection
    (MQTT.defaultSettings mqttBrokerUri)
      { MQTT.tracer = traceWith (contramap MQTT.showConnectionTrace rawTracer)
      , MQTT.connectRetryPolicy = constantDelay 100_000
      , MQTT.failFast = True
      }

traceTestConnection ::
  (MonadIO m, MonadMask m) => IOTracer T.Text -> (MQTT.Connection -> m a) -> m (a, [MQTT.ConnectionTrace])
traceTestConnection tracer f = do
  logRef <- liftIO $ newIORef []
  ret <-
    MQTT.withConnection
      ( (MQTT.defaultSettings mqttBrokerUri)
          { MQTT.tracer = \x -> atomicModifyIORef' logRef (\xs -> (x : xs, ())) >> traceWith (contramap MQTT.showConnectionTrace tracer) x
          , MQTT.connectRetryPolicy = constantDelay 100_000
          , MQTT.failFast = True
          }
      )
      f
  (ret,) . reverse <$> liftIO (readIORef logRef)

type MosquittoHandle = Process () Handle Handle

killMosquitto :: (MonadIO m) => MosquittoHandle -> m ()
killMosquitto h =
  liftIO $ do
    getPid (unsafeProcessHandle h) >>= mapM_ (signalProcess sigTERM)
    void $ waitExitCode h

withMosquitto ::
  IOTracer String ->
  Int ->
  (MosquittoHandle -> IO a) ->
  IO a
withMosquitto tracer p f = withSystemTempDirectory "mosquitto" $ \workDir -> do
  let configFile = workDir </> "mosquitto.conf"
  writeFile configFile mosquittoConf
  let procConfig = setWorkingDir workDir $ proc mosquittoExe ["-v", "-c", configFile]
  withNamedProcessTerm tracer "mosquitto" procConfig $ \h -> do
    traceWith tracer "waiting for mosquitto"
    Wait.wait "localhost" mosquittoPort
    traceWith tracer "mosquitto is up"
    f h
  where
    mosquittoConf =
      unlines
        [ "allow_anonymous true"
        , "port " <> show p
        , "log_type debug"
        ]

withNamedProcessTerm ::
  IOTracer String ->
  String ->
  ProcessConfig stdin stdout stderr ->
  (Process () Handle Handle -> IO a) ->
  IO a
withNamedProcessTerm tracer name cfg f = do
  withProcessTermKillable (setStdout createPipe $ setStderr createPipe $ setStdin nullStream cfg) $ \p ->
    withAsync (forever (hGetLine (getStdout p) >>= \l -> traceWith tracer (name <> " (stdout): " <> l))) $ \_ ->
      withAsync (forever (hGetLine (getStderr p) >>= \l -> traceWith tracer (name <> " (stderr): " <> l))) $ \_ -> do
        mECode <- getExitCode p
        case mECode of
          Just eCode -> throwIO (userError ("Process " <> name <> " exited with " <> show eCode))
          Nothing -> f p

withProcessTermKillable ::
  ProcessConfig stdin stdout stderr ->
  (Process stdin stdout stderr -> IO a) ->
  IO a
withProcessTermKillable config =
  bracket (startProcess config) (flip catch (\(_ :: IOException) -> pure ()) . stopProcess)

-- | Retry an assertion several times during a maximum time of 5 seconds
eventually :: (MonadIO m, MonadMask m) => m a -> m a
eventually =
  recovering
    (limitRetriesByCumulativeDelay 20_000_000 (fullJitterBackoff 10))
    [ const (Handler (\(_ :: HUnitFailure) -> pure True))
    , const (Handler (\(_ :: SomeException) -> pure False))
    ]
    . const

retryingTimeout :: (MonadIO m, MonadMask m) => m a -> m a
retryingTimeout =
  recovering
    (constantDelay 10)
    [ const (Handler (\case MQTT.PubSubTimeout{} -> pure True; _ -> pure False))
    , const (Handler (\(_ :: SomeException) -> pure False))
    ]
    . const
