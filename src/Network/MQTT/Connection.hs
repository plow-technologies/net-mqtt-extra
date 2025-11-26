{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecursiveDo #-}
{-# LANGUAGE TypeApplications #-}
{-# OPTIONS_GHC -fno-warn-unrecognised-pragmas #-}

{- HLINT ignore "Avoid restricted function" -}
{-ignore threadDelay because the only use in this module does not overflow 32
 - bits and ignore hPutStrLn because it is used in a helper to display plow-log
 - exceptions -}

-- |
-- Module      : Network.MQTT.Connection
-- Copyright   : (c) Plow Technologies, 2023
-- Maintainer  : alberto.valverde@plowtech.net
--
-- This module provides a simpler and more convenient API over net-mqtt:
--
-- * It allows multiple listener callbacks to be added at any point of the lifetime of
-- 'Connection', not just one at creation time. These listeners will be
-- un-subscribed automatically when they're garbage-collected which allows for
-- simpler book-keeping by client code.
--
-- * Multiple listeners can subscribe to the same or overlapping topics and
-- messages will be routed to all of them as expected. This allows greater
-- composability in which components can setup their own subscriptions if they
-- can get a handle of the 'Connection' in 'IO' .
--
-- * The connection will automatically reconnect when it goes down.
module Network.MQTT.Connection
  ( Settings (..),
    defaultSettings,
    Connection,
    Channel,
    FilterSet,
    SubscribeCmd (..),
    ConnectionError (..),
    newConnection,
    closeConnection,
    closeConnectionWith,
    connectionSettings,
    withConnection,
    withAsyncSubscribe,
    withAsyncSubscribeWith,
    withAsyncSubscribeWith2,
    ConnectionTrace (..),
    showConnectionTrace,
    printConnectionTrace,
    isConnected,
    isConnectedSTM,
    subscribe,
    subscribeWith,
    subscribeWith2,
    subscribeChan,
    subscribeChanWith,
    subscribeChanWith2,
    readChannel,
    readChannelSTM,
    publish,
    onReconnect,
    getConnectionError,
    module ReExport,

    -- * Internal
    withClient,
  )
where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async
import Control.Concurrent.MVar
import qualified Control.Concurrent.STM.TBMChan as TBMChan
import qualified Control.Concurrent.STM.TChan as TChan
import Control.Concurrent.STM.TMVar
import Control.Concurrent.STM.TVar
import Control.DeepSeq (NFData (..), liftRnf, rwhnf, ($!!))
import Control.Exception
  ( BlockedIndefinitelyOnSTM (..),
    Exception,
    IOException,
    SomeException,
    bracket_,
    handle,
    throwIO,
    toException,
    uninterruptibleMask,
    uninterruptibleMask_,
  )
import Control.Monad
import Control.Monad.Catch (Handler (..), MonadMask, mask, onException)
import Control.Monad.Cont (ContT (ContT), evalContT)
import Control.Monad.IO.Class (MonadIO (..))
import Control.Monad.STM
import Control.Retry
import qualified Data.ByteString.Lazy.Char8 as BL
import Data.Default.Class (def)
import Data.Function (fix, on)
import qualified Data.HashSet as Set
import Data.Hashable (Hashable (..))
import qualified Data.List as L
import qualified Data.List.Extra as L
import qualified Data.List.NonEmpty as NE
import Data.List.Split (chunksOf)
import qualified Data.Map.Strict as Map
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Text.Encoding.Error as T
import qualified Data.Text.IO as T
import GHC.Generics
import GHC.Stack
import Network.Connection (TLSSettings (..))
import qualified Network.MQTT.Client as MQTT
import Network.MQTT.Connection.FilterTree (FilterTree)
import qualified Network.MQTT.Connection.FilterTree as FT
import Network.MQTT.Topic
import Network.MQTT.Topic as ReExport
  ( Filter,
    Topic,
    mkFilter,
    mkTopic,
    toFilter,
    unFilter,
    unTopic,
  )
import Network.MQTT.Types
import Network.MQTT.Types as ReExport
  ( DiscoReason (..),
    LastWill (..),
    Property (..),
    ProtocolLevel (..),
    PublishRequest (..),
    QoS (..),
    SubOptions (..),
    subOptions,
  )
import Network.URI as ReExport (URI, parseURI)
import System.IO (stderr)
import System.IO.Unsafe (unsafePerformIO)
import UnliftIO.Exception (catchAny, handleAny, tryAny)

-- | Settings to create a new 'Connection'
data Settings = Settings
  { brokerUri :: URI
  -- ^ The MQTT broker URI
  , cleanSession :: Bool
  -- ^ False if a session should be reused.
  , username :: Maybe String
  -- ^ Optional username (parsed from the URI)
  , password :: Maybe String
  -- ^ Optional password (parsed from the URI)
  , lwt :: IO (Maybe LastWill)
  -- ^ LastWill message to be sent by broker if connection is lost with us
  , protocol :: ProtocolLevel
  -- ^ Protocol to use for the connection.
  , connProps :: [Property]
  -- ^ Properties to send to the broker in the CONNECT packet.
  , connectTimeout :: Int
  -- ^ Connection timeout (microseconds)
  , tlsSettings :: TLSSettings
  -- ^ TLS Settings for secure connections
  , connectRetryPolicy :: RetryPolicyM IO
  -- ^ The retry policy when connecting or re-connecting to the broker
  , tracer :: ConnectionTrace -> IO ()
  -- ^ Callback called with traces of the library's operation
  , failFast :: Bool
  -- ^ Attempt to connect to broker on 'newConnection' synchronously and fail
  -- fast if the broker isn't up
  , pubSubTimeout :: Maybe Int
  -- ^ Optional timeout (microseconds) for 'publish' / 'subscribe' when waiting for
  -- connection to come back up
  , pingPeriod :: Int
  -- ^ Time between pings (microseconds)
  , pingPatience :: Int
  -- ^ Time in microseconds for which there must be no incoming packets before the broker is considered dead.
  -- Should be more than the ping period plus the maximum expected ping round trip time.
  , maxQueuedMessages :: Int
  -- ^ Length of the bounded queue for incoming messages per subscription.
  -- When the queue is full QoS0 messages will be dropped (this will be logged) and QoS1&2
  -- messages will block until processed. This will cause
  -- back-pressure and accumulate in the queue in the broker, what happens
  -- when queue fills up there is up to the broker (mosquitto drops them silently)
  }
  deriving (Generic)

-- | Sensible default 'Settings' for a given 'URI'
defaultSettings :: URI -> Settings
defaultSettings brokerUri =
  Settings
    { brokerUri
    , cleanSession = True
    , username = Nothing
    , password = Nothing
    , lwt = pure Nothing
    , protocol = Protocol50
    , connProps = mempty
    , connectTimeout = 10_000_000
    , tlsSettings = TLSSettingsSimple False False False def
    , connectRetryPolicy = constantDelay 30_000_000
    , tracer = printConnectionTrace
    , failFast = False
    , pubSubTimeout = Nothing
    , pingPeriod = 30_000_000
    , pingPatience = 90_000_000
    , maxQueuedMessages = 100_000
    }

data ConnectionError
  = ConnectionIsClosed CallStack URI
  | PubSubTimeout CallStack URI
  | QueueFull Topic
  | Closing Topic
  deriving (Exception)

instance Show ConnectionError where
  show (ConnectionIsClosed stack uri) =
    "Tried to use an already closed connection to " <> show uri <> ".\n" <> prettyCallStack stack
  show (PubSubTimeout stack uri) =
    "Timed out when waiting for connection to " <> show uri <> " to come back up.\n" <> prettyCallStack stack
  show (QueueFull topic) =
    "Internal queue for " <> show topic <> " is full so don't ack message"
  show (Closing topic) =
    "Closing connection so don't ack message for " <> show topic

data ReconnectThreadStopReason = StoppedBecauseConnectionClosed | StoppedBecauseConnectError SomeException
  deriving (Show, Generic)

data DropReason
  = DropNotConnected
  | DropClosed
  | DropError SomeException
  | DropQueueFull
  deriving (Show, Generic)

-- | A trace sent by the library when some events occur
data ConnectionTrace
  = -- | About to attempt to connect to the MQTT broker at given 'URI'
    Connecting URI
  | -- | Got a retryable exception when connecting
    ConnectRetryableError URI SomeException RetryStatus
  | -- | Just connected succesfully to the MQTT broken at given 'URI'
    Connected URI
  | -- | The keep-connected thread has detected that the connection to the MQTT broken at
    -- given 'URI' is down
    ConnectionDown URI
  | -- | About to attempt to subscribe to the given 'Filter' at the MQTT broker with the
    -- given 'URI'
    Subscribing URI [Filter]
  | -- | Just subscribed to the given 'Filter' at the MQTT broker with the given 'URI'
    Subscribed URI [Filter] ([Either SubErr QoS], [Property])
  | -- | Channel is being GCed
    ChannelGarbageCollected URI
  | -- | About to attempt to unsubscribe from the given 'Filter' at the MQTT broker with
    -- the given 'URI'
    Unsubscribing URI [Filter]
  | -- | Just unsubscribed from the given 'Filter' at the MQTT broker with the given 'URI'
    Unsubscribed URI [Filter] ([UnsubStatus], [Property])
  | -- | 'closeConnection' has been called on the 'Connection'
    ConnectionClosing URI (Maybe DiscoReason)
  | -- | 'closeConnection' has finished closing the connection
    ConnectionClosed URI (Maybe DiscoReason)
  | -- | The broker sent a malformed topic when delivering a message
    InvalidTopicFromBroker T.Text
  | -- | The broker sent a badly utf-8 encoded topic
    InvalidTopicEncodingFromBroker T.UnicodeException
  | -- | Received an unrecoverable exception when trying to connect
    ConnectionErrored SomeException
  | -- | Dropped a QoS0 outgoing message because the connection was down
    DroppedQoS0OutgoingMessage URI Topic DropReason
  | -- | Dropped an incoming message because the queue was full
    DroppedIncomingMessage URI Topic DropReason
  | -- | Reconnect thread has stopped
    ReconnectThreadStopped URI ReconnectThreadStopReason
  deriving (Show, Generic)

-- | Turns a 'ConnectionTrace' into a textual representation
showConnectionTrace :: ConnectionTrace -> T.Text
showConnectionTrace = \case
  Connecting uri -> "Connecting to MQTT broker at " <> tshow uri
  Connected uri -> "Connected to MQTT broker at " <> tshow uri
  ConnectRetryableError uri exc rs ->
    "Got " <> tshow (rsIterNumber rs + 1) <> " retryable error(s) when connecting to MQTT broker at " <> tshow uri <> ": " <> tshow exc
  ConnectionDown uri -> "Connection to MQTT broker at " <> tshow uri <> " has gone down"
  Subscribing uri f -> "Subscribing to " <> tshow (map unFilter f) <> " on MQTT broker at " <> tshow uri
  Subscribed uri f resp -> "Subscribed to " <> tshow (map unFilter f) <> " on MQTT broker at " <> tshow uri <> ": " <> tshow resp
  ChannelGarbageCollected uri -> "A channel on MQTT broker at " <> tshow uri <> " is being GCed"
  Unsubscribing uri f -> "Unsubscribing from " <> tshow (map unFilter f) <> " on MQTT broker at " <> tshow uri
  Unsubscribed uri f resp -> "Unsubscribed from " <> tshow (map unFilter f) <> " on MQTT broker at " <> tshow uri <> ": " <> tshow resp
  ConnectionClosing uri mReason -> "Closing connection to MQTT broker at " <> tshow uri <> ": " <> tshow mReason
  ConnectionClosed uri mReason -> "Closed connection to MQTT broker at " <> tshow uri <> ": " <> tshow mReason
  InvalidTopicFromBroker badTopic -> "Broker sent an invalid topic: " <> badTopic
  InvalidTopicEncodingFromBroker unicodeErr -> "Broker sent a badly encoded topic: " <> tshow unicodeErr
  ConnectionErrored err -> "Unrecoverable error when trying to connect to broker: " <> tshow err
  DroppedQoS0OutgoingMessage uri topic reason -> "Dropped a QoS0 outgoing message to " <> tshow (unTopic topic) <> " at " <> tshow uri <> " because " <> tshow reason
  DroppedIncomingMessage uri topic reason -> "Dropped an incoming message to " <> tshow (unTopic topic) <> " at " <> tshow uri <> " because " <> tshow reason
  ReconnectThreadStopped uri reason -> "Reconnect thread at " <> tshow uri <> "has stopped because " <> tshow reason
  where
    tshow :: (Show a) => a -> T.Text
    tshow = T.pack . show

printLock :: MVar ()
printLock = unsafePerformIO $ newMVar ()
{-# NOINLINE printLock #-}

-- | Displays a 'ConnectionTrace' on stderr.
printConnectionTrace :: ConnectionTrace -> IO ()
printConnectionTrace = bracket_ (takeMVar printLock) (putMVar printLock ()) . T.hPutStrLn stderr . showConnectionTrace

-- | A 'Connection' to a MQTT broker.
-- A 'Filter' can be subscribed to multiple times and messages will delivered to
-- all listeners.
-- When there are no longer any listeners to a given 'Filter' then it will be automatically unsubscribed from.
-- This 'Connection' will detect when the underlying network connection has gone
-- down and will attempt to re-connect. Note that when the connection is down,
-- 'subscribe' and 'publish' will block until it is up again.
data Connection = Connection
  { connClientRef :: TMVar Client
  -- ^ The currently MQTTClient. It is a TMVar because we want to ensure that
  -- only the keepConnected thread is in charge of performing the actual
  -- (re)connections to avoid the thundering-herd problem (in which several
  -- threads would be trying to establish a connection simultaneously).
  -- Theese semantics should not be mixed with those of a mutex (else
  -- isConnectedSTM will cease to work properly)
  , connRoutingTree :: TVar RoutingTree
  , connSettings :: Settings
  -- ^ The 'Settings' used to configure this connection
  , connKeepConnectedThread :: Async ()
  -- ^ A reference to the async thread the performs the re-connections. This
  -- thread must be the only producer of connClientRef
  , connOnConnectListeners :: TVar (IO ())
  }

-- | The 'Settings' used to create a 'Connection'
connectionSettings :: Connection -> Settings
connectionSettings = connSettings
{-# INLINE connectionSettings #-}

data Client
  = New
  | Closed
  | Open {-# UNPACK #-} !MQTT.MQTTClient
  | Errored SomeException

type FilterSet = Map.Map Filter (SubOptions, [Property])

type RoutingTree = FilterTree RoutingEntry

data RoutingEntry = RoutingEntry
  { reSubs :: !(Set.HashSet Subscription)
  , reSubOptions :: !SubOptions
  , reSubProps :: ![Property]
  }
  deriving (Generic)

instance NFData RoutingEntry where
  rnf (RoutingEntry a b c) =
    rnf a `seq` rnfSubOptions b `seq` liftRnf rnfProp c `seq` ()
    where
      rnfSubOptions (SubOptions w x y z) = rwhnf w `seq` rnf x `seq` rnf y `seq` rwhnf z `seq` ()
      rnfProp (PropPayloadFormatIndicator x) = rnf x
      rnfProp (PropMessageExpiryInterval x) = rnf x
      rnfProp (PropContentType x) = rnf x
      rnfProp (PropResponseTopic x) = rnf x
      rnfProp (PropCorrelationData x) = rnf x
      rnfProp (PropSubscriptionIdentifier x) = rnf x
      rnfProp (PropSessionExpiryInterval x) = rnf x
      rnfProp (PropAssignedClientIdentifier x) = rnf x
      rnfProp (PropServerKeepAlive x) = rnf x
      rnfProp (PropAuthenticationMethod x) = rnf x
      rnfProp (PropAuthenticationData x) = rnf x
      rnfProp (PropRequestProblemInformation x) = rnf x
      rnfProp (PropWillDelayInterval x) = rnf x
      rnfProp (PropRequestResponseInformation x) = rnf x
      rnfProp (PropResponseInformation x) = rnf x
      rnfProp (PropServerReference x) = rnf x
      rnfProp (PropReasonString x) = rnf x
      rnfProp (PropReceiveMaximum x) = rnf x
      rnfProp (PropTopicAliasMaximum x) = rnf x
      rnfProp (PropTopicAlias x) = rnf x
      rnfProp (PropMaximumQoS x) = rnf x
      rnfProp (PropRetainAvailable x) = rnf x
      rnfProp (PropUserProperty x y) = rnf x `seq` rnf y `seq` ()
      rnfProp (PropMaximumPacketSize x) = rnf x
      rnfProp (PropWildcardSubscriptionAvailable x) = rnf x
      rnfProp (PropSubscriptionIdentifierAvailable x) = rnf x
      rnfProp (PropSharedSubscriptionAvailable x) = rnf x

data SubscribeCmd
  = Subscribe Filter SubOptions [Property] (IO ())
  | Unsubscribe Filter [Property] (IO ())
  | ResetSubscriptions FilterSet (IO ())

data Subscription = Subscription
  { subChan :: {-# UNPACK #-} !(TBMChan.TBMChan PublishRequest)
  , subUpdater :: {-# UNPACK #-} !(Async ())
  }
  deriving (Generic)

instance NFData Subscription where
  rnf !_ = () -- everything's UNPACKd and strict so this should be enough

instance Eq Subscription where
  (==) = (==) `on` subUpdater

instance Hashable Subscription where
  hashWithSalt s Subscription{subUpdater} = hashWithSalt s subUpdater

-- | A Channel where incoming messages can be read from.
newtype Channel = Channel
  { --  A TVar is used so we can reliably attach a finalizer to
    --  automatically unsubscribe when there are no more listeners to a topic
    unChannel :: TVar (TBMChan.TBMChan PublishRequest)
  }

-- | Block until the next 'PublishRequest' from the 'Channel' is received.
-- If 'Nothing' is returned it means that the 'Channel' has been closed and will
-- no longer return any 'Just' value.
readChannel :: Channel -> IO (Maybe PublishRequest)
readChannel = atomically . readChannelSTM

-- | Same as 'readChannel' but for use inside an 'STM' transaction
readChannelSTM :: Channel -> STM (Maybe PublishRequest)
readChannelSTM = readTVar . unChannel >=> TBMChan.readTBMChan

-- | Subscribe to a topic/filter and loop forever processing incoming messages with the given function.
-- This function blocks until the 'Connection' is closed.
-- For a non-blocking version use 'withAsyncSubscribe' for a version that spawns an
-- 'Async' thread.
-- If the given callback throws an exception no more messages will be processed
subscribe :: (HasCallStack) => Connection -> Filter -> (PublishRequest -> IO ()) -> IO ()
subscribe conn topic f = do
  chan <- subscribeChanWith conn (Map.singleton topic (MQTT.subOptions, [])) Nothing
  subscribeLoop 1 chan f

-- | Same as 'subscribe' but can override defaults
subscribeWith :: (HasCallStack) => Int -> IO () -> Connection -> FilterSet -> Maybe (TChan.TChan FilterSet) -> (PublishRequest -> IO ()) -> IO ()
subscribeWith numThreads onSubscribed conn topics topicsChan f = do
  chan <- subscribeChanWith conn topics topicsChan
  onSubscribed
  subscribeLoop numThreads chan f

-- | Same as 'subscribe' but can override defaults
subscribeWith2 :: (HasCallStack) => Int -> IO () -> Connection -> FilterSet -> Maybe (TChan.TChan SubscribeCmd) -> (PublishRequest -> IO ()) -> IO ()
subscribeWith2 numThreads onInitialSubscribe conn topics cmdsChan f = do
  chan <- subscribeChanWith2 conn topics cmdsChan
  onInitialSubscribe
  subscribeLoop numThreads chan f

subscribeLoop :: Int -> Channel -> (PublishRequest -> IO a) -> IO ()
subscribeLoop numThreads chan f = case numThreads of
  n | n > 0 -> evalContT $ do
    threads <- replicateM (numThreads - 1) (ContT (withAsync loop))
    liftIO $ do
      mapM_ link threads
      loop
  n -> throwIO $ userError $ "Invalid number of threads: " <> show n
  where
    loop = readChannel chan >>= maybe (pure ()) (\pr -> f pr >> loop)

-- | Subscribe to a topic/filter and return a 'Channel'. Use 'readChannel' or 'readChannelSTM' to
-- poll messages from it.
--
-- This can be used to implement a conduit source, for example:
--
-- @
-- chanSource
--    :: MonadIO m
--    => Channel                  -- ^ The channel.
--    -> ConduitT z a m ()
-- chanSource ch reader = loop
--   where
--     loop = liftIO (readChannel ch) >>= maybe (pure ()) (\x -> yield x >> loop)
-- @
subscribeChan :: (HasCallStack) => Connection -> Filter -> IO Channel
subscribeChan conn topic = subscribeChanWith conn (Map.singleton topic (MQTT.subOptions, [])) Nothing

-- | Same as 'subscribeChan' but can override defaults
subscribeChanWith :: (HasCallStack) => Connection -> FilterSet -> Maybe (TChan.TChan FilterSet) -> IO Channel
subscribeChanWith conn topics Nothing = subscribeChanWith2 conn topics Nothing
subscribeChanWith conn topics (Just chan) = do
  chan2 <- TChan.newTChanIO
  t <- async $ translateChan chan2
  ret@(Channel chanRef) <- subscribeChanWith2 conn topics (Just chan2)
  void $ mkWeakTVar chanRef (cancel t)
  pure ret
  where
    translateChan outChan =
      forever $
        atomically $
          TChan.readTChan chan
            >>= TChan.writeTChan outChan . flip ResetSubscriptions (pure ())

-- | Same as 'subscribeChan' but can override defaults
subscribeChanWith2 :: (HasCallStack) => Connection -> FilterSet -> Maybe (TChan.TChan SubscribeCmd) -> IO Channel
subscribeChanWith2 conn@Connection{connSettings} topics mTopicsChan =
  uninterruptibleMask $ \restore -> mdo
    onReady <- newEmptyTMVarIO
    subUpdater <- restore $ async $ case mTopicsChan of
      Just topicsChan -> updateSubscriptionsThread topicsChan sub' onReady
      Nothing -> forever (threadDelay 300_000_000)
    (sub', chanRef', sendReqs') <- restore $ atomically $ do
      subChan <- TBMChan.newTBMChan (maxQueuedMessages connSettings)
      let sub = Subscription{subChan, subUpdater}
      chanRef <- newTVar subChan
      sendReqs <- updateRoutingTree conn sub $ ResetSubscriptions topics mempty
      putTMVar onReady ()
      pure (sub, chanRef, sendReqs)
    -- This finalizer is called reliably because it is attached to a TVar (See
    -- System.Mem.Weak.Weak docs). Channel is a newtype over a TVar just for
    -- this reason.
    void $ mkWeakTVar chanRef' (onChannelGarbageCollected sub')
    restore sendReqs'
    pure (Channel chanRef')
  where
    updateSubscriptionsThread :: TChan.TChan SubscribeCmd -> Subscription -> TMVar () -> IO ()
    updateSubscriptionsThread cmdsChan sub onReady = do
      atomically (takeTMVar onReady)
      forever $ do
        sendRequests <-
          atomically $
            updateRoutingTree conn sub =<< TChan.readTChan cmdsChan
        sendRequests `catchAny` const (pure ())

    -- It is important that a finalizer never throws so catchAny
    onChannelGarbageCollected sub@Subscription{subUpdater} = handleAny (const (pure ())) $ do
      tracer connSettings $ ChannelGarbageCollected (brokerUri connSettings)
      cancel subUpdater
      sendRequests <- atomically $ updateRoutingTree conn sub $ ResetSubscriptions mempty mempty
      sendRequests `catchAny` const (pure ())

updateRoutingTree :: (HasCallStack) => Connection -> Subscription -> SubscribeCmd -> STM (IO ())
updateRoutingTree conn@Connection{connRoutingTree} sub (ResetSubscriptions newFilters cb) = do
  routingTree <- readTVar connRoutingTree
  let fsBefore = filterTreeToSet routingTree
      fsAfter = filterTreeToSet routingTree'
      routingTreeWithoutSub =
        FT.mapMaybe removeSubAndPrune routingTree
      removeSubAndPrune re
        | Set.null subs' = Nothing
        | otherwise = Just re{reSubs = subs'}
        where
          subs' = Set.delete sub (reSubs re)
      routingTree' =
        L.foldl'
          ( \m (t, (os, ps)) ->
              m `seq` insertIntoRoutingTree t sub os ps m
          )
          routingTreeWithoutSub
          (Map.toList newFilters)
  writeTVar connRoutingTree $!! routingTree'
  pure $ do
    sendUnsubscribeRequests conn $ Map.toList (fsBefore `Map.difference` fsAfter)
    sendSubscribeRequests conn $ Map.toList (fsAfter `Map.difference` fsBefore)
    cb
updateRoutingTree conn@Connection{connRoutingTree} sub (Subscribe t o ps cb) = do
  needsSubscribe <- not . FT.member t <$> readTVar connRoutingTree
  modifyTVar' connRoutingTree (insertIntoRoutingTree t sub o ps)
  pure $ do
    when needsSubscribe $ sendSubscribeRequests conn [(t, (o, ps))]
    cb
updateRoutingTree conn@Connection{connRoutingTree} sub (Unsubscribe t ps cb) = do
  modifyTVar' connRoutingTree (FT.update upd t)
  needsUnsubscribe <- not . FT.member t <$> readTVar connRoutingTree
  pure $ do
    when needsUnsubscribe $ sendUnsubscribeRequests conn [(t, (MQTT.subOptions {-dummy-}, ps))]
    cb
  where
    upd re
      | Set.null subs' = Nothing
      | otherwise = Just (re{reSubs = subs'})
      where
        subs' = Set.delete sub (reSubs re)

insertIntoRoutingTree ::
  Filter ->
  Subscription ->
  SubOptions ->
  [Property] ->
  FilterTree RoutingEntry ->
  FilterTree RoutingEntry
insertIntoRoutingTree t sub subOpts subProps =
  FT.insertWith
    combine
    t
    RoutingEntry
      { reSubs = Set.singleton sub
      , reSubOptions = subOpts
      , reSubProps = subProps
      }
  where
    combine (RoutingEntry subs os ps) (RoutingEntry subs' os' ps') =
      RoutingEntry
        { reSubs = subs `Set.union` subs'
        , reSubOptions = os `combineSubOpts` os'
        , reSubProps = L.nubOrd (ps <> ps')
        }
    combineSubOpts _ a = a -- FIXME

subChunkSize :: Int
subChunkSize = 8 -- Max size supported by AWS IoT

sendSubscribeRequests :: (HasCallStack) => Connection -> [(Filter, (SubOptions, [Property]))] -> IO ()
sendSubscribeRequests conn@Connection{connSettings} allReqs = do
  withClient conn $ \c ->
    sendSubscribeRequests2 connSettings c allReqs

sendSubscribeRequests2 :: Settings -> MQTT.MQTTClient -> [(Filter, (SubOptions, [Property]))] -> IO ()
sendSubscribeRequests2 connSettings client allReqs = do
  let groupedReqs = NE.groupAllWith (\(_, (_, ps)) -> show ps) allReqs
  forM_ groupedReqs $ \reqs@((_, (_, subProps)) NE.:| _) ->
    forM_ (chunksOf subChunkSize (NE.toList reqs)) $ \chunk -> do
      let topOpts = map (\(t, (opts, _)) -> (t, opts)) chunk
      tracer connSettings $ Subscribing (brokerUri connSettings) (map fst topOpts)
      res <- MQTT.subscribe client topOpts subProps
      tracer connSettings $ Subscribed (brokerUri connSettings) (map fst topOpts) res

sendUnsubscribeRequests :: (HasCallStack) => Connection -> [(Filter, (SubOptions, [Property]))] -> IO ()
sendUnsubscribeRequests conn@Connection {connSettings} allReqs = do
  let groupedReqs = NE.groupAllWith (\(_, (_, ps)) -> show ps) allReqs
  forM_ groupedReqs $ \reqs@((_, (_, subProps)) NE.:| _) ->
    forM_ (chunksOf subChunkSize (NE.toList reqs)) $ \chunk -> do
      let tops = map fst chunk
      tracer connSettings $ Unsubscribing (brokerUri connSettings) tops
      res <- withClient conn (\c -> MQTT.unsubscribe c tops subProps)
      tracer connSettings $ Unsubscribed (brokerUri connSettings) tops res

filterTreeToSet :: RoutingTree -> FilterSet
filterTreeToSet = filterListToSet . FT.toList

filterListToSet ::
  [(Filter, RoutingEntry)] ->
  Map.Map Filter (SubOptions, [Property])
filterListToSet = Map.fromList . map (\(a, re) -> (a, (reSubOptions re, reSubProps re)))

-- | Same as 'withAsyncSubscribe' but can override defaults
withAsyncSubscribeWith ::
  (HasCallStack) =>
  Connection ->
  Int ->
  FilterSet ->
  Maybe (TChan.TChan FilterSet) ->
  (PublishRequest -> IO ()) ->
  IO a ->
  IO a
withAsyncSubscribeWith conn numThreads topics topicsChan sub f = do
  v <- newEmptyTMVarIO
  withAsync (subscribeWith numThreads (atomically (putTMVar v ())) conn topics topicsChan sub) $ \t -> do
    link t
    when (failFast (connSettings conn)) $
      atomically (takeTMVar v)
    f

-- | Same as 'withAsyncSubscribe' but can override defaults
withAsyncSubscribeWith2 ::
  (HasCallStack) =>
  Connection ->
  Int ->
  FilterSet ->
  Maybe (TChan.TChan SubscribeCmd) ->
  (PublishRequest -> IO ()) ->
  IO a ->
  IO a
withAsyncSubscribeWith2 conn numThreads topics cmdsChan sub f = do
  v <- newEmptyTMVarIO
  withAsync (subscribeWith2 numThreads (atomically (putTMVar v ())) conn topics cmdsChan sub) $ \t -> do
    link t
    when (failFast (connSettings conn)) $
      atomically (takeTMVar v)
    f

-- | 'subscribe' to a topic in an async thread so that the calling thread is
-- not blocked.
--
-- If 'failFast' is True, the continuation will not be called until the broker has acknowledged the suscription, blocking until this happens
withAsyncSubscribe :: Connection -> Filter -> (PublishRequest -> IO ()) -> IO b -> IO b
withAsyncSubscribe conn topic =
  withAsyncSubscribeWith conn 1 (Map.singleton topic (MQTT.subOptions, [])) Nothing

-- | Execute an action when a dropped connection is re-established. The action
-- is only called on the rising edge when the state transitions from "not
-- connected" to "connected", that is, if the connection is initially up, the
-- action won't be executed immediately.
-- This function does not block
onReconnect :: Connection -> IO () -> IO ()
onReconnect Connection{connOnConnectListeners} f =
  atomically $ modifyTVar connOnConnectListeners (<> f)

-- | Create a new 'Connection' with the given 'Settings'.
newConnection :: Settings -> IO Connection
newConnection connSettings = do
  connRoutingTree <- newTVarIO FT.empty
  connOnConnectListeners <- newTVarIO mempty
  connClientRef <-
    if failFast connSettings
      then newTMVarIO . Open =<< connect connRoutingTree
      else newTMVarIO New
  connKeepConnectedThread <- linkedAsync (keepConnected connOnConnectListeners connRoutingTree connClientRef)
  pure
    Connection
      { connSettings
      , connClientRef
      , connRoutingTree
      , connKeepConnectedThread
      , connOnConnectListeners
      }
  where
    uri = brokerUri connSettings
    connect :: TVar RoutingTree -> IO MQTT.MQTTClient
    connect connRoutingTree = do
      tracer connSettings (Connecting uri)
      will <- lwt connSettings
      MQTT.connectURI
        MQTT.mqttConfig
          { MQTT._cleanSession = cleanSession connSettings
          , MQTT._username = username connSettings
          , MQTT._password = password connSettings
          , MQTT._lwt = will
          , -- Use LowLevelCallback instead of the ordered version because the
            -- orderded version serializes calling callbacks and we
            -- don't want a full channel blocking the rest of the channels
            MQTT._msgCB = MQTT.LowLevelCallback (messageCallback connRoutingTree)
          , MQTT._protocol = protocol connSettings
          , MQTT._connProps = connProps connSettings
          , MQTT._connectTimeout = connectTimeout connSettings
          , MQTT._tlsSettings = tlsSettings connSettings
          , MQTT._pingPeriod = pingPeriod connSettings
          , MQTT._pingPatience = pingPatience connSettings
          }
        uri

    -- This linked async thread loops forever waiting until the TMVar is empty and
    -- fills it in. It must be the the only producer of the TMVar, else deadlock.
    keepConnected :: TVar (IO ()) -> TVar RoutingTree -> TMVar Client -> IO ()
    keepConnected connOnConnectListeners connRoutingTree clientRef = fix $ \loop -> do
      atomically
        ( takeTMVar clientRef >>= \case
            Open client -> do
              -- The following 'check' blocks (retrying inside STM) until the connection goes down
              check . not =<< MQTT.isConnectedSTM client
              pure (Right True)
            Closed -> putTMVar clientRef Closed >> pure (Left StoppedBecauseConnectionClosed)
            e@(Errored exc) -> putTMVar clientRef e >> pure (Left (StoppedBecauseConnectError exc))
            New -> pure (Right False)
        )
        >>= \case
          Right wasOpen -> do
            when wasOpen $
              tracer connSettings (ConnectionDown uri)
            -- At this point the connection is down, reconnect
            tryAny (recovering (connectRetryPolicy connSettings) retryExceptionHandlers (const (connect connRoutingTree)))
              >>= \case
                Left err -> do
                  tracer connSettings (ConnectionErrored err)
                  atomically $ putTMVar clientRef $ Errored err
                Right client -> do
                  tracer connSettings (Connected uri)
                  subs <- Map.toList . filterTreeToSet <$> readTVarIO connRoutingTree
                  when wasOpen $
                    sendSubscribeRequests2 connSettings client subs
                  atomically $ putTMVar clientRef $ Open client
                  -- call onReconnect listeners. We do this
                  -- here AFTER putting the new client on the TMVar
                  when wasOpen $
                    void $
                      async (join (readTVarIO connOnConnectListeners))
            loop
          Left stopReason ->
            tracer connSettings (ReconnectThreadStopped uri stopReason)

    messageCallback :: TVar RoutingTree -> MQTT.MQTTClient -> PublishRequest -> IO ()
    messageCallback connRoutingTree _ req =
      case T.decodeUtf8' (BL.toStrict (_pubTopic req)) of
        Right topicTxt ->
          case mkTopic topicTxt of
            Just topic ->
              logDropped $ atomically $ do
                routingTree <- readTVar connRoutingTree
                forM_ (matchingSubs topic routingTree) $ \sub ->
                  TBMChan.tryWriteTBMChan (subChan sub) req >>= \case
                    Just True -> pure ()
                    -- Throw exception so message is not acknowledged. This should cause the
                    -- broker to attempt to deliver it to us later,
                    -- hopefully when less busy.
                    -- We throw inside STM instead of IO so STM
                    -- transaction is aborted, otherwise some
                    -- subscriptions may process QoS2 messages more than
                    -- once
                    Just False -> throwSTM (QueueFull topic)
                    Nothing -> throwSTM (Closing topic)
            Nothing -> tracer connSettings (InvalidTopicFromBroker topicTxt)
        Left unicodeEx -> tracer connSettings (InvalidTopicEncodingFromBroker unicodeEx)

    logDropped = handle $ \e -> do
      case e of
        QueueFull topic -> tracer connSettings (DroppedIncomingMessage uri topic DropQueueFull)
        Closing topic -> tracer connSettings (DroppedIncomingMessage uri topic DropClosed)
        _ -> pure ()
      throwIO e

    retryExceptionHandlers =
      skipAsyncExceptions
        ++ [ -- Retry any IOException
             logAndRetry (const @_ @IOException True)
           , -- Don't retry AsyncCancelled
             logAndRetry $ \AsyncCancelled{} -> False
           , -- Retry any MQTTException except an invalid URI
             logAndRetry $ \case
              MQTT.MQTTException err -> not $ "invalid URI scheme: " `L.isInfixOf` err
              _ -> True
           ]

    logAndRetry :: (Exception e) => (e -> Bool) -> RetryStatus -> Handler IO Bool
    logAndRetry f rs = Handler $ \e ->
      if f e
        then do
          tracer connSettings (ConnectRetryableError uri (toException e) rs)
          pure True
        else pure False

    matchingSubs :: Topic -> RoutingTree -> Set.HashSet Subscription
    matchingSubs topic = mconcat . map reSubs . FT.match topic

-- | Close a 'Connection'
closeConnection :: Connection -> IO ()
closeConnection = closeConnectionWith (Just DiscoNormalDisconnection)

-- | Close a 'Connection' with an optional disconnection reason. If no reason is
-- given no DISCONNECT message will be sent to the broker. This should cause the
-- connection to close abruptly and cause the broker to send any will message
-- that has been defined when connecting.
closeConnectionWith :: Maybe DiscoReason -> Connection -> IO ()
closeConnectionWith
  mReason
  Connection
    { connRoutingTree
    , connKeepConnectedThread
    , connSettings
    , connClientRef
    , connOnConnectListeners
    } = do
    tracer connSettings (ConnectionClosing (brokerUri connSettings) mReason)
    cancel connKeepConnectedThread
    join $ atomically $ do
      writeTVar connOnConnectListeners mempty
      tryTakeTMVar connClientRef >>= \case
        Just (Open client) -> do
          routingTree <- swapTVar connRoutingTree FT.empty
          -- Close channel indicating there will be no more writes.
          -- this allows listeners to explicitly handle this case
          -- instead of catching the BlockedIndefinitelyOnSTM exception that will
          -- eventually be thrown when they block reading on a channel where the
          -- producer at the write end has gone
          let allSubs = mconcat $ map reSubs $ FT.elems routingTree
          forM_ allSubs $ \sub ->
            TBMChan.closeTBMChan (subChan sub)
          putTMVar connClientRef Closed
          MQTT.isConnectedSTM client >>= \case
            True -> pure $ do
              forM_ mReason $ \reason ->
                handleAny (const (pure ())) $ do
                  MQTT.disconnect client reason []
                  MQTT.waitForClient client
              MQTT.stopClient client
            False -> pure (pure ())
        _ -> putTMVar connClientRef Closed >> pure (pure ())
    tracer connSettings (ConnectionClosed (brokerUri connSettings) mReason)

-- | Brackets 'newConnection' and 'closeConnection'
withConnection :: (MonadMask m, MonadIO m) => Settings -> (Connection -> m a) -> m a
withConnection x f = mask $ \restore ->
  liftIO (newConnection x) >>= \conn ->
    restore (f conn) `onException` liftIO (closeConnectionWith Nothing conn)
      <* liftIO (closeConnection conn)

-- | Is the connection to the MQTT broker up?
isConnected :: Connection -> IO Bool
isConnected = atomically . isConnectedSTM

-- | Is the connection to the MQTT broker up? (STM version)
isConnectedSTM :: Connection -> STM Bool
isConnectedSTM Connection{connClientRef} = do
  tryReadTMVar connClientRef >>= \case
    Just (Open client) -> MQTT.isConnectedSTM client
    _ -> pure False

-- | Publish a message using the given 'Connection'. This function will block if
-- the underlying MQTT connection is down until it is re-established. Use
-- 'isConnected' or 'isConnectedSTM' to detect if the connection is up
publish ::
  (HasCallStack) =>
  -- | Connection
  Connection ->
  -- | Topic
  Topic ->
  -- | Message body
  BL.ByteString ->
  -- | Retain flag
  Bool ->
  -- | QoS
  QoS ->
  -- | Properties
  [Property] ->
  IO ()
publish Connection{connClientRef, connSettings} a b c d@QoS0 e =
  join $
    atomically $
      tryReadTMVar connClientRef >>= \case
        Just (Open client) ->
          MQTT.isConnectedSTM client >>= \case
            True -> pure $ MQTT.publishq client a b c d e
            False -> dropMessage DropNotConnected
        Just Closed -> dropMessage DropClosed
        Just (Errored err) -> dropMessage (DropError err)
        Just New -> dropMessage DropNotConnected
        Nothing -> dropMessage DropNotConnected
  where
    dropMessage dropReason =
      pure $
        tracer connSettings $
          DroppedQoS0OutgoingMessage (brokerUri connSettings) a dropReason
publish conn a b c d e =
  withClient conn $ \client -> MQTT.publishq client a b c d e

-- | Returns the active 'MQTTClient'. This function will block (up to optional
-- 'pubSubTimeout') if there's no active connected 'MQTTClient' and the
-- 'Connection' is still attempting re-connects, if it isn't it will throw a
-- 'ConnectionIsClosed' exception
-- If 'pubSubTimeout' is not 'Nothing' an operation times out, a 'PubSubTimeout'
-- error will be thrown
withClient :: (HasCallStack) => Connection -> (MQTT.MQTTClient -> IO a) -> IO a
withClient Connection{connClientRef, connSettings} f = do
  timedOutRef <- maybe (newTVarIO False) registerDelay (pubSubTimeout connSettings)
  join $ handle blockedIndefToClosedConnErr $ atomically $ do
    timedOut <- readTVar timedOutRef
    when timedOut $
      throwSTM pubSubTimeoutError
    readTMVar connClientRef >>= \case
      Open client -> do
        check =<< MQTT.isConnectedSTM client
        pure (f client)
      Closed -> throwSTM connIsClosedError
      Errored e -> throwSTM e
      New -> retry
  where
    blockedIndefToClosedConnErr BlockedIndefinitelyOnSTM = throwIO connIsClosedError
    pubSubTimeoutError = PubSubTimeout callStack (brokerUri connSettings)
    connIsClosedError = ConnectionIsClosed callStack (brokerUri connSettings)

-- | Return any unrecoverable connection error that may have been produced in the
-- async thread that handles re-connections.
-- This function will block (perhaps forever!) until an exception is actually
-- produced or the connection is gracefully closed by `closeConnection` or
-- `closeConnectionWith` so it should be called in an async thread to handle it
-- or in the main thread to end the program, for example
-- @
--     withConnection .. $ \conn ->
--       withAsync (doStuffWithConn conn) $ \thread -> do
--          link thread -- so we die if doStuffWithConn throws
--          maybe (pure ()) throwIO =<< getConnection conn -- wait until conn is closed or produces an unrecoverable error
-- @
getConnectionError :: Connection -> IO (Maybe SomeException)
getConnectionError Connection{connClientRef} =
  atomically $
    readTMVar connClientRef >>= \case
      Open _ -> retry
      Closed -> pure Nothing
      Errored e -> pure $ Just e
      New -> retry

linkedAsync :: IO a -> IO (Async a)
linkedAsync io = uninterruptibleMask_ $ do
  t <- asyncWithUnmask ($ io)
  link t
  pure t
