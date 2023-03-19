# Revision history for net-mqtt-extra

## 0.3.5.1 -- 2023-11-30
* Retry any IOException when reconnecting as timeouts come from the socket
  sometimes, not only System.Timeout.timeout

## 0.3.5.0 -- 2023-11-30
* Improve reconnecting logic logging and how resubscriners are sent

## 0.3.4.0 -- 2023-11-27
* Use a new structure for routing so dispatching is logarithmic complexity on
  the total number of filters instead of linear
* Fix reconnecting logic so it doesn't abort reconnection attempts on most
  MQTTExceptions

## 0.3.3.1 -- 2023-11-17
* Fixed bug introduced in 0.3.3.0 in subscribeWith

## 0.3.3.0 -- 2023-11-16
* Added versions of subscribeWith which receive subscription commands
  thru the updates channel instead of resulting filter sets

## 0.3.2.3 -- 2023-10-23
* Run onReconnect callbacks in an async thread. Otherwise the reconnecting
  thread will block (or deadlock) if callback blocks

## 0.3.2.2 -- 2023-09-19
* Fixed race-condition which could cause onReconnect callbacks to access old
  client on the TMVar instead of the new one

## 0.3.2.1 -- 2023-08-18
* Raised default 'maxQueuedMessages' to 100k so client code that worked fine
  before 0.3.1.0 is more likely to work fine now.

## 0.3.2.0 -- 2023-07-21
* Expose 'subscribeChan' and friends so a conduit source can be implemented

## 0.3.1.0 -- 2023-07-21
* Add a `maxQueuedMessages` configuration option to control the size of the
  bounded queues that broadcast messages to subscribers.
  When the queue is full, messages will be dropped. QoS1 and QoS2 messages
  will not be acknowledged so the broker should attempt delivery again,
  hopefully when the queue has room.

## 0.3.0.0 -- 2023-07-10
* Calling `onReconnect` no longer blocks so no need to call it asynchronously.
* onReconnect's callback is now guaranteed to be called after SUBSCRIBE messages
  have been sent to the broker.
* lwt is now an IO action that returns a `Maybe LastWill` instead of a `Maybe
  LastWill`. This allows implementing Sparkplug's protocol incrementing
  sequences on the last will message.

## 0.2.2.0 -- 2023-06-30
* Expose closeConnectionWith to send an optional disconnection reason.
* withConnection will no longer send a DISCONNECT message if an expection was
  thrown, otherwise broker will discard last-will message

## 0.2.0.1 -- 2023-05-29
* Use try from unliftio in keepAliveThread so AsyncExceptions aren't caught

## 0.2.0.0 -- 2023-05-3
* Breaking change to efficiently handle many subscriptions and to update these
  without unsubscribing and re-subscribing

## 0.1.0.1 -- 2023-03-22
* Handle the case of an unrecoverable exception in keepConnected thread
* Expose unTopic and unFilter

## 0.1.0.0 -- 2023-03-08
* First version. Released on an unsuspecting world.
