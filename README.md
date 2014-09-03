## Riak Java client v2.0 release candidate

This branch of the Riak Java Client is for the new v2.0 client, to be used with the
upcoming Riak 2.0 (also currently in RC). 

As a release candidate, minor changes may occur before final release. That said, 
these changes should be limited in scope and not fundamentally change the API. 

The current weak point is documentation (Javadoc) which is now the main focus
for release.

## Overview

Version 2.0 of the Riak Java client is a completely new codebase. It relies on 
Netty4 in the core for handling network operations and all operations can
be executed synchronously or asynchronously. 

## Getting started with the 2.0 client.

The new client is designed to model a Riak cluster:

![RJC model](http://brianroach.info/blog/wp-content/uploads/2013/10/RJC2.png)

The easiest way to get started with the client is using one of the static 
methods provided to instantiate and start the client:

```java
List<String> addresses = new LinkedList<String>();
addresses.add("192.168.1.1");
addresses.add("192.168.1.2");
addresses.add("192.168.1.3");
RiakClient client = RiakClient.newClient(addresses);
```

The client object is thread safe and may be shared across multiple threads.

For more complex configurations, you can instantiate a RiakCluster from the 
core packages and supply it to the RiakClient constructor.

The client executes commands found in the `com.basho.riak.client.api.commands`
package.  Some basic examples of building and executing these commands is shown
below.

## Getting Data In

```java
Namespace ns = new Namespace("default", "my_bucket");
Location location = new Location(ns, "my_key");
RiakObject riakObject = new RiakObject();
riakObject.setValue(BinaryValue.create("my_value"));
StoreValue store = new StoreValue.Builder(riakObject)
  .withLocation(location)
  .withOption(Option.W, new Quorum(3)).build();
client.execute(store);
```

## Getting Data Out

```java
Namespace ns = new Namespace("default","my_bucket");
Location location = new Location(ns, "my_key");
FetchValue fv = new FetchValue.Builder(location).build();
FetchValue.Response response = client.execute(fv);
RiakObject obj = response.getValue(RiakObject.class);
```

## Using 2.0 Data Types (Maps & Registers)

A [bucket type](http://docs.basho.com/riak/latest/dev/advanced/bucket-types) must be created (in all local and remote clusters) before 2.0
data types can be used.  In the example below, it is assumed that the type
"my_map_type" has been created and associated to the "my_map_bucket" prior
to this code executing.

Once a bucket has been associated with a type, all values stored in that bucket
must belong to that data type.

```java
Namespace ns = new Namespace("my_map_type", "my_map_bucket");
Location location = new Location(ns, "my_key");
RegisterUpdate ru1 = new RegisterUpdate(BinaryValue.create("map_value_1"));
RegisterUpdate ru2 = new RegisterUpdate(BinaryValue.create("map_value_2"));
MapUpdate mu = new MapUpdate();
mu.update("map_key_1", ru1);
mu.update("map_key_1", ru2);
UpdateMap update = new UpdateMap.Builder(location, mu).build();
client.execute(update);
```

## Search 2.0

```java
# TBD
```
