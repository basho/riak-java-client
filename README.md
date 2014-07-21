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

For more complex configurations, you can instantiate a RiakCluster from the 
core packages and supply it to the RiakClient constructor.

Once you have a client, commands from the `com.basho.riak.client.api.commands.*` 
packages are built then executed by the client:

```java
Namespace ns = new Namespace("default","my_bucket");
Location loc = new Location(ns, "my_key");
FetchValue fv = new FetchValue.Builder(loc).build();
FetchValue.Response response = client.execute(fv);
RiakObject obj = response.getValue(RiakObject.class);
```


