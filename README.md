## Riak Java client v2.0

This branch of the Riak Java Client is for the new v2.0 client, to be used with 
 Riak 2.0.

Previous versions:

[riak-client-1.4.2](https://github.com/basho/riak-java-client/tree/1.4.2) - For use with Riak 1.4.x

[riak-client-1.1.3](https://github.com/basho/riak-java-client/tree/1.1.3) - For use with < Riak 1.4.0

This client is published to Maven Central and can be included in your project by adding:

```xml
<dependencies>
  <dependency>
    <groupId>com.basho.riak</groupId>
    <artifactId>riak-client</artifactId>
    <version>2.0.0</version>
  </dependency>
  ...
</dependencies>
```

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

```java
RiakNode.Builder builder = new RiakNode.Builder();
builder.withMinConnections(10);
builder.withMaxConnections(50);

List<String> addresses = new LinkedList<String>();
addresses.add("192.168.1.1");
addresses.add("192.168.1.2");
addresses.add("192.168.1.3");

List<RiakNode> nodes = RiakNode.Builder.buildNodes(builder, addresses);
RiakCluster cluster = new RiakCluster.Builder(nodes).build();
cluster.start();

RiakClient client = new RiakClient(cluster);
```

Once you have a client, commands from the `com.basho.riak.client.api.commands.*` 
packages are built then executed by the client:

```java
Namespace ns = new Namespace("default","my_bucket");
Location loc = new Location(ns, "my_key");
FetchValue fv = new FetchValue.Builder(loc).build();
FetchValue.Response response = client.execute(fv);
RiakObject obj = response.getValue(RiakObject.class);
```


