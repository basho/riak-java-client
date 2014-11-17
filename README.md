## Riak Java client v2.0

This branch of the Riak Java Client is for the new v2.0 client, to be used with 
 Riak 2.0.

Previous versions:

[riak-client-1.4.4](https://github.com/basho/riak-java-client/tree/1.4.4) - For use with Riak 1.4.x

[riak-client-1.1.4](https://github.com/basho/riak-java-client/tree/1.1.4) - For use with < Riak 1.4.0

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
RiakClient client = 
    RiakClient.newClient("192.168.1.1","192.168.1.2","192.168.1.3");
```

The RiakClient object is thread safe and may be shared across multiple threads.

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
RiakClient client = new RiakClient(cluster)
```

Once you have a client, commands from the [com.basho.riak.client.api.commands.*](#riakcommand-subclasses) 
packages are built then executed by the client.

Some basic examples of building and executing these commands is shown
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
mu.update("map_key_2", ru2);
UpdateMap update = new UpdateMap.Builder(location, mu).build();
client.execute(update);
```

 <h1>RiakCommand subclasses</h1>
 <h4>Fetching, storing and deleting objects</h4>
 <ul>
 <li><a href="http://basho.github.io/riak-java-client/2.0.0/com/basho/riak/client/api/commands/kv/FetchValue.html" title="class in com.basho.riak.client.api.commands.kv"><code>FetchValue</code></a></li>
 <li><a href="http://basho.github.io/riak-java-client/2.0.0/com/basho/riak/client/api/commands/kv/MultiFetch.html" title="class in com.basho.riak.client.api.commands.kv"><code>MultiFetch</code></a></li>
 <li><a href="http://basho.github.io/riak-java-client/2.0.0/com/basho/riak/client/api/commands/kv/StoreValue.html" title="class in com.basho.riak.client.api.commands.kv"><code>StoreValue</code></a></li>
 <li><a href="http://basho.github.io/riak-java-client/2.0.0/com/basho/riak/client/api/commands/kv/UpdateValue.html" title="class in com.basho.riak.client.api.commands.kv"><code>UpdateValue</code></a></li>
 <li><a href="http://basho.github.io/riak-java-client/2.0.0/com/basho/riak/client/api/commands/kv/DeleteValue.html" title="class in com.basho.riak.client.api.commands.kv"><code>DeleteValue</code></a></li>
 </ul>
 <h4>Listing keys in a namespace</h4>
 <ul><li><a href="http://basho.github.io/riak-java-client/2.0.0/com/basho/riak/client/api/commands/kv/ListKeys.html" title="class in com.basho.riak.client.api.commands.kv"><code>ListKeys</code></a></li></ul>
 <h4>Secondary index (2i) commands</h4>
 <ul>
 <li><a href="http://basho.github.io/riak-java-client/2.0.0/com/basho/riak/client/api/commands/indexes/RawIndexQuery.html" title="class in com.basho.riak.client.api.commands.indexes"><code>RawIndexQuery</code></a></li>
 <li><a href="http://basho.github.io/riak-java-client/2.0.0/com/basho/riak/client/api/commands/indexes/BinIndexQuery.html" title="class in com.basho.riak.client.api.commands.indexes"><code>BinIndexQuery</code></a></li>
 <li><a href="http://basho.github.io/riak-java-client/2.0.0/com/basho/riak/client/api/commands/indexes/IntIndexQuery.html" title="class in com.basho.riak.client.api.commands.indexes"><code>IntIndexQuery</code></a></li>
 <li><a href="http://basho.github.io/riak-java-client/2.0.0/com/basho/riak/client/api/commands/indexes/BigIntIndexQuery.html" title="class in com.basho.riak.client.api.commands.indexes"><code>BigIntIndexQuery</code></a></li>
 </ul>
 <h4>Fetching and storing datatypes (CRDTs)</h4>
 <ul>
 <li><a href="http://basho.github.io/riak-java-client/2.0.0/com/basho/riak/client/api/commands/datatypes/FetchCounter.html" title="class in com.basho.riak.client.api.commands.datatypes"><code>FetchCounter</code></a></li>
 <li><a href="http://basho.github.io/riak-java-client/2.0.0/com/basho/riak/client/api/commands/datatypes/FetchSet.html" title="class in com.basho.riak.client.api.commands.datatypes"><code>FetchSet</code></a></li>
 <li><a href="http://basho.github.io/riak-java-client/2.0.0/com/basho/riak/client/api/commands/datatypes/FetchMap.html" title="class in com.basho.riak.client.api.commands.datatypes"><code>FetchMap</code></a></li>
 <li><a href="http://basho.github.io/riak-java-client/2.0.0/com/basho/riak/client/api/commands/datatypes/UpdateCounter.html" title="class in com.basho.riak.client.api.commands.datatypes"><code>UpdateCounter</code></a></li>
 <li><a href="http://basho.github.io/riak-java-client/2.0.0/com/basho/riak/client/api/commands/datatypes/UpdateSet.html" title="class in com.basho.riak.client.api.commands.datatypes"><code>UpdateSet</code></a></li>
 <li><a href="http://basho.github.io/riak-java-client/2.0.0/com/basho/riak/client/api/commands/datatypes/UpdateMap.html" title="class in com.basho.riak.client.api.commands.datatypes"><code>UpdateMap</code></a></li>
 </ul>
 <h4>Querying and modifying buckets</h4>
 <ul>
 <li><a href="http://basho.github.io/riak-java-client/2.0.0/com/basho/riak/client/api/commands/buckets/FetchBucketProperties.html" title="class in com.basho.riak.client.api.commands.buckets"><code>FetchBucketProperties</code></a></li>
 <li><a href="http://basho.github.io/riak-java-client/2.0.0/com/basho/riak/client/api/commands/buckets/StoreBucketProperties.html" title="class in com.basho.riak.client.api.commands.buckets"><code>StoreBucketProperties</code></a></li>
 <li><a href="http://basho.github.io/riak-java-client/2.0.0/com/basho/riak/client/api/commands/buckets/ListBuckets.html" title="class in com.basho.riak.client.api.commands.buckets"><code>ListBuckets</code></a></li>
 </ul>
 <h4>Search commands</h4>
 <ul>
 <li><a href="http://basho.github.io/riak-java-client/2.0.0/com/basho/riak/client/api/commands/search/Search.html" title="class in com.basho.riak.client.api.commands.search"><code>Search</code></a></li>
 <li><a href="http://basho.github.io/riak-java-client/2.0.0/com/basho/riak/client/api/commands/search/FetchIndex.html" title="class in com.basho.riak.client.api.commands.search"><code>FetchIndex</code></a></li>
 <li><a href="http://basho.github.io/riak-java-client/2.0.0/com/basho/riak/client/api/commands/search/StoreIndex.html" title="class in com.basho.riak.client.api.commands.search"><code>StoreIndex</code></a></li>
 <li><a href="http://basho.github.io/riak-java-client/2.0.0/com/basho/riak/client/api/commands/search/DeleteIndex.html" title="class in com.basho.riak.client.api.commands.search"><code>DeleteIndex</code></a></li>
 <li><a href="http://basho.github.io/riak-java-client/2.0.0/com/basho/riak/client/api/commands/search/FetchSchema.html" title="class in com.basho.riak.client.api.commands.search"><code>FetchSchema</code></a></li>
 <li><a href="http://basho.github.io/riak-java-client/2.0.0/com/basho/riak/client/api/commands/search/StoreSchema.html" title="class in com.basho.riak.client.api.commands.search"><code>StoreSchema</code></a></li>
 </ul>
 <h4>Map-Reduce</h4>
 <ul>
 <li><a href="http://basho.github.io/riak-java-client/2.0.0/com/basho/riak/client/api/commands/mapreduce/BucketMapReduce.html" title="class in com.basho.riak.client.api.commands.mapreduce"><code>BucketMapReduce</code></a></li>
 <li><a href="http://basho.github.io/riak-java-client/2.0.0/com/basho/riak/client/api/commands/mapreduce/BucketKeyMapReduce.html" title="class in com.basho.riak.client.api.commands.mapreduce"><code>BucketKeyMapReduce</code></a></li>
 <li><a href="http://basho.github.io/riak-java-client/2.0.0/com/basho/riak/client/api/commands/mapreduce/IndexMapReduce.html" title="class in com.basho.riak.client.api.commands.mapreduce"><code>IndexMapReduce</code></a></li>
 <li><a href="http://basho.github.io/riak-java-client/2.0.0/com/basho/riak/client/api/commands/mapreduce/SearchMapReduce.html" title="class in com.basho.riak.client.api.commands.mapreduce"><code>SearchMapReduce</code></a></li>
 </ul>

