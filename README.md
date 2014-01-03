## Riak Java client v2.0 development branch

This branch of the Riak Java Client is the development branch for the new v2.0 client, to be used with the
upcoming Riak 2.0 (also currently in pre-release). 

The current release branches are:

[riak-client-2.0-technical-preview](https://github.com/basho/riak-java-client/tree/master) - Core preview for Riak 2.0

[riak-client-1.4.2](https://github.com/basho/riak-java-client/tree/1.4.2) - For use with Riak 1.4+

[riak-client-1.1.3](https://github.com/basho/riak-java-client/tree/1.1.3) - For use with < Riak 1.4.0

## Overview

Version 2.0 of the Riak Java client is a completely new codebase. It relies on 
Netty4 in the core for handling network operations, and will eventually present 
a new user API for interacting with Riak.

With Riak 2.0 in technical preview there has been demand for a client able to 
exercise the new Riak features (mainly, CRDTs). While our new user API is still 
under development, the core of the client which it sits on top of is usable 
for this purpose. 

What this preview is **not**: 

- Ready for production use.
- The user level API. 
- Something that will not change prior to release.

## Getting started with the preview client core

The new client is designed to model a Riak cluster:

![RJC model](http://brianroach.info/blog/wp-content/uploads/2013/10/RJC2.png)

Starting up the core involves instantiating one or more `RiakNode`s and 
adding them to a `RiakCluster`:

```java
RiakNode node = new RiakNode.Builder().build();
RiakCluster cluster = new RiakCluster.Builder(node).build();
cluster.start();
```

Once you have a cluster up and running, you can create operations and execute them:

```java
BinaryValue bucket = BinaryValue.create("test_bucket");
BinaryValue key = BinaryValue.create("test_key");
FetchOperation fetchop =
            new FetchOperation.Builder(bucket, key)
                .build();
cluster.execute(fetchOp);
FetchOperation.Response resp = fetchOp.get();
if(resp.isNotFound()) 
{
    System.out.println("Not found");
} 
else 
{
    for (RiakObject ro : resp.getObjectList())
    {
        System.out.println("value: " + ro.getValue());
        System.out.println(ro.isDeleted());
    }
}
```

Note that the core is asynchronous; all operations are subclassed from `FutureOperation`
which implements the `RiakFuture` interface. 

## Roadmap

The user API that will sit on top of the core is under development. We will be releasing 
a Release candidate for the RJC v2.0 as soon as it is ready for people to try out and 
provide feedback on.
