

This document describes how to use this client to interact with Riak.  See the [DEVELOPERS](https://github.com/basho/riak-java-client/blob/master/DEVELOPERS.md) document for a technical overview of the project.

# Overview #
There are two interfaces to Riak: HTTP and Protocol Buffers.

There are two client implementations in this library: an Apache HTTPClient based REST client and (since 0.14.0), a Protocol buffers client.

# Protocol Buffers client #

An example of how to use the protocol buffers client can be seen in the integration tests [here](https://github.com/basho/riak-java-client/tree/master/src/test/java/com/basho/riak/pbc/itest).
A quick primer for get/put is [here](https://github.com/basho/riak-java-client/blob/master/src/test/java/com/basho/riak/pbc/itest/ITestBasic.java#L180).

The protocol buffers interface is far faster than the REST interface.

# REST client #

The HTTP Riak client uses Commons HttpClient to perform HTTP requests. It provides:

*   **HttpClient**-provided functionality such as connection pooling, timeouts, and retries, which is not provided by `HttpURLConnection`.

*   **Link** and **Map/Reduce** query building support.

*   **HTTP response data** returned directly to the client rather than via exceptions. While this slightly couples the domain model with the underlying HTTP model, it gives the benefit of allowing the full suite of HTTP to be used (in possibly unforseen ways) without requiring modifications to the client library. In reality, clients need to and do understand that each operation in fact translates to an HTTP operation.  In any case, **PlainClient** provides the more traditional interface with domain objects and checked exceptions which can be used when appropriate.

*   **Stream handling** for GET requests.

*   **Exceptions** are unchecked and the **RiakExceptionHandler** interface allows all exceptions to be handled in a central location. This means the client does not need to wrap each operation in try/catch blocks.

# Including in your project #

To use `riak-client` in a [Maven](http://maven.apache.org/) project, add the following dependency to `pom.xml`:

	<dependency>
	  <groupId>com.basho.riak</groupId>
	  <artifactId>riak-client</artifactId>
	  <version>0.14.1</version>
	  <type>pom</type>
	</dependency>

To build and install from source, first install Apache Maven (http://maven.apache.org/download.html). With Maven installed, run:

    mvn clean install

# Quick start #

Connect to Riak:

    RiakClient riak = new RiakClient("http://localhost:8098/riak");

Build an object:

    RiakObject o = new RiakObject("bucket", "key", "value");

Store it:

    riak.store(o);

Retrieve it:

    FetchResponse r = riak.fetch("bucket", "key");
    if (r.hasObject())
        o = r.getObject();

Update it:

    o.setValue("foo");
    riak.store(o);

Handling siblings:

    if (r.hasSiblings())
        Collection<RiakObject> siblings = r.getSiblings();


# Connecting #

Connect to a Riak server by specifying the base URL of the Riak HTTP interface:
    
    RiakClient riak = new RiakClient("http://localhost:8098/riak");

HttpClient parameters can be provided using a RiakConfig object:

    RiakConfig config = new RiakConfig("http://localhost:8098/riak");
    
    config.setTimeout(2000);        // 2 second connection timeout
    config.setMaxConnections(50);   // 50 concurrent connections
    
    RiakClient riak = new RiakClient(config);

The HttpClient instance itself can also be given:

    MultiThreadedHttpConnectionManager m = new MultiThreadedHttpConnectionManager();
    m.getParams().setIntParameter(HttpConnectionManagerParams.MAX_TOTAL_CONNECTIONS, 50);
    
    HttpClient http = new HttpClient(m);
    http.getParams().setLongParameter(HttpClientParams.CONNECTION_MANAGER_TIMEOUT, 2000);
    
    RiakConfig config = new RiakConfig("http://localhost:8098/riak");
    config.setHttpClient(http);
    
    RiakClient riak = new RiakClient(config);

As an alternative to `RiakClient`, the `PlainClient` interface can be used, which exposes a different interface that hides HTTP response information from results.

    PlainClient plain = PlainClient.getClient("http://localhost:8098/riak");
    PlainClient plain = PlainClient.getClient(new RiakConfig("http://localhost:8098/riak"));

# Operations #

Using RiakClient, you can manipulate Riak objects and buckets.

## Store Objects ##

First create the object type corresponding to the interface being used and fill in any metadata and value.  Then, store the object and check the server response. The response contains the updated vclock, last modified date, and vtag of the object, if it was returned by the server.

    RiakObject o = new RiakObject("bucket", "key", "value");
    o.getUsermeta().put("custom-max-age", "60");
    o.setContentType("text/plain");
    
    StoreResponse r = riak.store(o);
    if (r.isSuccess()) obj.updateMeta(r);

Or, with Plain:

    plain.store(new RiakObject("a", "b"));

The request `w` and `dw` values (number of write or durable write responses required for success) can be specified using a RequestMeta object:

    client.store(obj, RequestMeta.writeParams(2 /* w-value */, 2 /* dw-value */));

## Fetch Objects ##

Simply request the object by bucket and key. The response contains the requested object.

    FetchResponse r = riak.fetch("bucket", "key");
    if (r.isSuccess())
        RiakObject o = r.getObject();

With Plain:

    RiakObject o = plain.fetch("bucket", "key");

The request `r` value (number of servers responding to a read request required for success) can be specified using a RequestMeta object:

    client.fetch("bucket", "key", RequestMeta.readParams(2 /* r-value */));

## Fetch Object Metadata ##

This works identically to fetching an object, except that the object's value will not necessarily be populated.

    client.fetchMeta("bucket", "key", /* optional */ RequestMeta.readParams(2));

## Modify Objects ##

To ensure that no conflicts are created when modifying an object, first fetch it, then update and store it.

    FetchResponse r = riak.fetch("bucket", "key");
    if (r.isSuccess()) {
        RiakObject o = r.getObject();
        o.setValue("foo");
        client.store(o);
    }

With Plain:

    RiakObject o = plain.fetch("bucket", "key");
    o.setValue("foo");
    plain.store(o);

## Delete Objects ##

Simply execute the delete method.  The `dw` value can be optionally specified as before with a RequestMeta object.

    client.delete("bucket", "key", RequestMeta.writeParams(null /* no w value */, 2 /* dw value */));

## Handling Conflicts and Siblings ##

The Riak HTTP interface is able to return conflicting versions of the same object, known as siblings.

    // ensure the that allow_mult bucket property is set to true, or Riak will not return siblings.
    FetchResponse r = riak.fetch("bucket", "key");
    if (r.isSuccess() && r.hasSiblings())
        Collection<RiakObject> siblings = r.getSiblings();

With Plain:

    Collection<? extends RiakObject> objects = plain.fetchAll("bucket", "key");

## Streaming Objects ##

To process an object as a stream, implement a StreamHandler and use the stream() method. The input stream of the HTTP response is given to the handler.

    client.stream("bucket", "key", new MyStreamHandler(), RequestMeta.readParams(1 /* r value */));

RiakClient also contains a stream() method that returns a standard RiakObject with a value stream. The user is responsible for closing the response in this case.

    FetchResponse r;
    try {
        r = riak.stream("bucket", "key");
        if (r.isSuccess()) {
            InputStream valueStream = r.getObject().getValueStream();
            
            /* process valueStream */
        }
    } finally {
        if (r != null) {
            r.close();
        }
    }

## Buckets Keys and Schema ##

The bucket schema and a list the keys of all the objects in the bucket can be read using the listBucket() method and written using the setBucketSchema() method.  The bucket schema is presented as a JSONObject and contains per-bucket information.  For example, the `allow_mult` property allows Riak to return conflicting versions of the same object.  See the Riak documentation for more information.

    BucketResponse r = riak.listBucket("bucket");
    if (r.isSuccess()) {
        BucketInfo info = r.getBucketInfo();
        Collection<String> keys = info.getKeys();         // list of all object keys in this bucket
        
        // Update the schema and put
        info.setAllowMult(true);
        riak.setBucketSchema("bucket", info);
    }

It is also possible to stream the keys in the bucket, which sends the `?keys=stream` query parameter to Riak.

    BucketResponse r;
    try {
        r = riak.streamBucket("bucket");
        if (r.isSuccess())
            for (String key : r.getBucketInfo().getKeys())
                // process key
    } finally {
        if (r != null)
            r.close();
    }


With Plain:

    RiakBucketInfo info = plain.listBucket("bucket");
    info.getSchema().put("allow_mult", true);
    plain.setBucketSchema("bucket", info);

## Links and Link Walking ##

Links can be stored with each object. A link consists of the target object's bucket and key and a tag to identify the link.

    RiakObject o = new RiakObject("bucket", "key");
    o.getLinks().add(new RiakLink("bucket", "target-object", "link-tag"));

Link walking is performed by calling walk() with a walk specification (see the Riak documentation and JavaDocs for RiakWalkSpec).  A list of lists of objects is returned.  Each list of objects represents all the objects returned in a single step of the walk.

    WalkResponse r = riak.walk("bucket", "key", "bucket,_,1");
    if (r.isSuccess()) {
        List<? extends List<RiakObject>> steps = r.getSteps();
        for (List<RiakObject> step : steps) {
            for (RiakObject o : step) {
                // process the object
            }
        }
    }

Alternatively, the link walk can be built from a `RiakObject`.

    RiakObject o = new RiakObject(riak, "bucket", "key");
    WalkResponse r = o.walk("bucket").run();

## Map/Reduce Queries ##

Refer to the [Riak Javascript Map/Reduce documentation](http://bitbucket.org/basho/riak/src/tip/doc/js-mapreduce.org) for a detailed explanation of how map/reduce works in Riak over HTTP.

First, build a Map/Reduce query by calling `mapReduceOverBucket()` or `mapReduceOverObjects()`. Then execute it by calling `submit()`.

    MapReduceResponse r = riak.mapReduceOverBucket("bucket")
        .link("bucket", "tag", false)
        .map(JavascriptFunction.named("Riak.mapValuesJson"), false)
        .reduce(new ErlangFunction("riak_mapreduce", "reduce_sort"), true)
        .submit();
    if (r.isSuccess()) {
        JSONArray results = r.getResults();
        // process the results array
    }

A Map/Reduce query is built by chaining methods calls that to corresponding Riak Map/Reduce phase types.

* `map(function, keep)`
* `reduce(function, keep)`
* `link(bucket, tag, keep)` 

The `keep` flag determines whether the results from that phase is returned by Riak.

The supported `map()` and `reduce()` function types are

* `ErlangFunction`: an Erlang function specified by a module and function
* `JavascriptFunction`: a named or anonymous Javascript function
* `LinkFunction`: a link specification consisting of a bucket name and tag

For example:

    // Erlang function riak_mapreduce:reduce_sort
    new ErlangFunction("riak_mapreduce", "reduce_sort");
    
    // Built-in named Javascript function Riak.mapValuesJson
    JavascriptFunction.named("Riak.mapValuesJson");
    
    // Unnamed Javascript function
    JavascriptFunction.anon("function(v) { return [v.values[0].data]; }");
    
    // Follow links to bucket "b" tagged with tag "t"
    new LinkFunction("b", "t");

If `submit()` succeeds, Riak returns the query result, which is a JSON array.


# HTTP Request/Response Information #

All of the above operations can also be called with a RequestMeta object to specify extra HTTP headers and query parameters:

    RequestMeta meta = new RequestMeta();
    meta.putHeader("X-Custom-Header", "value");
    meta.addHeader("custom-query-param", "param");
    
    client.fetch("bucket", "key", meta);

The operations also return results that implement the HttpResponse interface, which exposes the HTTP status code, headers, body, and original HttpMethod used for the request. The entity stream, however is closed, so the stream() function should be used to stream objects.

    HttpResponse r = client.fetch("bucket", "key");
    Map<String, String> httpHeaders = r.getHttpHeaders();
    String httpBody = r.getBody();

# Exception Handling #

RiakClient will usually throw the unchecked exceptions RiakIORuntimeException and RiakResponseRuntimeException if there is an error talking to the server or if the server returns a response that can't be parsed.  However, a RiakExceptionHandler can be installed in the client to prevent them from throwing the exceptions. Instead, they are passed to the exception handler.  This allows a user, for example, to consolidate processing for these exceptional cases in a single class and avoid inline try/catch blocks.

In addition, the `ClientUtils.throwChecked()` method allows the unchecked exceptions to be easily converted to checked exceptions. Of course, in this case, methods using the `RiakClient` instance must be careful to declare the exception in their signatures, since the compiler will not produce the usual warnings.

    RiakClient c = new RiakClient("");
    c.setExceptionHandler(new RiakExceptionHandler() {
        
        public void handle(RiakResponseRuntimeException e) {
            // Log and ignore malformed responses. The operation in progress
            // will return an HttpResponse with 0 status code.
            LOG.warn("Received malformed server response", e);
        }
        
        public void handle(RiakIORuntimeException e) {
            // Convert to a checked exception, which should be declared in
            // the calling method signature
            ClientUtils.throwChecked(
                new IOException("Riak connection is down", e.getCause()));
        }
    });

