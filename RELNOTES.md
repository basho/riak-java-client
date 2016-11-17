Release Notes
=============

### 2.1.0

**Notes**
 * Java 7 support has been deprecated, and Java 8+ is now required.

Following issues / PRs addressed:

* [Added HyperLogLog Datatype Support](https://github.com/basho/riak-java-client/pull/669)
* [Added streaming support for ListKeys, ListBuckets, Secondary Index, and MapReduce Commands/Operations](https://github.com/basho/riak-java-client/pull/677)
* [Added cleanup() method to RiakCluster & RiakClient for container Environments](https://github.com/basho/riak-java-client/pull/674)
* [Moved CI from internal buildbot to public Travis CI](https://travis-ci.org/basho/riak-java-client)
* [Enhanced - Reduce the number of charset lookups performed in BinaryValue class](https://github.com/basho/riak-java-client/pull/688)

### 2.0.8

**Notes**
 * This is the last version of Riak Java Client that supports Java 7.

Following issues / PRs addressed:

 * [Upgrade Netty to 4.1.5 to address higher memory usage with Netty 4.1.3](https://github.com/basho/riak-java-client/pull/671)

### 2.0.7

**Notes**
 * This will be the last version of Riak Java Client that supports Java 7.
 * Some of the changes are binary-incompatible with RJC 2.0.6, so you will need to recompile your project with this new version.

Following issues / PRs addressed:

* [Fixed - Disallow 0 as a timeout value for TimeSeries operations](https://github.com/basho/riak-java-client/pull/662)
* Fixed - In `RiakUserMetadata#containsKey()`, use the charset method parameter when encoding the key [[1]](https://github.com/basho/riak-java-client/pull/558), [[2]](https://github.com/basho/riak-java-client/pull/646)
* Fixed - Don't return success to update future after fetch future error [[1]](https://github.com/basho/riak-java-client/pull/633), [[2]](https://github.com/basho/riak-java-client/pull/636)
* [Fixed - Demoted "channel close" log messages to info level](https://github.com/basho/riak-java-client/pull/637)
* [Fixed - Made domain name more invalid for `UnknownHostException` test](https://github.com/basho/riak-java-client/pull/641)
* [Fixed - Separate Content-type and charset in `RiakObject`](https://github.com/basho/riak-java-client/pull/647)
* [Fixed - BinaryValue JSON encoding for MapReduce inputs](https://github.com/basho/riak-java-client/pull/655)
* [Fixed - Catch & handle `BlockingOperationException` in `RiakNode#execute`](https://github.com/basho/riak-java-client/pull/661)
* Added Batch Delete Command [[1]](https://github.com/basho/riak-java-client/pull/487), [[2]](https://github.com/basho/riak-java-client/pull/650)
* Added `equals()`, `hashCode()`, `toString()` to `RiakObject` and associated files [[1]](https://github.com/basho/riak-java-client/pull/557), [[2]](https://github.com/basho/riak-java-client/pull/648)
* Added `getLocation()` to `KvResponseBase` [[1]](https://github.com/basho/riak-java-client/pull/606), [[2]](https://github.com/basho/riak-java-client/pull/643)
* [Added creation of `RiakClient` from a collection of `HostAndPort` objects](https://github.com/basho/riak-java-client/pull/607)
* Added overload of `RiakClient#execute` that accepts a timeout [[1]](https://github.com/basho/riak-java-client/pull/610), [[2]](https://github.com/basho/riak-java-client/pull/642)
* [Added shortcut commands for $bucket and $key 2i indices](https://github.com/basho/riak-java-client/pull/652)
* [Added `isNotFound()` field to data type responses](https://github.com/basho/riak-java-client/pull/654)
* Added - Dataplatform / Riak Spark Connector changes merged back into main client [[1]](https://github.com/basho/riak-java-client/pull/621), [[2]](https://github.com/basho/riak-java-client/pull/626), [[3]](https://github.com/basho/riak-java-client/pull/644), [[4]](https://github.com/basho/riak-java-client/pull/659), [[5]](https://github.com/basho/riak-java-client/pull/665)
* [Updated plugins and dependencies](https://github.com/basho/riak-java-client/pull/631)
* [Updated TS objects and Commands for TS 1.4](https://github.com/basho/riak-java-client/pull/651)
* [Enhanced - Made Integration Tests Great Again](https://github.com/basho/riak-java-client/pull/657)
* [Removed Antlr dependency](https://github.com/basho/riak-java-client/pull/629)

Special thanks to @bwittwer, @stela, @gerardstannard, @christopherfrieler, @guidomedina, @Tolsi, @hankipanky, @gfbett, @TimurFayruzov, @urzhumskov, @srgg, @aleksey-suprun, @jbrisbin, @christophermancini, and @lukebakken for all the PRs, reported issues, and reviews.

### 2.0.6
Following issues / PRs addressed:
 * [Removed riak_pb external dependency](https://github.com/basho/riak-java-client/pull/615)
 * [Fixed connection handling deadlock issue](https://github.com/basho/riak-java-client/pull/598)
 * [Added TimeSeries Create Table command](https://github.com/basho/riak-java-client/pull/602/)
 * [Added Erlang Term Serialization support for some Time Series operations](https://github.com/basho/riak-java-client/pull/611/)

### 2.0.5
Following issues / PRs addressed:
 * [Fix DNS cache issue when using the client with ELB](https://github.com/basho/riak-java-client/pull/573)
 * [Remove superfluous declarations of UnknownHostException](https://github.com/basho/riak-java-client/pull/553)
 * [Allow RiakNode max connections to be set to 0](https://github.com/basho/riak-java-client/pull/582)
 * [Fix buildbot support, speed up some tests](https://github.com/basho/riak-java-client/pull/596)
 * [Update Security Tests to use common client test setup & certs, fix Linux TLS setup race condition](https://github.com/basho/riak-java-client/pull/595)
 * [Improve Search Tests, fix bug that doesn't allow counts via search, add n_val property to Yokozuna Index Creation](https://github.com/basho/riak-java-client/pull/594)
 * [Improve Flaky Tests](https://github.com/basho/riak-java-client/pull/593)
 * [Scala Typing Improvements](https://github.com/basho/riak-java-client/pull/591)
 * [Added Location Accessor to FetchDataType base class](https://github.com/basho/riak-java-client/pull/590)

### 2.0.4
Following issues / PRs addressed:
 * [Time Series - Create Tables Support & Tests](https://github.com/basho/riak-java-client/pull/588)
 * [Time Series - Describe Table Support & Tests](https://github.com/basho/riak-java-client/pull/589)
 * [General - Add Release Notes, better README, and Top Level License](https://github.com/basho/riak-java-client/pull/583)

### 2.0.3
Following issues / PRs addressed:
 * [Time Series support](https://github.com/basho/riak-java-client/pull/543)
 * [Upgrade Netty to 4.0.33](https://github.com/basho/riak-java-client/pull/581)
 * [Add HostAndPort class - Simplify Cluster Creation](https://github.com/basho/riak-java-client/pull/577)
 * [ListBuckets ignores bucket type parameter](https://github.com/basho/riak-java-client/pull/566)
 * [Client settable Default Charset](https://github.com/basho/riak-java-client/pull/550)
 * [RiakIndex annotation should not have a default value](https://github.com/basho/riak-java-client/pull/541)
 * [Typo in shaded package name](https://github.com/basho/riak-java-client/pull/555)
 * Deprecated Java 6 Support

### 2.0.2
Following issues / PRs addressed:
 * [Option to temporarily queue commands when maxConnections is reached](https://github.com/basho/riak-java-client/issues/510)
 * [Made mocking easier with K/V commands](https://github.com/basho/riak-java-client/pull/528)
 * [Improved JavaDocs for Search and MapReduce](https://github.com/basho/riak-java-client/pull/524)
 * [Added Reset Bucket Properties Command](https://github.com/basho/riak-java-client/pull/522)
 * [Upgraded Animal Sniffer Maven Plugin](https://github.com/basho/riak-java-client/pull/514)
 * [Fixed Java 6 Runtime Compatibility](https://github.com/basho/riak-java-client/pull/530)
 * [Partially fixed missing No UnknownHostException, added logging](https://github.com/basho/riak-java-client-client/pull/529)
 * [Fixed Java 8 Build Support](https://github.com/basho/riak-java-client/pull/517)
 * [Removed dependency on Sun Tools jar](https://github.com/basho/riak-java-client/pull/517)
 * [Fixed an inconsistency of visibility at secondary index query.response.get entries](https://github.com[com/basho/riak-java-client/pull/515)
 * [Fixed an inconsistency in StringBinIndex.named()returning type](https://github.com/basho/riak-java-client/pull/511)
