Release Notes
=============

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
