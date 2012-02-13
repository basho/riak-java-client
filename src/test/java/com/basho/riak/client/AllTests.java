/*
 * This file is provided to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.basho.riak.client;

import java.io.IOException;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.bucket.WriteBucketTest;
import com.basho.riak.client.cap.ClobberMutationTest;
import com.basho.riak.client.cap.QuoraTest;
import com.basho.riak.client.convert.ConversionUtilTest;
import com.basho.riak.client.convert.RiakBeanSerializerModifierTest;
import com.basho.riak.client.convert.RiakJacksonModuleTest;
import com.basho.riak.client.convert.UsermetaConverterTest;
import com.basho.riak.client.http.TestRiakBucketInfo;
import com.basho.riak.client.http.TestRiakClient;
import com.basho.riak.client.http.TestRiakConfig;
import com.basho.riak.client.http.TestRiakLink;
import com.basho.riak.client.http.TestRiakObject;
import com.basho.riak.client.http.itest.ITestBasic;
import com.basho.riak.client.http.itest.ITestDataLoad;
import com.basho.riak.client.http.itest.ITestMapReduce;
import com.basho.riak.client.http.itest.ITestMapReduceSearch;
import com.basho.riak.client.http.itest.ITestSecondaryIndexes;
import com.basho.riak.client.http.itest.ITestStreaming;
import com.basho.riak.client.http.itest.ITestWalk;
import com.basho.riak.client.http.mapreduce.TestMapReduceBuilder;
import com.basho.riak.client.http.mapreduce.TestMapReduceFunctions;
import com.basho.riak.client.http.plain.TestConvertToCheckedExceptions;
import com.basho.riak.client.http.plain.TestPlainClient;
import com.basho.riak.client.http.request.TestRequestMeta;
import com.basho.riak.client.http.response.TestBucketResponse;
import com.basho.riak.client.http.response.TestDefaultHttpResponse;
import com.basho.riak.client.http.response.TestFetchResponse;
import com.basho.riak.client.http.response.TestHttpResponseDecorator;
import com.basho.riak.client.http.response.TestListBucketsResponse;
import com.basho.riak.client.http.response.TestStoreResponse;
import com.basho.riak.client.http.response.TestStreamedKeysCollection;
import com.basho.riak.client.http.response.TestStreamedSiblingsCollection;
import com.basho.riak.client.http.response.TestWalkResponse;
import com.basho.riak.client.http.response.TestStatsResponse;
import com.basho.riak.client.http.util.TestBranchableInputStream;
import com.basho.riak.client.http.util.TestClientHelper;
import com.basho.riak.client.http.util.TestClientUtils;
import com.basho.riak.client.http.util.TestCollectionWrapper;
import com.basho.riak.client.http.util.TestLinkHeader;
import com.basho.riak.client.http.util.TestMultipart;
import com.basho.riak.client.http.util.TestOneTokenInputStream;
import com.basho.riak.client.http.util.TestStreamedMultipart;
import com.basho.riak.client.itest.ITestDomainBucketHTTP;
import com.basho.riak.client.itest.ITestDomainBucketPB;
import com.basho.riak.client.itest.ITestHTTPBucket;
import com.basho.riak.client.itest.ITestHTTPClient;
import com.basho.riak.client.itest.ITestHTTPLinkWalk;
import com.basho.riak.client.itest.ITestPBBucket;
import com.basho.riak.client.itest.ITestPBClient;
import com.basho.riak.client.itest.ITestPBLinkWalk;
import com.basho.riak.client.itest.ITestHTTPStats;
import com.basho.riak.client.itest.ITestHTTPClusterStats;
import com.basho.riak.client.operations.DeleteObjectTest;
import com.basho.riak.client.operations.FetchObjectTest;
import com.basho.riak.client.operations.StoreObjectTest;
import com.basho.riak.client.query.BuckeyKeyMapReduceTest;
import com.basho.riak.client.query.filter.LogicalAndFilterTest;
import com.basho.riak.client.query.serialize.FunctionToJsonTest;
import com.basho.riak.client.raw.ClusterClientTest;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.config.ClusterConfigTest;
import com.basho.riak.client.raw.http.HTTPRiakClientFactoryTest;
import com.basho.riak.client.raw.http.NamedErlangFunctionDeserializerTest;
import com.basho.riak.client.raw.http.QuorumDeserializerTest;
import com.basho.riak.client.raw.http.TestKeySource;
import com.basho.riak.client.raw.itest.ITestHTTPClientAdapter;
import com.basho.riak.client.raw.itest.ITestPBClientAdapter;
import com.basho.riak.client.raw.query.indexes.IndexQueryEqualsHashCodeTest;
import com.basho.riak.client.util.CharsetUtilsTest;
import com.basho.riak.client.util.UnmodifiableIteratorTest;
import com.basho.riak.pbc.RiakObjectTest;
import com.basho.riak.pbc.TestBucketProperties;
import com.basho.riak.pbc.itest.ITestRiakConnectionPool;

/**
 * @author russell
 *
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({BuckeyKeyMapReduceTest.class,
    CharsetUtilsTest.class,
    ClobberMutationTest.class,
    ClusterClientTest.class,
    ClusterConfigTest.class,
    ConversionUtilTest.class,
    com.basho.riak.client.raw.http.ConversionUtilTest.class,
    DeleteObjectTest.class,
    FetchObjectTest.class,
    FunctionToJsonTest.class,
    HTTPRiakClientFactoryTest.class,
    ITestBasic.class,
    com.basho.riak.pbc.itest.ITestBasic.class,
    ITestHTTPBucket.class,
    ITestPBBucket.class,
    ITestHTTPClient.class,
    ITestPBClient.class,
    ITestDataLoad.class,
    com.basho.riak.pbc.itest.ITestDataLoad.class,
    ITestDomainBucketHTTP.class,
    ITestDomainBucketPB.class,
    ITestHTTPClient.class,
    ITestHTTPStats.class,
    ITestHTTPClusterStats.class,
    com.basho.riak.pbc.itest.ITestMapReduce.class,
    ITestHTTPLinkWalk.class,
    ITestPBLinkWalk.class,
    ITestMapReduce.class,
    ITestMapReduceSearch.class,
    com.basho.riak.client.itest.ITestMapReduceSearchHTTP.class,
    com.basho.riak.pbc.itest.ITestMapReduceSearch.class,
    ITestPBClient.class,
    ITestPBClientAdapter.class,
    ITestHTTPClientAdapter.class,
    ITestRiakConnectionPool.class,
    ITestSecondaryIndexes.class,
    ITestStreaming.class,
    ITestWalk.class,
    IndexQueryEqualsHashCodeTest.class,
    LogicalAndFilterTest.class,
    NamedErlangFunctionDeserializerTest.class,
    QuoraTest.class,
    QuorumDeserializerTest.class,
    RiakBeanSerializerModifierTest.class,
    RiakFactoryTest.class,
    RiakJacksonModuleTest.class,
    RiakObjectTest.class,
    StoreObjectTest.class,
    TestBranchableInputStream.class,
    TestBucketProperties.class,
    TestBucketResponse.class,
    TestClientHelper.class,
    TestClientUtils.class,
    TestCollectionWrapper.class,
    TestConvertToCheckedExceptions.class,
    TestDefaultHttpResponse.class,
    TestFetchResponse.class,
    TestStatsResponse.class,
    TestHttpResponseDecorator.class,
    TestKeySource.class,
    TestLinkHeader.class,
    TestListBucketsResponse.class,
    TestMapReduceBuilder.class,
    com.basho.riak.pbc.mapreduce.TestMapReduceBuilder.class,
    TestMapReduceFunctions.class,
    com.basho.riak.pbc.mapreduce.TestMapReduceFunctions.class,
    TestMultipart.class,
    TestOneTokenInputStream.class,
    TestPlainClient.class,
    TestRequestMeta.class,
    com.basho.riak.pbc.TestRequestMeta.class,
    TestRiakBucketInfo.class,
    TestRiakClient.class,
    TestRiakConfig.class,
    TestRiakLink.class,
    com.basho.riak.pbc.TestRiakLink.class,
    TestRiakObject.class,
    com.basho.riak.pbc.TestRiakObject.class,
    TestStoreResponse.class,
    TestStreamedKeysCollection.class,
    TestStreamedMultipart.class,
    TestStreamedSiblingsCollection.class,
    TestWalkResponse.class,
    UnmodifiableIteratorTest.class,
    UsermetaConverterTest.class,
    WriteBucketTest.class,
    com.basho.riak.client.itest.ITestMapReduceHTTP.class,
    com.basho.riak.client.itest.ITestMapReducePB.class})
public class AllTests {

    public static void emptyBucket(String bucketName, IRiakClient client) throws RiakException {
        Bucket b = client.fetchBucket(bucketName).execute();
        for (String k : b.keys()) {
            b.delete(k).execute();
        }
    }

    public static void emptyBucket(String bucketName, RawClient client) throws IOException {
        for (String k : client.listKeys(bucketName)) {
            client.delete(bucketName, k);
        }
    }
}
