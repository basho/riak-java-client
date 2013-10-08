/*
 * Copyright 2013 Basho Technologies Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.basho.riak.client.core.operations.itest;

import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.operations.DeleteOperation;
import com.basho.riak.client.core.operations.ListKeysOperation;
import com.basho.riak.client.core.operations.ResetBucketPropsOperation;
import com.basho.riak.client.util.ByteArrayWrapper;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public abstract class ITestBase
{
    protected static RiakCluster cluster;
    protected static boolean testYokozuna;
    protected static boolean test2i;
    protected static boolean testBucketType;
    protected static ByteArrayWrapper bucketName;
    
    @BeforeClass
    public static void setUp() throws UnknownHostException
    {
        testYokozuna = Boolean.parseBoolean(System.getProperty("com.basho.riak.yokozuna"));
        test2i = Boolean.parseBoolean(System.getProperty("com.basho.riak.2i"));
        testBucketType = Boolean.parseBoolean(System.getProperty("com.basho.riak.buckettype"));
        
        bucketName = ByteArrayWrapper.unsafeCreate("ITestBase".getBytes());
        RiakNode.Builder builder = new RiakNode.Builder()
                                        .withMinConnections(10);
        
        cluster = new RiakCluster.Builder(builder.build()).build();
        cluster.start();
    }
    
    @Before
    public void beforeTest() throws InterruptedException, ExecutionException
    {
        resetAndEmptyBucket(bucketName);
    }
    
    @AfterClass
    public static void tearDown()
    {
        cluster.stop();
    }
    
    protected void resetAndEmptyBucket(ByteArrayWrapper name) throws InterruptedException, ExecutionException
    {
        ListKeysOperation keysOp = new ListKeysOperation(name);
        cluster.execute(keysOp);
        List<ByteArrayWrapper> keyList = keysOp.get();
        for (ByteArrayWrapper k : keyList)
        {
            DeleteOperation delOp = new DeleteOperation(name, k);
            cluster.execute(delOp);
            delOp.get();
        }
        ResetBucketPropsOperation resetOp = new ResetBucketPropsOperation(name);
        cluster.execute(resetOp);
        resetOp.get();
        
    }
    
}
