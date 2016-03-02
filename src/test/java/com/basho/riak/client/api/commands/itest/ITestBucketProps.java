/*
 * Copyright 2014 Basho Technologies Inc.
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

package com.basho.riak.client.api.commands.itest;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.cap.Quorum;
import com.basho.riak.client.api.commands.buckets.FetchBucketProperties;
import com.basho.riak.client.core.operations.FetchBucketPropsOperation;
import com.basho.riak.client.core.operations.itest.ITestAutoCleanupBase;
import com.basho.riak.client.core.query.BucketProperties;
import com.basho.riak.client.api.commands.buckets.ResetBucketProperties;
import com.basho.riak.client.api.commands.buckets.StoreBucketProperties;
import com.basho.riak.client.core.query.Namespace;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

/**
 *
 * @author Chris Mancini <cmancini at basho dot com>
 */
public class ITestBucketProps extends ITestAutoCleanupBase
{
    private final String propsBucketName = bucketName.toString() + "_props";

    @Test
    public void simpleTest() throws ExecutionException, InterruptedException
    {
        RiakClient client = new RiakClient(cluster);
        Namespace ns = new Namespace(Namespace.DEFAULT_BUCKET_TYPE, propsBucketName);

        // set Nval = 4 and R = 1
        StoreBucketProperties storeProps = new StoreBucketProperties.Builder(ns)
                .withNVal(4)
                .withR(1)
                .build();
        client.execute(storeProps);

        FetchBucketProperties fetchProps = new FetchBucketProperties.Builder(ns).build();
        FetchBucketPropsOperation.Response fetchResponse = client.execute(fetchProps);
        BucketProperties bp = fetchResponse.getBucketProperties();

        // assert that it took
        assertEquals(bp.getNVal(), Integer.valueOf(4));
        assertEquals(bp.getR().getIntValue(), 1);

        // reset back to type defaults
        ResetBucketProperties resetProps = new ResetBucketProperties.Builder(ns).build();
        client.execute(resetProps);

        fetchProps = new FetchBucketProperties.Builder(ns).build();
        fetchResponse = client.execute(fetchProps);
        bp = fetchResponse.getBucketProperties();

        // assert that it took
        assertEquals(bp.getNVal(), Integer.valueOf(3));
        assertEquals(bp.getR(), Quorum.quorumQuorum());
    }

    @Test(expected=IllegalArgumentException.class)
    public void fetchBucketPropsIllegalArgumentException() throws ExecutionException, InterruptedException
    {
        RiakClient client = new RiakClient(cluster);

        FetchBucketProperties fetchProps = new FetchBucketProperties.Builder(null).build();
        FetchBucketPropsOperation.Response fetchResponse = client.execute(fetchProps);
    }

    @Test(expected=IllegalArgumentException.class)
    public void storeBucketPropsIllegalArgumentException() throws ExecutionException, InterruptedException
    {
        RiakClient client = new RiakClient(cluster);

        StoreBucketProperties storeProps = new StoreBucketProperties.Builder(null)
                .withNVal(4)
                .withR(1)
                .build();
        client.execute(storeProps);
    }

    @Test(expected=IllegalArgumentException.class)
    public void resetBucketPropsIllegalArgumentException() throws ExecutionException, InterruptedException
    {
        RiakClient client = new RiakClient(cluster);

        // reset back to type defaults
        ResetBucketProperties resetProps = new ResetBucketProperties.Builder(null).build();
        client.execute(resetProps);
    }
}
