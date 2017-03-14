/*
 * Copyright 2017 Basho Technologies Inc.
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
import com.basho.riak.client.api.commands.buckets.FetchBucketTypeProperties;
import com.basho.riak.client.api.commands.buckets.StoreBucketTypeProperties;
import com.basho.riak.client.core.operations.FetchBucketTypePropsOperation;
import com.basho.riak.client.core.operations.itest.ITestAutoCleanupBase;
import com.basho.riak.client.core.query.BucketProperties;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

/**
 * @author Luke Bakken <lbakken@basho.com>
 */
public class ITestBucketTypeProps extends ITestAutoCleanupBase
{
    private final String bt = "plain";

    @Test
    public void simpleTest() throws ExecutionException, InterruptedException
    {
        RiakClient client = new RiakClient(cluster);

        // set Nval = 4 and R = 1
        StoreBucketTypeProperties storeProps = new StoreBucketTypeProperties.Builder(bt)
                .withNVal(4)
                .withR(1)
                .build();
        client.execute(storeProps);

        FetchBucketTypeProperties fetchProps = new FetchBucketTypeProperties.Builder(bt).build();
        FetchBucketTypePropsOperation.Response fetchResponse = client.execute(fetchProps);
        BucketProperties bp = fetchResponse.getProperties();

        // assert that it took
        assertEquals(bp.getNVal(), Integer.valueOf(4));
        assertEquals(bp.getR().getIntValue(), 1);

        // reset back to type defaults
        storeProps = new StoreBucketTypeProperties.Builder(bt)
                .withNVal(3)
                .withR(Quorum.quorumQuorum().getIntValue())
                .build();
        client.execute(storeProps);

        fetchProps = new FetchBucketTypeProperties.Builder(bt).build();
        fetchResponse = client.execute(fetchProps);
        bp = fetchResponse.getProperties();

        // assert that it took
        assertEquals(bp.getNVal(), Integer.valueOf(3));
        assertEquals(bp.getR(), Quorum.quorumQuorum());
    }

    @Test(expected=IllegalArgumentException.class)
    public void fetchBucketTypePropsIllegalArgumentException() throws ExecutionException, InterruptedException
    {
        FetchBucketTypeProperties fetchProps = new FetchBucketTypeProperties.Builder((String)null).build();
    }

    @Test(expected=IllegalArgumentException.class)
    public void storeBucketTypePropsIllegalArgumentException() throws ExecutionException, InterruptedException
    {
        StoreBucketTypeProperties storeProps = new StoreBucketTypeProperties.Builder((String)null)
                .withNVal(4)
                .withR(1)
                .build();
    }
}
