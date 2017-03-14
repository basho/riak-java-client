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

import com.basho.riak.client.core.operations.FetchBucketTypePropsOperation;
import com.basho.riak.client.core.operations.StoreBucketTypePropsOperation;
import com.basho.riak.client.core.query.BucketProperties;
import java.util.concurrent.ExecutionException;
import static org.junit.Assert.*;

import com.basho.riak.client.core.util.BinaryValue;
import org.junit.Assume;
import org.junit.Test;

/**
 * @author Luke Bakken <lbakken@basho.com>
 */
public class ITestBucketTypeProperties extends ITestAutoCleanupBase
{
    @Test
    public void testFetchBucketTypeProps() throws InterruptedException, ExecutionException
    {
        BucketProperties props = fetchBucketTypeProps(plainBucketType);
        assertTrue(props.hasNVal());
        assertTrue(props.hasAllowMulti());
        assertTrue(props.hasBasicQuorum());
        assertTrue(props.hasBigVClock());
        assertTrue(props.hasChashKeyFunction());
        assertTrue(props.hasLinkwalkFunction());
        assertTrue(props.hasDw());
        assertTrue(props.hasLastWriteWins());
        assertTrue(props.hasLinkwalkFunction());
        assertTrue(props.hasNotFoundOk());
        assertTrue(props.hasOldVClock());
        assertTrue(props.hasPr());
        assertTrue(props.hasPw());
        assertTrue(props.hasR());
        assertTrue(props.hasLegacyRiakSearchEnabled());
        assertTrue(props.hasRw());
        assertTrue(props.hasSmallVClock());
        assertTrue(props.hasW());
        assertTrue(props.hasYoungVClock());

        assertFalse(props.hasBackend());
        assertFalse(props.hasPostcommitHooks());
        assertFalse(props.hasPrecommitHooks());
        assertFalse(props.hasSearchIndex());
    }

    @Test
    public void testSetBucketTypeProps() throws InterruptedException, ExecutionException
    {
        StoreBucketTypePropsOperation.Builder builder =
            new StoreBucketTypePropsOperation.Builder(plainBucketType)
                .withAllowMulti(true)
                .withNVal(4);

        storeBucketTypeProps(builder);

        BucketProperties props = fetchBucketTypeProps(plainBucketType);

        assertEquals(props.getNVal(), Integer.valueOf(4));
        assertTrue(props.getAllowMulti());
    }

    @Test
    public void testSetHllPrecisionOnBucketType() throws ExecutionException, InterruptedException
    {
        Assume.assumeTrue(testHllDataType);
        StoreBucketTypePropsOperation.Builder storeOp =
                new StoreBucketTypePropsOperation.Builder(hllBucketType).withHllPrecision(13);

        storeBucketTypeProps(storeOp);

        BucketProperties props = fetchBucketTypeProps(hllBucketType);

        assertEquals(Integer.valueOf(13), props.getHllPrecision());
    }

    @Test(expected = ExecutionException.class)
    public void testIncreaseHllPrecisionThrowsError() throws ExecutionException, InterruptedException
    {
        Assume.assumeTrue(testHllDataType);
        StoreBucketTypePropsOperation.Builder storeOp =
                new StoreBucketTypePropsOperation.Builder(hllBucketType).withHllPrecision(15);

        storeBucketTypeProps(storeOp);
    }

    private BucketProperties fetchBucketTypeProps(BinaryValue bucketType) throws InterruptedException, ExecutionException
    {
        FetchBucketTypePropsOperation.Builder builder = new FetchBucketTypePropsOperation.Builder(bucketType);
        FetchBucketTypePropsOperation op = builder.build();
        cluster.execute(op);
        return op.get().getProperties();
    }

    private void storeBucketTypeProps(StoreBucketTypePropsOperation.Builder builder) throws InterruptedException, ExecutionException
    {
        StoreBucketTypePropsOperation op = builder.build();
        cluster.execute(op);
        op.get();
    }
}
