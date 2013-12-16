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
import com.basho.riak.client.core.operations.ResetBucketTypePropsOperation;
import com.basho.riak.client.core.operations.StoreBucketTypePropsOperation;
import static com.basho.riak.client.core.operations.itest.ITestBase.testBucketType;
import com.basho.riak.client.query.BucketProperties;
import com.basho.riak.client.util.ByteArrayWrapper;
import java.util.concurrent.ExecutionException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Assume;
import org.junit.Test;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class ITestBucketTypeProperties extends ITestBase
{
    
    @Test
    public void testFetchBucketTypeProperties() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(testBucketType);
        BucketProperties props = fetchBucketTypeProps(bucketType);
        assertTrue(props.hasNVal());
        assertTrue(props.hasAllowMulti());
        assertTrue(props.hasBasicQuorum());
        assertTrue(props.hasBigVClock());
        assertTrue(props.hasChashKeyFunction());
        assertTrue(props.hasDw());
        assertTrue(props.hasLastWriteWins());
        assertTrue(props.hasNotFoundOk());
        assertTrue(props.hasOldVClock());
        assertTrue(props.hasPr());
        assertTrue(props.hasPw());
        assertTrue(props.hasR());
        assertTrue(props.hasRiakSearchEnabled());
        assertTrue(props.hasRw());
        assertTrue(props.hasSmallVClock());
        assertTrue(props.hasW());
        assertTrue(props.hasYoungVClock());
        
        assertFalse(props.hasBackend());
        assertFalse(props.hasPostcommitHooks());
        assertFalse(props.hasPrecommitHooks());
        assertFalse(props.hasYokozunaIndex());
    }
    
    @Test
    public void testSetBucketTypeProperties() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(testBucketType);
        BucketProperties props = 
            new BucketProperties()
                .withAllowMulti(true)
                .withNVal(4)
                .withR(1);
        
        storeBucketTypeProps(bucketType, props);
        
        props = fetchBucketTypeProps(bucketType);
        
        assertEquals(props.getNVal(), Integer.valueOf(4));
        assertEquals(props.getR().getIntValue(), 1);
        assertTrue(props.getAllowMulti());
    }
    
    @Test
    public void testResetBucketTypeProps() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(testBucketType);
        BucketProperties props = 
            new BucketProperties()
                .withAllowMulti(true)
                .withNVal(4);
        
        storeBucketTypeProps(bucketType, props);
        props = fetchBucketTypeProps(bucketType);
        
        assertEquals(props.getNVal(), Integer.valueOf(4));
        assertTrue(props.getAllowMulti());
        
        ResetBucketTypePropsOperation resetOp = 
            new ResetBucketTypePropsOperation.Builder(bucketType).build();
        cluster.execute(resetOp);
        resetOp.get();
        
        props = fetchBucketTypeProps(bucketType);
        assertEquals(Integer.valueOf(3), props.getNVal());
        assertTrue(props.getAllowMulti());
    }
    
    private BucketProperties fetchBucketTypeProps(ByteArrayWrapper bucketType) throws InterruptedException, ExecutionException
    {
        FetchBucketTypePropsOperation op = new FetchBucketTypePropsOperation(bucketType);
        cluster.execute(op);
        return op.get();
    }
    
    private void storeBucketTypeProps(ByteArrayWrapper bucketType, BucketProperties props) throws InterruptedException, ExecutionException
    {
        StoreBucketTypePropsOperation op = new StoreBucketTypePropsOperation(bucketType, props);
        cluster.execute(op);
        
        op.get();
    }
    
}
