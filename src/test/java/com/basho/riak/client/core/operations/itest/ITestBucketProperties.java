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

import com.basho.riak.client.api.cap.Quorum;
import com.basho.riak.client.core.operations.FetchBucketPropsOperation;
import com.basho.riak.client.core.operations.StoreBucketPropsOperation;
import com.basho.riak.client.core.query.BucketProperties;
import com.basho.riak.client.core.query.Namespace;
import java.util.concurrent.ExecutionException;
import static org.junit.Assert.*;
import org.junit.Assume;
import org.junit.Test;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class ITestBucketProperties extends ITestAutoCleanupBase
{
    
    @Test
    public void testFetchDefaultBucketProps() throws InterruptedException, ExecutionException
    {
        Namespace namespace = new Namespace(Namespace.DEFAULT_BUCKET_TYPE, bucketName.toString());
        BucketProperties props = fetchBucketProps(namespace);
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
    public void testSetDefaultBucketProps() throws InterruptedException, ExecutionException
    {
        Namespace namespace = new Namespace(Namespace.DEFAULT_BUCKET_TYPE, bucketName.toString());
        StoreBucketPropsOperation.Builder builder = 
            new StoreBucketPropsOperation.Builder(namespace)
                .withAllowMulti(true)
                .withNVal(4);
        
        storeBucketProps(builder);
        
        BucketProperties props = fetchBucketProps(namespace);
        
        assertEquals(props.getNVal(), Integer.valueOf(4));
        assertTrue(props.getAllowMulti());
        
    }
    
    @Test
    public void testResetBucketProps() throws InterruptedException, ExecutionException
    {
        Namespace namespace = new Namespace(Namespace.DEFAULT_BUCKET_TYPE, bucketName.toString());
        StoreBucketPropsOperation.Builder builder = 
            new StoreBucketPropsOperation.Builder(namespace)
                .withNVal(4)
                .withR(1);
        
        storeBucketProps(builder);
        BucketProperties props = fetchBucketProps(namespace);
        
        assertEquals(props.getNVal(), Integer.valueOf(4));
        assertEquals(props.getR().getIntValue(), 1);
        
        resetAndEmptyBucket(bucketName);
        
        props = fetchBucketProps(namespace);
        assertEquals(props.getNVal(), Integer.valueOf(3));
        assertEquals(props.getR(), Quorum.quorumQuorum());
        
    }
    
    @Test
    public void testFetchBucketPropsFromType() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(testBucketType);
        Namespace namespace = new Namespace(bucketType, bucketName);
        BucketProperties props = fetchBucketProps(namespace);
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
    public void testSetBucketPropsInType() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(testBucketType);
        Namespace namespace = new Namespace(bucketType, bucketName);
        StoreBucketPropsOperation.Builder builder = 
            new StoreBucketPropsOperation.Builder(namespace)
                .withR(1)
                .withNVal(4);
        
        storeBucketProps(builder);
        BucketProperties props = fetchBucketProps(namespace);
        
        assertEquals(props.getNVal(), Integer.valueOf(4));
        assertEquals(props.getR().getIntValue(), 1);
        
        namespace = new Namespace(Namespace.DEFAULT_BUCKET_TYPE, bucketName.toString());
        props = fetchBucketProps(namespace);
        assertEquals(props.getNVal(), Integer.valueOf(3));
    }
    
    private BucketProperties fetchBucketProps(Namespace namespace) throws InterruptedException, ExecutionException
    {
        FetchBucketPropsOperation.Builder builder = new FetchBucketPropsOperation.Builder(namespace);
        FetchBucketPropsOperation op = builder.build();
        cluster.execute(op);
        return op.get().getBucketProperties();
    }
    
    private void storeBucketProps(StoreBucketPropsOperation.Builder builder) throws InterruptedException, ExecutionException
    {
        StoreBucketPropsOperation op = 
            builder.build();
        cluster.execute(op);
        
        op.get();
    }
    
}
