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

import com.basho.riak.client.core.operations.FetchBucketPropsOperation;
import com.basho.riak.client.core.operations.StoreBucketPropsOperation;
import com.basho.riak.client.query.BucketProperties;
import java.util.concurrent.ExecutionException;
import static org.junit.Assert.*;
import org.junit.Test;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class ITestBucketProperties extends ITestBase
{
    
    @Test
    public void testFetchDefaultBucketProps() throws InterruptedException, ExecutionException
    {
        FetchBucketPropsOperation op = new FetchBucketPropsOperation(bucketName);
        cluster.execute(op);
        BucketProperties props = op.get();
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
    public void testSetDefaultBucketProps() throws InterruptedException, ExecutionException
    {
        BucketProperties props = 
            new BucketProperties()
                .withAllowMulti(true)
                .withNVal(4);
        
        StoreBucketPropsOperation op = new StoreBucketPropsOperation(bucketName, props);
        cluster.execute(op);
        
        op.get();
        
        FetchBucketPropsOperation fetchOp = new FetchBucketPropsOperation(bucketName);
        cluster.execute(fetchOp);
        props = fetchOp.get();
        
        assertEquals(props.getNVal(), Integer.valueOf(4));
        assertTrue(props.getAllowMulti());
        
    }
    
}
