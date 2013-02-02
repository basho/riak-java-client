/*
 * Copyright 2013 Brian Roach <roach at basho dot com>.
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
package com.basho.riak.client.itest;

import static com.basho.riak.client.AllTests.emptyBucket;
import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakRetryFailedException;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.cap.ConflictResolver;
import com.basho.riak.client.convert.RiakTombstone;
import java.util.Collection;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public abstract class ITestTombstone
{
    protected IRiakClient client;
    protected String bucketName;

    @Before public void setUp() throws RiakException, InterruptedException 
    {
        client = getClient();
        bucketName = this.getClass().getName();
        emptyBucket(bucketName, client);
    }

    protected abstract IRiakClient getClient() throws RiakException;
    
    
    @Test public void singleWithoutReturnDeletedVClock() throws RiakRetryFailedException, RiakException
    {
        Bucket b = client.fetchBucket(bucketName).execute();
        
        // store something, delete it, fetch it
        b.store("key", "value").execute();
        b.delete("key").execute();
        IRiakObject o = b.fetch("key").execute();
        
        assertNull(o);
        
    }
    
    @Test public void singleWithReturnDeletedVClock() throws RiakRetryFailedException, RiakException
    {
        Bucket b = client.fetchBucket(bucketName).execute();
        
        // store something, delete it, fetch it
        b.store("key", "value").execute();
        b.delete("key").execute();
        IRiakObject o = b.fetch("key").returnDeletedVClock(true).execute();
        
        assertNotNull(o);
        assertTrue(o.isDeleted());
    }
    
    @Test public void multiWithoutReturnDeletedVClock() throws RiakRetryFailedException, RiakException
    {
        String bucket2 = bucketName + "_2";
        Bucket b = client.createBucket(bucket2).allowSiblings(true).execute();
        
        // This will create a value with a tombstone sibling
        b.store("key", "value").execute();
        b.delete("key").execute();
        b.store("key", "value").withoutFetch().execute();
        
        IRiakObject o = b.fetch("key").execute();
        
        assertNotNull(o);
        assertFalse(o.isDeleted());
        assertEquals(o.getValueAsString(), "value");
        
        emptyBucket(bucket2, client);
    }
    
    @Test public void multiWithReturnDeletedVclock() throws RiakRetryFailedException, RiakException
    {
        String bucket2 = bucketName + "_3";
        Bucket b = client.createBucket(bucket2).allowSiblings(true).execute();
        
        // This will create a value with a tombstone sibling
        b.store("key", "value").execute();
        b.delete("key").execute();
        b.store("key", "value").withoutFetch().execute();
        
        IRiakObject o = b.fetch("key").returnDeletedVClock(true).withResolver(new PlainTombstoneResolver()).execute();
        
        emptyBucket(bucket2, client);
        
    }
    
    private class PlainTombstoneResolver implements ConflictResolver<IRiakObject>
    {
        private boolean foundTombstone = false;
        
        public IRiakObject resolve(Collection<IRiakObject> siblings)
        {
            assertTrue(siblings.size() > 1);
            for (IRiakObject o : siblings)
            {
                if (o.isDeleted())
                {
                    foundTombstone = true;
                }
            }
            
            assertTrue(foundTombstone);
            return siblings.iterator().next();
            
        }
        
    }
    
    @Test public void singleWithAnnotatedPojo() throws RiakRetryFailedException, RiakException
    {
        String bucket2 = bucketName + "_4";
        Bucket b = client.createBucket(bucket2).allowSiblings(true).execute();
    
        AnnotatedPojo pojo = new AnnotatedPojo();
        pojo.value = "Some Value";
        
        b.store("key", pojo).execute();
        b.delete("key").execute();
        pojo = b.fetch("key", AnnotatedPojo.class).returnDeletedVClock(true).execute();
        
        assertTrue(pojo.isTombstone);
        assertNull(pojo.value);
        
        emptyBucket(bucketName, client);
    
    }
    
    @Test public void multiWithAnnotatedPojo() throws RiakRetryFailedException, RiakException, InterruptedException
    {
        String bucket2 = bucketName + "_5";
        Bucket b = client.createBucket(bucket2).allowSiblings(true).execute();
        
        AnnotatedPojo pojo = new AnnotatedPojo();
        pojo.value = "Some Value";
        
        b.store("key", pojo).execute();
        b.delete("key").execute();
        b.store("key", pojo).withoutFetch().execute();
        pojo = b.fetch("key", AnnotatedPojo.class).withResolver(new PojoTombstoneResolver()).returnDeletedVClock(true).execute();
        
        emptyBucket(bucketName, client);
        
    }
    
    
    public static class AnnotatedPojo
    {
        @RiakTombstone public boolean isTombstone = false;
        public String value;

    }
    
    private class PojoTombstoneResolver implements ConflictResolver<AnnotatedPojo>
    {
        private boolean foundTombstone = false;
        
        public AnnotatedPojo resolve(Collection<AnnotatedPojo> siblings)
        {
            assertTrue(siblings.size() > 1);
            for (AnnotatedPojo ap : siblings)
            {
                if (ap.isTombstone)
                {
                    foundTombstone = true;
                }
            }
            
            assertTrue(foundTombstone);
            return siblings.iterator().next();
            
        }
        
    }
    
}
