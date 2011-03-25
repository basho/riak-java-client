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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.megacorp.kv.exceptions.BailException;
import com.megacorp.kv.exceptions.MyCheckedBusinessException;

/**
 * @author russell
 * 
 */
public class BasicOperations {

    @Test public void basicOpertaions() throws Exception {
        final RiakClient c = RiakFactory.defaultClient();
        
        c.createBucket("testBucket").retry(2).nval(3).execute();

        Bucket b = c.fetchBucket("bucket").retry(1).fetchKeys(false).fetchProperties(true).execute();
        
        assertEquals(3, b.getNVal());
        assertEquals("bucket", b.getName());
        
        b = c.updateBucket(b).r(CAP.QUORUM).w(CAP.ALL).dw(CAP.ONE).rw(2)
                                .nval(5)
                                    .allowSiblings(true)
                                        .chashKeyFunction(new NamedErlangFunction("keys", "hash"))
                           .execute();
        
        assertEquals(5, b.getNVal());
        assertTrue(b.isAllowSiblings());
        assertEquals(2, b.getRW());

        // most simple store
        b.store("k", "v").execute();

        // most simple fetch
        RiakObject o = b.fetch("k").execute();
        
        assertEquals("v", o.getValue());

        try {
            b.fetch("k").r(1).withResolver(new DoNothingResolver()).execute();
            fail("Expected UnresolvedConflictException");
        } catch (UnresolvedConflictException e) {
            assertEquals("meh", e.getReason());
            throw new MyCheckedBusinessException(e);
        } catch(RiakRetryFailedException e) {
            throw new BailException(e);
        }

        // update value
        o = b.store(o)
                .w(3).dw(1)
                    .returnBody(true)
                        .retry(3)
                            .withMutator(new ClobberMutator("new value"))
                                .withResolver(new TakeTheFirst())
              .execute();
        
        
        assertEquals("new value", o.getValue());
        
        o = b.fetch(o).execute();
        
        //with default mutator
        b.store(o).withValue("new value").execute();
        
        b.delete(o).rw(3).retry(2).execute();
        
        o = b.fetch(o).execute();
        
        assertNull(o);
    }
}
