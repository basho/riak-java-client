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
package com.basho.riak.client.http;

import static org.junit.Assert.*;

import org.junit.Test;

import com.basho.riak.client.http.RiakLink;

public class TestRiakLink {

    @Test public void constructor_args_persisted() {
        RiakLink link = new RiakLink("bucket", "key", "tag");
        assertEquals("bucket", link.getBucket());
        assertEquals("key", link.getKey());
        assertEquals("tag", link.getTag());
    }
    
    @Test public void equals_handles_null_values() {
        RiakLink link1 = new RiakLink("bucket", "key", "tag");
        RiakLink link2 = new RiakLink(null, null, null);
        assertFalse(link1.equals(link2));
        assertFalse(link2.equals(link1));
    }
    
    @Test public void hashCode_handles_null_values() {
        RiakLink link1 = new RiakLink("bucket", "key", "tag");
        RiakLink link2 = new RiakLink(null, null, null);
        assertFalse(link1.hashCode() == link2.hashCode());
    }
    
    @Test public void equals_performs_equality_check_on_fields() {
        RiakLink link1 = new RiakLink("bucket", "key", "tag");
        RiakLink link2 = new RiakLink("bucket", "key", "tag");
        RiakLink link3 = new RiakLink("bucket", "key", "different");
        assertTrue(link1.equals(link2));
        assertFalse(link1.equals(link3));
    }
    
    @Test public void hashCode_calculates_on_fields() {
        RiakLink link1 = new RiakLink("bucket", "key", "tag");
        RiakLink link2 = new RiakLink("bucket", "key", "tag");
        RiakLink link3 = new RiakLink("bucket", "key", "different");
        
        assertEquals(link1.hashCode(), link2.hashCode());
        assertFalse(link1.hashCode() == link3.hashCode());
    }
    
    @Test public void string_correct() {
        RiakLink link1 = new RiakLink("bucket1", "key2", "tag3");
        
        assertEquals("[bucket1,key2,tag3]", link1.toString());
    }
    
    @Test public void string_correct_with_nulls() {
        RiakLink link1 = new RiakLink(null, null, null);
        
        assertEquals("[null,null,null]", link1.toString());
    }
}
