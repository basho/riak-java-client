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
package com.basho.riak.newapi.cap;

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import org.junit.Test;

/**
 * @author russell
 *
 */
public class ClobberMutationTest {

    /**
     * Test method for {@link com.basho.riak.newapi.cap.ClobberMutation#ClobberMutation(java.lang.Object)}.
     */
    @Test public void apply() {
        final Object oldValue = new Object();
        
        ClobberMutation<Object> mutation = new ClobberMutation<Object>(null);
        
        assertNull(mutation.apply(oldValue));
        
        Object newValue = new Object();
        
        assertNotSame(oldValue, newValue);
        mutation = new ClobberMutation<Object>(newValue);
        
        assertSame(newValue, mutation.apply(new Object()));
    }

}
