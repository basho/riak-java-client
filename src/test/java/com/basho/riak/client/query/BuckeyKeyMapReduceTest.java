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
package com.basho.riak.client.query;

import static org.junit.Assert.fail;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.basho.riak.client.RiakException;
import com.basho.riak.client.query.functions.NamedErlangFunction;
import com.basho.riak.client.raw.RawClient;

/**
 * @author russell
 * 
 */
public class BuckeyKeyMapReduceTest {

    @Mock private RawClient client;

    /**
     * @throws java.lang.Exception
     */
    @Before public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    /**
     * Test method for {@link com.basho.riak.client.query.MapReduce#execute()}.
     */
    @Test public void executeInvalid() throws RiakException {
        BucketKeyMapReduce mr = new BucketKeyMapReduce(client);

        try {
            mr.execute();
            fail("expected NoPhasesException");
        } catch (NoPhasesException e) {
            // NO-OP
        }

        mr.addReducePhase(NamedErlangFunction.REDUCE_IDENTITY);
        try {
            mr.execute();
            fail("expected NoInputsException");
        } catch (NoInputsException e) {
            // NO-OP
        }

        mr.addInput("b", "k");

        mr.execute();
    }
}
