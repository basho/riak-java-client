/*
 * Copyright 2014 Basho Technologies Inc.
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

package com.basho.riak.client.operations.itest;

import com.basho.riak.client.core.operations.itest.ITestBase;
import com.basho.riak.client.operations.RiakClient;
import com.basho.riak.client.operations.StoreOption;
import com.basho.riak.client.operations.UpdateValue;
import com.basho.riak.client.operations.UpdateValue.Update;
import com.basho.riak.client.query.Location;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.concurrent.ExecutionException;
import static org.junit.Assert.*;
import org.junit.Test;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class ITestUpdateValue extends ITestBase
{
    @Test
    public void simpleTest() throws ExecutionException, InterruptedException
    {
        RiakClient client = new RiakClient(cluster);
        
        Location loc = new Location(bucketName).setKey("test_update_key1");
        UpdateValue uv = new UpdateValue.Builder(loc)
                            .withStoreOption(StoreOption.RETURN_BODY, true)
                            .withUpdate(new UpdatePojo())
                            .build();
        
        UpdateValue.Response resp = client.execute(uv);
        Pojo pojo = resp.getValue(Pojo.class);
        assertEquals(1, pojo.value);
        
        resp = client.execute(uv);
        pojo = resp.getValue(Pojo.class);
        
        assertEquals(2, pojo.value);
        
    }
    
    public static class UpdatePojo extends Update<Pojo>
    {

        @Override
        public Pojo apply(Pojo original)
        {
            if (original == null)
            {
                original = new Pojo();
            }
            
            original.value++;
            return original;
        }
        
    }
    
    public static class Pojo
    {
        @JsonProperty
        int value;
    }
}
