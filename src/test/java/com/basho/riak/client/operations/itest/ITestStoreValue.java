/*
 * Copyright 2014 Brian Roach <roach at basho dot com>.
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
import com.basho.riak.client.operations.FetchOption;
import com.basho.riak.client.operations.FetchValue;
import com.basho.riak.client.operations.RiakClient;
import com.basho.riak.client.operations.StoreOption;
import com.basho.riak.client.operations.StoreValue;
import com.basho.riak.client.query.Location;
import com.basho.riak.client.query.RiakObject;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.concurrent.ExecutionException;
import static org.junit.Assert.*;
import org.junit.Test;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class ITestStoreValue extends ITestBase
{
    @Test
    public void simpleTest() throws ExecutionException, InterruptedException
    {
        RiakClient client = new RiakClient(cluster);
        
        Location loc = new Location(bucketName).setKey("test_store_key");
        Pojo pojo = new Pojo();
        pojo.value = "test store value";
        StoreValue sv = 
            new StoreValue.Builder(loc, pojo)
                .withOption(StoreOption.RETURN_BODY, true)
                .build();
        
        StoreValue.Response resp = client.execute(sv);
        
        Pojo pojo2 = resp.getValue(Pojo.class);
        
        assertEquals(pojo.value, pojo2.value);
        
        FetchValue fv = new FetchValue.Builder(loc)
                            .withOption(FetchOption.DELETED_VCLOCK, false)
                            .build(); 
        
        FetchValue.Response fResp = client.execute(fv);
        
        assertEquals(pojo.value, fResp.getValue(Pojo.class).value);
        
    }
    
    
    
    public static class Pojo
    {
        @JsonProperty
        String value;
    }
    
}
