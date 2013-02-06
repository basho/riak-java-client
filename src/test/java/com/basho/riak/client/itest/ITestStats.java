/*
 * Copyright 2012 roach.
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

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.query.NodeStats;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import java.io.IOException;
import java.math.BigInteger;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

/**
 *
 * @author roach
 */

@Ignore
public abstract class ITestStats
{
    protected IRiakClient client;
    
    @Before public void setUp() throws RiakException
    {
        this.client = getClient();
    }
    
    protected abstract IRiakClient getClient() throws RiakException;
    
    @Test public void testDeserializer() throws IOException
    {
        NodeStats.UndefinedStatDeserializer usd = new NodeStats.UndefinedStatDeserializer();
        SimpleModule module = new SimpleModule("UndefinedStatDeserializer", 
                                                       new Version(1,0,0,null,null,null));
        module.addDeserializer(BigInteger.class, usd);
    
        String json = "{\"vnode_gets\":\"deprecated\",\"vnode_gets_total\":12345678}";
        
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(module);
        NodeStats stats = mapper.readValue(json, NodeStats.class);
        assertEquals(stats.vnodeGets(), BigInteger.ZERO);
        assertEquals(stats.vnodeGetsTotal(), BigInteger.valueOf(12345678));
    
    }
    
}
