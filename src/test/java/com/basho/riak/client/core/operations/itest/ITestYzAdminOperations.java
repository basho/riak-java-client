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

import com.basho.riak.client.core.operations.YzDeleteIndexOperation;
import com.basho.riak.client.core.operations.YzFetchIndexOperation;
import com.basho.riak.client.core.operations.YzGetSchemaOperation;
import com.basho.riak.client.core.operations.YzPutIndexOperation;
import com.basho.riak.client.core.operations.YzPutSchemaOperation;
import com.basho.riak.client.query.search.YokozunaIndex;
import com.basho.riak.client.query.search.YokozunaSchema;
import java.util.List;
import java.util.concurrent.ExecutionException;
import static org.junit.Assert.*;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class ITestYzAdminOperations extends ITestBase
{
    
    // This is set to ignore as I've found a problem with this schema being accepted as 
    // valid but then causing problems later if you try to create an index. Need to talk to Z.
    // After that I can expand the unit tests. As is though, they show the networky 
    // bits work. 
    @Ignore
    @Test
    public void testStoreandFetchSchema() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(testYokozuna);
        YokozunaSchema yzSchema = new YokozunaSchema("test_schema", 
            "<schema name=\"test_schema\" version=\"1.0\">"
            + "<fields>"
            + "<field name=\"_version_\" type=\"long\" indexed=\"true\" stored=\"true\"/>"
            + "<field name=\"_yz_id\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" required=\"true\"/>"
            + "<field name=\"_yz_ed\" type=\"_yz_str\" indexed=\"true\" stored=\"true\"/>" 
            + "<field name=\"_yz_pn\" type=\"_yz_str\" indexed=\"true\" stored=\"true\"/>"
            + "<field name=\"_yz_fpn\" type=\"_yz_str\" indexed=\"true\" stored=\"true\"/>"
            + "<field name=\"_yz_vtag\" type=\"_yz_str\" indexed=\"true\" stored=\"true\"/>" 
            + "<field name=\"_yz_node\" type=\"_yz_str\" indexed=\"true\" stored=\"true\"/>"
            + "<field name=\"_yz_rk\" type=\"_yz_str\" indexed=\"true\" stored=\"true\"/>"
            + "<field name=\"_yz_rb\" type=\"_yz_str\" indexed=\"true\" stored=\"true\"/>"
            + "</fields>"
            + "<uniqueKey>_yz_id</uniqueKey>"
            + "<types>"
            + "<fieldType name=\"_yz_str\" class=\"solr.StrField\" sortMissingLast=\"true\" />"
            + "<fieldType name=\"long\" class=\"solr.TrieLongField\" precisionStep=\"0\" positionIncrementGap=\"0\"/>"
            + "</types>"
            + "</schema>");
        
        
        YzPutSchemaOperation putOp = new YzPutSchemaOperation(yzSchema);
        cluster.execute(putOp);
        putOp.get();
        
        YzGetSchemaOperation getOp =
            new YzGetSchemaOperation("test_schema");
        
        cluster.execute(getOp);
        YokozunaSchema yzSchema2 = getOp.get();
        
        assertEquals(yzSchema.getContent(), yzSchema2.getContent());
        
        
    }
    
    @Test
    public void fetchAndStoreDefaultSchema() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(testYokozuna);
        YzGetSchemaOperation getOp =
            new YzGetSchemaOperation("_yz_default");
        
        cluster.execute(getOp);
        YokozunaSchema yzSchema = getOp.get();
        
        assertNotNull(yzSchema.getName());
        assertNotNull(yzSchema.getContent());
        
        YzPutSchemaOperation putOp = new YzPutSchemaOperation(yzSchema);
        cluster.execute(putOp);
        putOp.get();
        
    }
    
    @Test
    public void testStoreAndFetchIndex() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(testYokozuna);
        YokozunaIndex index = new YokozunaIndex("test_index");
        YzPutIndexOperation putOp = new YzPutIndexOperation(index);
        
        cluster.execute(putOp);
        putOp.get();
        
        // Testing has shown that even though Riak responds to the create op ... 
        // the index isn't actually created yet and the delete op return "not found" 
        
        YzFetchIndexOperation fetchOp = new YzFetchIndexOperation("test_index");
        
        cluster.execute(fetchOp);
        List<YokozunaIndex> indexList = fetchOp.get();
        
        assertFalse(indexList.isEmpty());
        index = indexList.get(0);
        assertEquals(index.getSchema(), "_yz_default");
        
    }
    
    @Test
    public void testDeleteIndex() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(testYokozuna);
        YokozunaIndex index = new YokozunaIndex("test_index5");
        YzPutIndexOperation putOp = new YzPutIndexOperation(index);
        
        cluster.execute(putOp);
        putOp.get();
        
        // Testing has shown that even though Riak responds to the create op ... 
        // the index isn't actually created yet and the delete op return "not found" 
        Thread.sleep(5000);
        
        YzDeleteIndexOperation delOp = new YzDeleteIndexOperation("test_index5");
        cluster.execute(delOp);
        delOp.get();
        
    }
    
}
