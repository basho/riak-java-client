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
import com.basho.riak.client.core.query.search.YokozunaIndex;
import com.basho.riak.client.core.query.search.YokozunaSchema;
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
public class ITestYzAdminOperations extends ITestAutoCleanupBase
{

    public static final String indexName = "test_index_ITestYzAdminOperations";
    public static final String otherIndexName = "test_index5_ITestYzAdminOperations";

    // This is asSet to ignore as I've found a problem with this schema being accepted as
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
            + "<field name=\"_version_\" type=\"long\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>"
            + "<field name=\"_yz_id\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\" required=\"true\"/>"
            + "<field name=\"_yz_ed\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>" 
            + "<field name=\"_yz_pn\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>"
            + "<field name=\"_yz_fpn\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>"
            + "<field name=\"_yz_vtag\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>" 
            + "<field name=\"_yz_rk\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>"
            + "<field name=\"_yz_rb\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>"
            + "</fields>"
            + "<uniqueKey>_yz_id</uniqueKey>"
            + "<types>"
            + "<fieldType name=\"_yz_str\" class=\"solr.StrField\" sortMissingLast=\"true\" />"
            + "<fieldType name=\"long\" class=\"solr.TrieLongField\" precisionStep=\"0\" positionIncrementGap=\"0\"/>"
            + "</types>"
            + "</schema>");
        
        
        YzPutSchemaOperation putOp = new YzPutSchemaOperation.Builder(yzSchema).build();
        cluster.execute(putOp);
        putOp.get();
        
        YzGetSchemaOperation getOp =
            new YzGetSchemaOperation.Builder("test_schema").build();
        
        cluster.execute(getOp);
        YokozunaSchema yzSchema2 = getOp.get().getSchema();
        
        assertEquals(yzSchema.getContent(), yzSchema2.getContent());
        
        
    }
    
    @Test
    public void fetchAndStoreDefaultSchema() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(testYokozuna);
        YzGetSchemaOperation getOp =
            new YzGetSchemaOperation.Builder("_yz_default").build();
        
        cluster.execute(getOp);
        YokozunaSchema yzSchema = getOp.get().getSchema();
        
        assertNotNull(yzSchema.getName());
        assertNotNull(yzSchema.getContent());
        
        YzPutSchemaOperation putOp = new YzPutSchemaOperation.Builder(yzSchema).build();
        cluster.execute(putOp);
        putOp.get();
        
    }
    
    @Test
    public void testStoreAndFetchIndex() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(testYokozuna);
        YokozunaIndex index = new YokozunaIndex(indexName).withNVal(2);
        YzPutIndexOperation putOp = new YzPutIndexOperation.Builder(index).build();
        
        cluster.execute(putOp);
        putOp.get();
        
        assertTrue("Index not created", assureIndexExists(indexName));
        
        
        YzFetchIndexOperation fetchOp = 
            new YzFetchIndexOperation.Builder().withIndexName(indexName)
                .build();
        
        cluster.execute(fetchOp);
        fetchOp.await();
        if (!fetchOp.isSuccess())
        {
            assertTrue(fetchOp.cause().toString(), fetchOp.isSuccess());
        }
        
        List<YokozunaIndex> indexList = fetchOp.get().getIndexes();
        
        assertFalse(indexList.isEmpty());
        index = indexList.get(0);
        assertEquals(index.getSchema(), "_yz_default");
        assertEquals((Integer)2, index.getNVal());
        
        YzDeleteIndexOperation delOp = new YzDeleteIndexOperation.Builder(indexName).build();
        cluster.execute(delOp);
        delOp.await();
        assertTrue(delOp.isSuccess());
    }
    
    @Test
    public void testDeleteIndex() throws InterruptedException, ExecutionException
    {
        Assume.assumeTrue(testYokozuna);
        YokozunaIndex index = new YokozunaIndex(otherIndexName);
        YzPutIndexOperation putOp = new YzPutIndexOperation.Builder(index).build();
        
        cluster.execute(putOp);
        putOp.get();
        
        assertTrue("Index not created", assureIndexExists(otherIndexName));
        
        YzDeleteIndexOperation delOp = 
            new YzDeleteIndexOperation.Builder(otherIndexName).build();
        cluster.execute(delOp);
        delOp.get();
        
    }
    
}
