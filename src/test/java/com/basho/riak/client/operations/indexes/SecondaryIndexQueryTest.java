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

package com.basho.riak.client.operations.indexes;

import com.basho.riak.client.core.operations.SecondaryIndexQueryOperation;
import com.basho.riak.client.query.Namespace;
import com.basho.riak.client.util.BinaryValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import org.junit.Test;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class SecondaryIndexQueryTest
{
    @Test
    public void intIndexQueryBuildsCorrectly()
    {
        Namespace ns = new Namespace("bucket_type", "bucket_name");
        
        IntIndexQuery iiq = 
            new IntIndexQuery.Builder(ns, "test_index", Long.MAX_VALUE)
                .withKeyAndIndex(true)
                .withContinuation(BinaryValue.create("continuation"))
                .withMaxResults(Integer.MAX_VALUE)
                .withPaginationSort(true)
                .withTimeout(Integer.MAX_VALUE)
                .build();
        
        SecondaryIndexQueryOperation.Query query = iiq.createCoreQuery();
        
        assertEquals(ns, query.getNamespace());
        assertEquals("test_index_int", query.getIndexName().toString());
        assertEquals(Long.MAX_VALUE, Long.valueOf(query.getIndexKey().toString()).longValue());
        assertEquals(BinaryValue.create("continuation"), query.getContinuation());
        assertEquals(Integer.MAX_VALUE, query.getMaxResults());
        assertEquals(true, query.isPaginationSort());
        assertEquals(true, query.isReturnKeyAndIndex());
        assertEquals(Integer.MAX_VALUE, query.getTimeout().intValue());
        
        
        iiq = new IntIndexQuery.Builder(ns, "test_index", Long.MIN_VALUE, Long.MAX_VALUE)
                .withKeyAndIndex(true)
                .withContinuation(BinaryValue.create("continuation"))
                .withMaxResults(Integer.MAX_VALUE)
                .withPaginationSort(true)
                .build();
        
        query = iiq.createCoreQuery();
        
        assertEquals(ns, query.getNamespace());
        assertEquals("test_index_int", query.getIndexName().toString());
        assertEquals(Long.MIN_VALUE, Long.valueOf(query.getRangeStart().toString()).longValue());
        assertEquals(Long.MAX_VALUE, Long.valueOf(query.getRangeEnd().toString()).longValue());
        assertEquals(BinaryValue.create("continuation"), query.getContinuation());
        assertEquals(Integer.MAX_VALUE, query.getMaxResults());
        assertEquals(true, query.isPaginationSort());
        assertEquals(true, query.isReturnKeyAndIndex());
        

        // You can't use a term filter with an _int query
        try
        {
            iiq = new IntIndexQuery.Builder(ns, "test_index", Long.MIN_VALUE, Long.MAX_VALUE)
                .withRegexTermFilter("filter")
                .build();
            
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException e)
        {}
    }
    
    @Test 
    public void binIndexQueryBuildsCorrectly()
    {
        Namespace ns = new Namespace("bucket_type", "bucket_name");
        
        BinIndexQuery biq = new BinIndexQuery.Builder(ns, "test_index", "index_key")
            .withKeyAndIndex(true)
            .withMaxResults(Integer.MAX_VALUE)
            .withContinuation(BinaryValue.create("continuation"))
            .withPaginationSort(true)
            .withRegexTermFilter("filter")
            .build();
        
        SecondaryIndexQueryOperation.Query query = biq.createCoreQuery();
        
        assertEquals(ns, query.getNamespace());
        assertEquals("test_index_bin", query.getIndexName().toString());
        assertEquals("index_key", query.getIndexKey().toString());
        assertEquals(BinaryValue.create("continuation"), query.getContinuation());
        assertEquals(Integer.MAX_VALUE, query.getMaxResults());
        assertEquals(true, query.isPaginationSort());
        assertEquals(true, query.isReturnKeyAndIndex());
        
        biq = new BinIndexQuery.Builder(ns, "test_index", "aaa", "zzz")
            .withKeyAndIndex(true)
            .withMaxResults(Integer.MAX_VALUE)
            .withContinuation(BinaryValue.create("continuation"))
            .withPaginationSort(true)
            .withRegexTermFilter("filter")
            .build();
        
        query = biq.createCoreQuery();
        
        assertEquals(ns, query.getNamespace());
        assertEquals("test_index_bin", query.getIndexName().toString());
        assertEquals("aaa", query.getRangeStart().toString());
        assertEquals("zzz", query.getRangeEnd().toString());
        assertEquals(BinaryValue.create("continuation"), query.getContinuation());
        assertEquals(Integer.MAX_VALUE, query.getMaxResults());
        assertEquals(true, query.isPaginationSort());
        assertEquals(true, query.isReturnKeyAndIndex());
        
    }
    
    public void rawIndexQueryBuildsCorrectly()
    {
        Namespace ns = new Namespace("bucket_type", "bucket_name");
        
        BinaryValue indexMatch = BinaryValue.create("match");
        BinaryValue indexStart = BinaryValue.create("start");
        BinaryValue indexEnd = BinaryValue.create("end");
        
        RawIndexQuery riq = 
            new RawIndexQuery.Builder(ns, "test_index", SecondaryIndexQuery.Type._INT, indexMatch)
                .build();
        
        SecondaryIndexQueryOperation.Query query = riq.createCoreQuery();
        
        assertEquals(ns, query.getNamespace());
        assertEquals("test_index_int", query.getIndexName().toString());
        assertEquals(indexMatch, query.getIndexKey());
        
        riq = 
            new RawIndexQuery.Builder(ns, "test_index", SecondaryIndexQuery.Type._INT, 
                                      indexStart, indexEnd)
                .build();
                
        query = riq.createCoreQuery();
        
        assertEquals(ns, query.getNamespace());
        assertEquals("test_index_int", query.getIndexName().toString());
        assertEquals(indexStart, query.getRangeStart());
        assertEquals(indexEnd, query.getRangeEnd());
        
    }
}
