/*
 * Copyright 2013 Basho Technologies Inc
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
package com.basho.riak.client.query.indexes;

import junit.framework.Assert;
import org.junit.Test;

public class IndexTypeTest
{

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidIndexName()
    {

        IndexType.typeFromFullname("notavaildname");

    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidIndexTypeExtension()
    {

        IndexType.typeFromFullname("notavalidname_notavalidextension");

    }

    @Test
    public void correctSuffixes()
    {
        Assert.assertEquals(IndexType.BIN.suffix(), "_bin");
        Assert.assertEquals(IndexType.INT.suffix(), "_int");
    }
    
    @Test
    public void testValidIndexTypeExtensions() {

        IndexType indexType = IndexType.typeFromFullname("indexname_int");
        Assert.assertTrue(indexType.equals(IndexType.INT));

        IndexType indexType1 = IndexType.typeFromFullname("indexname_bin");
        Assert.assertTrue(indexType1.equals(IndexType.BIN));

    }
}
