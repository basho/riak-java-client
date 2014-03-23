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

package com.basho.riak.client.cap;

import java.util.Arrays;
import java.util.List;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class ConflictResolverFactoryTest
{
    @Before
    public void setUp()
    {
        ConflictResolverFactory factory = ConflictResolverFactory.getInstance();
        factory.unregisterConflictResolverForClass(Pojo.class);
    }
    
    @Test
    public void getDefaultResolver() throws UnresolvedConflictException
    {
        ConflictResolverFactory factory = ConflictResolverFactory.getInstance();
        ConflictResolver<Pojo> resolver = factory.getConflictResolverForClass(Pojo.class);
        
        assertTrue(resolver instanceof DefaultResolver);
        
        resolver.resolve(Arrays.asList(new Pojo()));
    }
    
    @Test
    public void registerResolverClass() throws UnresolvedConflictException
    {
        ConflictResolverFactory factory = ConflictResolverFactory.getInstance();
        MyResolver resolver = new MyResolver();
        factory.registerConflictResolverForClass(Pojo.class, resolver);
        
        ConflictResolver<Pojo> resolver2 = factory.getConflictResolverForClass(Pojo.class);
        
        assertTrue(resolver2 instanceof MyResolver);
        assertEquals(resolver, resolver2);
        
    }
    
    
    
    
    public static class Pojo
    {
        public Pojo(){}
        
        String foo;
        int bar;
    }
    
    public static class MyResolver implements ConflictResolver<Pojo>
    {

        @Override
        public Pojo resolve(List<Pojo> objectList) throws UnresolvedConflictException
        {
            return objectList.get(0);
        }
            
    }
    
}
