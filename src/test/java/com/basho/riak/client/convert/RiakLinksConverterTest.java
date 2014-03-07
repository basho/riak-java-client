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

package com.basho.riak.client.convert;

import com.basho.riak.client.RiakLink;
import java.util.Collection;
import java.util.LinkedList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class RiakLinksConverterTest extends ConversionUtilTest
{
    private final Collection<RiakLink> links = new LinkedList<RiakLink>();
    private final RiakLinksConverter converter = new RiakLinksConverter();
    
    @Before
    public void setUp()
    {
        RiakLink link = new RiakLink("bucket", "key", "tag");
        links.add(link);
        link = new RiakLink("bucket", "key2", "tag");
        links.add(link);
    }
    
    @Test
    public void getRiakLinksField()
    {
        PojoWithAnnotatedFields<Void> pojo = new PojoWithAnnotatedFields<Void>();
        pojo.links = links;
        Collection<RiakLink> actual = converter.getLinks(pojo);
        assertNotNull(actual);
        assertTrue(links.containsAll(actual));
    }
    
    @Test
    public void getRiakLinksMethod()
    {
        PojoWithAnnotatedMethods<Void> pojo = new PojoWithAnnotatedMethods<Void>();
        pojo.setLinks(links);
        Collection<RiakLink> actual = converter.getLinks(pojo);
        assertNotNull(actual);
        assertTrue(links.containsAll(actual));
    }
    
    @Test
    public void setRiakLinksField()
    {
        PojoWithAnnotatedFields<Void> pojo = new PojoWithAnnotatedFields<Void>();
        converter.populateLinks(links, pojo);
        Collection<RiakLink> actual = pojo.links;
        assertNotNull(actual);
        assertTrue(links.containsAll(actual));
    }
    
    @Test
    public void setRiakLinksMethod()
    {
        PojoWithAnnotatedMethods<Void> pojo = new PojoWithAnnotatedMethods<Void>();
        converter.populateLinks(links, pojo);
        Collection<RiakLink> actual = pojo.getLinks();
        assertNotNull(actual);
        assertTrue(links.containsAll(actual));
    }
    
    @Test
    public void illegalRiakLinksFieldType()
    {
        Object o = new Object()
        {
            @RiakLinks
            private final String domainProperty = null;

        };
        
        try
        {
            Collection<RiakLink> actual = converter.getLinks(o);
            fail("Excepted IllegalArgumentException to be thrown");
        }
        catch (RuntimeException e)
        {
            assertEquals(e.getCause().getClass(), IllegalArgumentException.class);
        }
        
        o = new Object()
        {
            @RiakLinks
            private final Collection<String> domainProperty = null;

        };
        
        try
        {
            Collection<RiakLink> actual = converter.getLinks(o);
            fail("Excepted IllegalArgumentException to be thrown");
        }
        catch (RuntimeException e)
        {
            assertEquals(e.getCause().getClass(), IllegalArgumentException.class);
        }
        
    }
    
    @Test 
    public void illegalRiakLinksGetterType()
    {
        Object o = new Object()
        {
            @RiakLinks
            public String getLinks()
            {
                return null;
            }
            
            @RiakLinks
            public void setLinks(Collection<RiakLink> links) {}
            
        };
        
        try
        {
            Collection<RiakLink> actual = converter.getLinks(o);
            fail("Excepted IllegalArgumentException to be thrown");
        }
        catch (RuntimeException e)
        {
            assertEquals(e.getCause().getClass(), IllegalArgumentException.class);
        }
        
        o = new Object()
        {
            @RiakLinks
            public Collection<String> getLinks()
            {
                return null;
            }
            
            @RiakLinks
            public void setLinks(Collection<RiakLink> links) {}
            
        };
        
        try
        {
            Collection<RiakLink> actual = converter.getLinks(o);
            fail("Excepted IllegalArgumentException to be thrown");
        }
        catch (RuntimeException e)
        {
            assertEquals(e.getCause().getClass(), IllegalArgumentException.class);
        }
        
    }
}
