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

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakLink;
import com.basho.riak.client.cap.BasicVClock;
import com.basho.riak.client.cap.VClock;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class JsonConverterTest
{
    private final VClock vclock = new BasicVClock(new byte[0]);
    private final ObjectMapper mapper = new ObjectMapper();
    
    @Test
    public void testRiakAnnotatedtFieldsExcludedFromJson() throws IOException
    {
        
        PojoWithRiakFields pojo = new PojoWithRiakFields();
        JSONConverter<PojoWithRiakFields> jc = 
            new JSONConverter<PojoWithRiakFields>(PojoWithRiakFields.class,"some_bucket");
        IRiakObject o = jc.fromDomain(pojo, vclock);
        
        String json = o.getValueAsString();
        assertFalse(fieldExistsInJson(json,"key"));
        assertFalse(fieldExistsInJson(json,"metadata"));
        assertFalse(fieldExistsInJson(json,"index"));
        assertFalse(fieldExistsInJson(json,"links"));
        assertFalse(fieldExistsInJson(json,"vclock"));
        assertFalse(fieldExistsInJson(json,"tombstone"));

        
        
    }
    
    @Test
    public void testRiakAnnotatedtFieldsIncludedInJson() throws IOException
    {
        
        PojoWithRiakFieldsIncluded pojo = new PojoWithRiakFieldsIncluded();
        JSONConverter<PojoWithRiakFieldsIncluded> jc = 
            new JSONConverter<PojoWithRiakFieldsIncluded>(PojoWithRiakFieldsIncluded.class,"some_bucket");
        IRiakObject o = jc.fromDomain(pojo, vclock);
        
        String json = o.getValueAsString();
        assertTrue(fieldExistsInJson(json,"key"));
        assertTrue(fieldExistsInJson(json,"metadata"));
        assertTrue(fieldExistsInJson(json,"index"));
        assertTrue(fieldExistsInJson(json,"links"));
        assertTrue(fieldExistsInJson(json,"vclock"));
        assertTrue(fieldExistsInJson(json,"tombstone"));

        
    }
    
    @Test
    public void testRiakAnnotatedMethodsExcludedFromJson() throws IOException
    {
        PojoWithRiakMethods pojo = new PojoWithRiakMethods();
        JSONConverter<PojoWithRiakMethods> jc = 
            new JSONConverter<PojoWithRiakMethods>(PojoWithRiakMethods.class,"some_bucket");
        IRiakObject o = jc.fromDomain(pojo, vclock);
        
        String json = o.getValueAsString();
        assertFalse(fieldExistsInJson(json,"key"));
        assertFalse(fieldExistsInJson(json,"usermeta"));
        assertFalse(fieldExistsInJson(json,"index"));
        assertFalse(fieldExistsInJson(json,"links"));
        assertFalse(fieldExistsInJson(json,"vclock"));
        assertFalse(fieldExistsInJson(json,"tombstone"));


    }
    
    @Test
    public void testRiakAnnotatedMethodsIncludedInJson() throws IOException
    {
        PojoWithRiakMethodsIncluded pojo = new PojoWithRiakMethodsIncluded();
        JSONConverter<PojoWithRiakMethodsIncluded> jc = 
            new JSONConverter<PojoWithRiakMethodsIncluded>(PojoWithRiakMethodsIncluded.class,"some_bucket");
        IRiakObject o = jc.fromDomain(pojo, vclock);
        
        String json = o.getValueAsString();
        assertTrue(fieldExistsInJson(json,"key"));
        assertTrue(fieldExistsInJson(json,"usermeta"));
        assertTrue(fieldExistsInJson(json,"index"));
        assertTrue(fieldExistsInJson(json,"links"));
        assertTrue(fieldExistsInJson(json,"vclock"));
        assertTrue(fieldExistsInJson(json,"tombstone"));

    }
    
    
    private boolean fieldExistsInJson(String json, String fieldname) throws IOException
    {
        JsonNode node = mapper.readTree(json);
        return node.has(fieldname);
    }
    
}

class Pojo
{
    public Pojo() {}
    @JsonProperty
    String value = "some_value";
}

class PojoWithRiakFields extends Pojo
{
    @RiakKey
    String key = "some_key";
    
    @RiakUsermeta
    Map<String,String> metadata = 
        new HashMap<String,String>(){{ put("metaKey", "metaValue");}};
    
    @RiakIndex(name="email")
    Set<String> index = new HashSet<String>(){{ add("bob@gmail.com");}};
    
    @RiakLinks
    Collection<RiakLink> links = new LinkedList<RiakLink>(){{ add(new RiakLink("bucket","key","tag"));}};
    
    @RiakVClock
    VClock vclock;
    
    @RiakTombstone
    boolean tombstone;
}

class PojoWithRiakFieldsIncluded extends Pojo
{
    @JsonProperty
    @RiakKey
    String key = "some_key";
    
    @JsonProperty
    @RiakUsermeta
    Map<String,String> metadata = 
        new HashMap<String,String>(){{ put("metaKey", "metaValue");}};
    
    @JsonProperty
    @RiakIndex(name="email")
    Set<String> index = new HashSet<String>(){{ add("bob@gmail.com");}};
    
    @JsonProperty
    @RiakLinks
    Collection<RiakLink> links = 
        new LinkedList<RiakLink>(){{ add(new RiakLink("bucket","key","tag"));}};
    
    @JsonProperty
    @RiakVClock
    VClock vclock;
    
    @JsonProperty
    @RiakTombstone
    boolean tombstone;
    
}

class PojoWithRiakMethods extends Pojo
{
    protected String key = "some_key";
    protected Map<String,String> metadata = 
        new HashMap<String,String>(){{ put("metaKey", "metaValue");}};
    protected Set<String> index = new HashSet<String>(){{ add("bob@gmail.com");}};
    protected Collection<RiakLink> links = 
        new LinkedList<RiakLink>(){{ add(new RiakLink("bucket","key","tag"));}};
    protected VClock vclock;
    protected boolean tombstone;
}
 
class PojoWithRiakMethodsExcluded extends PojoWithRiakMethods
{
    @RiakKey
    public void setKey(String key)
    {
        this.key = key;
    }
    
    @RiakKey
    public String getKey()
    {
        return this.key;
    }
    
    @RiakUsermeta
    public Map<String,String> getUsermeta()
    {
        return this.metadata;
    }
    
    @RiakUsermeta
    public void setUsermeta(Map<String,String> usermeta)
    {
        this.metadata = usermeta;
    }
    
    @RiakIndex(name="email")
    public void setIndex(Set<String> index)
    {
        this.index = index;
    }
    
    @RiakIndex(name="email")
    public Set<String> getIndex()
    {
        return this.index;
    }
    
    @RiakLinks
    public Collection<RiakLink> getLinks()
    {
        return this.links;
    }
    
    @RiakLinks
    public void setLinks(Collection<RiakLink> links)
    {
        this.links = links;
    }
    
    @RiakVClock
    public void setVClock(VClock vclock)
    {
        this.vclock = vclock;
    }
    
    @RiakVClock 
    public VClock getVClock()
    {
        return this.vclock;
    }
    
    @RiakTombstone
    public boolean getTombstone()
    {
        return this.tombstone;
    }
    
    @RiakTombstone
    public void setTombstone(boolean tombstone)
    {
        this.tombstone = tombstone;
    }
    
}

class PojoWithRiakMethodsIncluded extends PojoWithRiakMethods
{
    
    @JsonProperty
    @RiakKey
    public void setKey(String key)
    {
        this.key = key;
    }
    
    @JsonProperty
    @RiakKey
    public String getKey()
    {
        return this.key;
    }
    
    @JsonProperty
    @RiakUsermeta
    public Map<String,String> getUsermeta()
    {
        return this.metadata;
    }

    @JsonProperty
    @RiakUsermeta
    public void setUsermeta(Map<String,String> usermeta)
    {
        this.metadata = usermeta;
    }
    
    @JsonProperty
    @RiakIndex(name="email")
    public void setIndex(Set<String> index)
    {
        this.index = index;
    }
    
    @JsonProperty
    @RiakIndex(name="email")
    public Set<String> getIndex()
    {
        return this.index;
    }

    @JsonProperty
    @RiakLinks
    public Collection<RiakLink> getLinks()
    {
        return this.links;
    }
    
    @JsonProperty
    @RiakLinks
    public void setLinks(Collection<RiakLink> links)
    {
        this.links = links;
    }
    
    @JsonProperty
    @RiakVClock
    public void setVClock(VClock vclock)
    {
        this.vclock = vclock;
    }
    
    @JsonProperty
    @RiakVClock 
    public VClock getVClock()
    {
        return this.vclock;
    }
    
    @JsonProperty
    @RiakTombstone
    public boolean getTombstone()
    {
        return this.tombstone;
    }
    
    @JsonProperty
    @RiakTombstone
    public void setTombstone(boolean tombstone)
    {
        this.tombstone = tombstone;
    }
    
}