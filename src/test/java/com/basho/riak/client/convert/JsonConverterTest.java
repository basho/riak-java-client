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

package com.basho.riak.client.convert;

import com.basho.riak.client.annotations.*;
import com.basho.riak.client.cap.BasicVClock;
import com.basho.riak.client.cap.VClock;
import com.basho.riak.client.convert.Converter.OrmExtracted;
import com.basho.riak.client.query.RiakObject;
import com.basho.riak.client.query.links.RiakLink;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import static org.junit.Assert.*;
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
            new JSONConverter<PojoWithRiakFields>(new TypeReference<PojoWithRiakFields>(){});
        
        RiakObject o = jc.fromDomain(pojo, null).getRiakObject();
        
        String json = o.getValue().toString();
        assertFalse(fieldExistsInJson(json,"key"));
        assertFalse(fieldExistsInJson(json,"bucketName"));
        assertFalse(fieldExistsInJson(json,"bucketType"));
        assertFalse(fieldExistsInJson(json,"metadata"));
        assertFalse(fieldExistsInJson(json,"index"));
        assertFalse(fieldExistsInJson(json,"links"));
        assertFalse(fieldExistsInJson(json,"vclock"));
        assertFalse(fieldExistsInJson(json,"tombstone"));
        
        assertTrue(fieldExistsInJson(json, "value"));

        
        
    }
    
    @Test
    public void testRiakAnnotatedtFieldsIncludedInJson() throws IOException
    {
        
        PojoWithRiakFieldsIncluded pojo = new PojoWithRiakFieldsIncluded();
        JSONConverter<PojoWithRiakFieldsIncluded> jc = 
            new JSONConverter<PojoWithRiakFieldsIncluded>(new TypeReference<PojoWithRiakFieldsIncluded>(){});
        RiakObject o = jc.fromDomain(pojo, null).getRiakObject();
        
        String json = o.getValue().toString();
        assertTrue(fieldExistsInJson(json,"key"));
        assertTrue(fieldExistsInJson(json,"bucketName"));
        assertTrue(fieldExistsInJson(json,"bucketType"));
        assertTrue(fieldExistsInJson(json,"metadata"));
        assertTrue(fieldExistsInJson(json,"index"));
        assertTrue(fieldExistsInJson(json,"links"));
        assertTrue(fieldExistsInJson(json,"vclock"));
        assertTrue(fieldExistsInJson(json,"tombstone"));

        assertTrue(fieldExistsInJson(json, "value"));

        
    }
    
    @Test
    public void testRiakAnnotatedMethodsExcludedFromJson() throws IOException
    {
        PojoWithRiakMethodsExcluded pojo = new PojoWithRiakMethodsExcluded();
        JSONConverter<PojoWithRiakMethods> jc = 
            new JSONConverter<PojoWithRiakMethods>(new TypeReference<PojoWithRiakMethods>(){});
        
        RiakObject o = jc.fromDomain(pojo, null).getRiakObject();
        
        String json = o.getValue().toString();
        assertFalse(fieldExistsInJson(json,"key"));
        assertFalse(fieldExistsInJson(json,"bucketName"));
        assertFalse(fieldExistsInJson(json,"bucketType"));
        assertFalse(fieldExistsInJson(json,"usermeta"));
        assertFalse(fieldExistsInJson(json,"index"));
        assertFalse(fieldExistsInJson(json,"links"));
        assertFalse(fieldExistsInJson(json,"vclock"));
        assertFalse(fieldExistsInJson(json,"tombstone"));

        assertTrue(fieldExistsInJson(json, "value"));

    }
    
    @Test
    public void testRiakAnnotatedMethodsIncludedInJson() throws IOException
    {
        PojoWithRiakMethodsIncluded pojo = new PojoWithRiakMethodsIncluded();
        JSONConverter<PojoWithRiakMethodsIncluded> jc = 
            new JSONConverter<PojoWithRiakMethodsIncluded>(new TypeReference<PojoWithRiakMethodsIncluded>(){});
        
        RiakObject o = jc.fromDomain(pojo, null).getRiakObject();
        
        String json = o.getValue().toString();
        assertTrue(fieldExistsInJson(json,"key"));
        assertTrue(fieldExistsInJson(json,"bucketName"));
        assertTrue(fieldExistsInJson(json,"bucketType"));
        assertTrue(fieldExistsInJson(json,"usermeta"));
        assertTrue(fieldExistsInJson(json,"index"));
        assertTrue(fieldExistsInJson(json,"links"));
        assertTrue(fieldExistsInJson(json,"vclock"));
        assertTrue(fieldExistsInJson(json,"tombstone"));
        
        assertTrue(fieldExistsInJson(json, "value"));


    }
    
    @Test
    public void convertBackAndForth()
    {
        EmptyPojoWithRiakFields pojo = new EmptyPojoWithRiakFields();
        
        pojo.key = "some_key";
        pojo.bucketName = "some_bucket";
        pojo.bucketType = "some_type";
        pojo.value = "some_value";
        pojo.metadata = 
            new HashMap<String,String>(){{ put("metaKey", "metaValue");}};
        pojo.index = new HashSet<String>(){{ add("bob@gmail.com");}};
        pojo.links = new LinkedList<RiakLink>(){{ add(new RiakLink("bucket","key","tag"));}};
        pojo.vclock = vclock;
        pojo.contentType = RiakObject.DEFAULT_CONTENT_TYPE;
            
        JSONConverter<EmptyPojoWithRiakFields> jc = 
            new JSONConverter<EmptyPojoWithRiakFields>(new TypeReference<EmptyPojoWithRiakFields>(){});
        OrmExtracted orm = jc.fromDomain(pojo, null);
        
        RiakObject riakObject = orm.getRiakObject();
        riakObject.setLastModified(123);
        riakObject.setVTag("vtag");
        
        EmptyPojoWithRiakFields convertedPojo = jc.toDomain(riakObject, orm.getLocation());
        
        assertEquals(pojo.key, convertedPojo.key);
        assertEquals(pojo.bucketName, convertedPojo.bucketName);
        assertEquals(pojo.bucketType, convertedPojo.bucketType);
        assertEquals(pojo.value, convertedPojo.value);
        assertTrue(pojo.index.containsAll(convertedPojo.index));
        
        for (String key : pojo.metadata.keySet())
        {
            assertTrue(convertedPojo.metadata.containsKey(key));
            assertEquals(convertedPojo.metadata.get(key), pojo.metadata.get(key));
        }
        
        assertTrue(convertedPojo.links.containsAll(pojo.links));
        assertEquals(pojo.vclock, convertedPojo.vclock);
        assertEquals(123, convertedPojo.lastModified.longValue());
        assertEquals("vtag", convertedPojo.vtag);
        
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
    public String value = "some_value";
}

class PojoWithRiakFields extends Pojo
{
    @RiakKey
    public String key = "some_key";
    
    @RiakBucketName
    public String bucketName = "some_bucket";
    
    @RiakBucketType
    public String bucketType = "some_type";
        
    @RiakUsermeta
    public Map<String,String> metadata = 
        new HashMap<String,String>(){{ put("metaKey", "metaValue");}};
    
    @RiakIndex(name="email")
    public Set<String> index = new HashSet<String>(){{ add("bob@gmail.com");}};
    
    @RiakLinks
    public Collection<RiakLink> links = new LinkedList<RiakLink>(){{ add(new RiakLink("bucket","key","tag"));}};
    
    @RiakVClock
    public VClock vclock;
    
    @RiakTombstone
    public boolean tombstone;
    
    @RiakContentType
    public String contentType;
    
    @RiakLastModified
    public Long lastModified;
    
    @RiakVTag
    public String vtag;
}

class PojoWithRiakFieldsIncluded extends Pojo
{
    @JsonProperty
    @RiakKey
    String key = "some_key";
    
    @JsonProperty
    @RiakBucketName
    String bucketName = "some_bucket";
    
    @JsonProperty
    @RiakBucketType
    String bucketType = "some_type";
    
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
    protected String bucketName = "some_bucket";
    protected String bucketType = "some_type";
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
    
    @RiakBucketName
    public void setBucketName(String bucketName)
    {
        this.bucketName = bucketName;
    }
    
    @RiakBucketName
    public String getBucketName()
    {
        return bucketName;
    }
    
    @RiakBucketType
    public void setBucketType(String bucketType)
    {
        this.bucketType = bucketType;
    }
    
    @RiakBucketType
    public String getBucketType()
    {
        return bucketType;
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
    @RiakBucketName
    public void setBucketName(String bucketName)
    {
        this.bucketName = bucketName;
    }
    
    @JsonProperty
    @RiakBucketName
    public String getBucketName()
    {
        return bucketName;
    }
    
    @JsonProperty
    @RiakBucketType
    public void setBucketType(String bucketType)
    {
        this.bucketType = bucketType;
    }
    
    @JsonProperty
    @RiakBucketType
    public String getBucketType()
    {
        return bucketType;
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

class EmptyPojoWithRiakFields
{
    @RiakKey
    String key;

    @RiakBucketName
    String bucketName;

    @RiakBucketType
    String bucketType;

    @RiakUsermeta
    Map<String,String> metadata;

    @RiakIndex(name="email")
    Set<String> index;

    @RiakLinks
    Collection<RiakLink> links;

    @RiakVClock
    VClock vclock;

    @RiakTombstone
    boolean tombstone;

    @RiakContentType
    String contentType;

    @RiakLastModified
    Long lastModified;

    @RiakVTag
    String vtag;

    @JsonProperty
    String value;

}