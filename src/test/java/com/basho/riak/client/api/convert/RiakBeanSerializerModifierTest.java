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
package com.basho.riak.client.api.convert;

import com.basho.riak.client.api.convert.RiakJacksonModule;
import com.basho.riak.client.api.annotations.RiakKey;
import com.basho.riak.client.api.annotations.RiakUsermeta;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

/**
 * @author russell
 * 
 */
@SuppressWarnings("unchecked")
public class RiakBeanSerializerModifierTest
{

    @Test public void changePropertiesDropsRiakAnnotatedProperties() throws Exception
    {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new RiakJacksonModule());

        RiakAnnotatedClass rac = new RiakAnnotatedClass();

        String json = mapper.writeValueAsString(rac);

        @SuppressWarnings("unchecked") Map<String, String> map = mapper.readValue(json, Map.class);

        assertEquals(2, map.size());
        assertTrue(map.containsKey("someField"));
        assertTrue(map.containsKey("someOtherField"));
        
        RiakAnnotatedClassWithPublicFields racwpf = new RiakAnnotatedClassWithPublicFields();

        json = mapper.writeValueAsString(racwpf);

        map = mapper.readValue(json, Map.class);

        assertEquals(2, map.size());
        assertTrue(map.containsKey("someField"));
        assertTrue(map.containsKey("someOtherField"));
		
        // Test the combination of a Riak annotation and the Jackson @JsonProperty
        RiakAnnotatedClassWithJsonProp racwjp = new RiakAnnotatedClassWithJsonProp();
        json = mapper.writeValueAsString(racwjp);

        map = mapper.readValue(json, Map.class);

        assertEquals(4, map.size());
        assertTrue(map.containsKey("keyField"));
        assertTrue(map.containsKey("metaValueField"));
        assertTrue(map.containsKey("someField"));
        assertTrue(map.containsKey("someOtherField"));
		
		
    }

    @SuppressWarnings("unused") private static final class RiakAnnotatedClass
    {
        @RiakKey private String keyField = "key";
        @RiakUsermeta(key = "metaKey1") private String metaValueField = "ONE";
        @RiakUsermeta private Map<String, String> usermeta = new HashMap<String, String>();

        private String someField = "TWO";
        private String someOtherField = "THREE";

        public String getSomeField()
        {
            return someField;
        }

        public void setSomeField(String someField)
        {
            this.someField = someField;
        }

        public String getSomeOtherField()
        {
            return someOtherField;
        }

        public void setSomeOtherField(String someOtherField)
        {
            this.someOtherField = someOtherField;
        }

        public String getKeyField()
        {
            return keyField;
        }

        public String getMetaValueField()
        {
            return metaValueField;
        }
    }

    @SuppressWarnings("unused") private static final class RiakAnnotatedClassWithPublicFields
    {
        @RiakKey public String keyField = "key";
        @RiakUsermeta(key = "metaKey1") public String metaValueField = "ONE";
        @RiakUsermeta public Map<String, String> usermeta = new HashMap<String, String>();

        public String someField = "TWO";
        public String someOtherField = "THREE";
    }

	@SuppressWarnings("unused") private static final class RiakAnnotatedClassWithJsonProp
    {
        @JsonProperty
		@RiakKey private String keyField = "key";
        @JsonProperty
		@RiakUsermeta(key = "metaKey1") public String metaValueField = "ONE";
        @RiakUsermeta public Map<String, String> usermeta = new HashMap<String, String>();

        public String someField = "TWO";
        public String someOtherField = "THREE";

        public String getKeyField()
        {
            return keyField;
        }

    }
	
}
