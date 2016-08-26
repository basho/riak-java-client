/*
 * This file is provided to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.basho.riak.client.api.convert;

import com.basho.riak.client.api.annotations.RiakBucketName;
import com.basho.riak.client.api.annotations.RiakBucketType;
import com.basho.riak.client.api.annotations.RiakContentType;
import com.basho.riak.client.api.annotations.RiakKey;
import com.basho.riak.client.api.annotations.RiakLinks;
import com.basho.riak.client.api.annotations.RiakUsermeta;
import com.basho.riak.client.api.annotations.RiakVClock;
import com.basho.riak.client.api.annotations.RiakIndex;
import com.basho.riak.client.api.annotations.RiakLastModified;
import com.basho.riak.client.api.annotations.RiakTombstone;
import com.basho.riak.client.api.annotations.RiakVTag;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.ser.BeanPropertyWriter;
import com.fasterxml.jackson.databind.ser.BeanSerializerModifier;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.List;

/**
 * {@link BeanSerializerModifier} that drops {@link RiakKey} and
 * {@link RiakUsermeta} annotated fields from the the set of fields that Jackson
 * will serialize
 *
 * @author russell
 *
 */
public class RiakBeanSerializerModifier extends BeanSerializerModifier
{

    private static final RiakBeanSerializerModifier INSTANCE = new RiakBeanSerializerModifier();

    /**
     *
     */
    private RiakBeanSerializerModifier()
    {
    }

    public static RiakBeanSerializerModifier getInstance()
    {
        return INSTANCE;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.codehaus.jackson.map.ser.BeanSerializerModifier#changeProperties(
     * org.codehaus.jackson.map.SerializationConfig,
     * org.codehaus.jackson.map.introspect.BasicBeanDescription, java.util.List)
     */
    @Override
    public List<BeanPropertyWriter> changeProperties(SerializationConfig config,
                                                     BeanDescription beanDesc,
                                                     List<BeanPropertyWriter> beanProperties)
    {
        final List<BeanPropertyWriter> keptProperties = new ArrayList<>();
        for (BeanPropertyWriter w : beanProperties)
        {
            if (keepProperty(w))
            {
                keptProperties.add(w);
            }
        }

        return keptProperties;
    }

    static final List<Class<? extends Annotation>> RIAK_ANNOTATIONS = new ArrayList<>();

    static
    {
        RIAK_ANNOTATIONS.add(RiakKey.class);
        RIAK_ANNOTATIONS.add(RiakBucketName.class);
        RIAK_ANNOTATIONS.add(RiakBucketType.class);
        RIAK_ANNOTATIONS.add(RiakContentType.class);
        RIAK_ANNOTATIONS.add(RiakVTag.class);
        RIAK_ANNOTATIONS.add(RiakLastModified.class);
        RIAK_ANNOTATIONS.add(RiakUsermeta.class);
        RIAK_ANNOTATIONS.add(RiakLinks.class);
        RIAK_ANNOTATIONS.add(RiakIndex.class);
        RIAK_ANNOTATIONS.add(RiakVClock.class);
        RIAK_ANNOTATIONS.add(RiakTombstone.class);
    }

    /**
     * Checks if the property has any of the Riak annotations on it or the
     * Jackson JsonProperty annotation.
     *
     * If a Riak annotation is present without the Jackson JsonProperty
     * annotation, this will return false.
     *
     * If a property has been annotated with both the Jackson JsonProperty
     * annotation and a Riak annotation, the Jackson annotation takes precedent
     * and this will return true.
     *
     * @param beanPropertyWriter a {@link BeanPropertyWriter} to check for Riak*
     * annotations
     * @return true if the property is not Riak annotated or is Jackson
     * JsonProperty annotated, false otherwise
     */
    private boolean keepProperty(BeanPropertyWriter beanPropertyWriter)
    {
        if (beanPropertyWriter.getAnnotation(JsonProperty.class) != null)
        {
            return true;
        }
        for (Class<? extends Annotation> annotation : RIAK_ANNOTATIONS)
        {
            if (beanPropertyWriter.getAnnotation(annotation) != null)
            {
                return false;
            }
        }
        return true;
    }
}
