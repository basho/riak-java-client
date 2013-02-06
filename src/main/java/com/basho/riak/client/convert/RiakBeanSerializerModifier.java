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
package com.basho.riak.client.convert;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.introspect.AnnotatedField;
import com.fasterxml.jackson.databind.introspect.AnnotatedMember;
import com.fasterxml.jackson.databind.ser.BeanPropertyWriter;
import com.fasterxml.jackson.databind.ser.BeanSerializerModifier;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Field;
import java.util.LinkedList;
import java.util.List;



/**
 * {@link BeanSerializerModifier} that drops {@link RiakKey} and
 * {@link RiakUsermeta} annotated fields from the the set of fields that Jackson
 * will serialize
 * 
 * @author russell
 * 
 */
public class RiakBeanSerializerModifier extends BeanSerializerModifier {

    private static final RiakBeanSerializerModifier INSTANCE = new RiakBeanSerializerModifier();

    /**
     * 
     */
    private RiakBeanSerializerModifier() {}

    public static RiakBeanSerializerModifier getInstance() {
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
    @Override public List<BeanPropertyWriter> changeProperties(SerializationConfig config,
                                                               BeanDescription beanDesc,
                                                               List<BeanPropertyWriter> beanProperties) {

        List<BeanPropertyWriter> keptProperties = new LinkedList<BeanPropertyWriter>();

        for (BeanPropertyWriter w : beanProperties) {
            if (keepProperty(w)) {
                keptProperties.add(w);
            }
        }
        return keptProperties;
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
     * @param beanPropertyWriter
     *            a {@link BeanPropertyWriter} to check for Riak* annotations
     * @return true if the property is not Riak annotated or is Jackson
	 * JsonProperty annotated, false otherwise
     */
    private boolean keepProperty(BeanPropertyWriter beanPropertyWriter) {
        RiakKey key = null;
        RiakUsermeta usermeta = null;
        RiakLinks links = null;
        RiakIndex index = null;
        RiakVClock vclock = null;
		JsonProperty jacksonJsonProperty = null;
        RiakTombstone tombstone = null;

        AnnotatedMember member = beanPropertyWriter.getMember();
        if (member instanceof AnnotatedField) {
            AnnotatedElement element = member.getAnnotated();
            key = element.getAnnotation(RiakKey.class);
            usermeta = element.getAnnotation(RiakUsermeta.class);
            links = element.getAnnotation(RiakLinks.class);
            index = element.getAnnotation(RiakIndex.class);
            vclock = element.getAnnotation(RiakVClock.class);
            tombstone = element.getAnnotation(RiakTombstone.class);
            jacksonJsonProperty = element.getAnnotation(JsonProperty.class);
        } else {
            @SuppressWarnings("rawtypes") Class clazz = member.getDeclaringClass();
            Field field;
            try {
                field = clazz.getDeclaredField(beanPropertyWriter.getName());
                key = field.getAnnotation(RiakKey.class);
                usermeta = field.getAnnotation(RiakUsermeta.class);
                links = field.getAnnotation(RiakLinks.class);
                index = field.getAnnotation(RiakIndex.class);
                vclock = field.getAnnotation(RiakVClock.class);
                tombstone = field.getAnnotation(RiakTombstone.class);
                jacksonJsonProperty = field.getAnnotation(JsonProperty.class);
            } catch (SecurityException e) {
                throw new RuntimeException(e);
            } catch (NoSuchFieldException e) {
                // ignore, not a field means not a Riak annotated field.
            }
        }

        if (jacksonJsonProperty != null) {
            return true;
		} else {
            return key == null && usermeta == null && links == null && vclock == null && index == null && tombstone == null;
        }
    }
}
