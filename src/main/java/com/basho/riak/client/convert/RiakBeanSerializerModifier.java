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

import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Field;
import java.util.LinkedList;
import java.util.List;

import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.map.introspect.AnnotatedField;
import org.codehaus.jackson.map.introspect.AnnotatedMember;
import org.codehaus.jackson.map.introspect.BasicBeanDescription;
import org.codehaus.jackson.map.ser.BeanPropertyWriter;
import org.codehaus.jackson.map.ser.BeanSerializerModifier;

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
                                                               BasicBeanDescription beanDesc,
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

        AnnotatedMember member = beanPropertyWriter.getMember();
        if (member instanceof AnnotatedField) {
            AnnotatedElement element = member.getAnnotated();
            key = element.getAnnotation(RiakKey.class);
            usermeta = element.getAnnotation(RiakUsermeta.class);
            links = element.getAnnotation(RiakLinks.class);
            index = element.getAnnotation(RiakIndex.class);
            vclock = element.getAnnotation(RiakVClock.class);
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
            return key == null && usermeta == null && links == null && vclock == null && index == null;
        }
    }
}
