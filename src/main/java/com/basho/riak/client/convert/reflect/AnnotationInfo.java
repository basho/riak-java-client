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
package com.basho.riak.client.convert.reflect;

import static com.basho.riak.client.convert.reflect.ClassUtil.getFieldValue;
import static com.basho.riak.client.convert.reflect.ClassUtil.setFieldValue;
import static com.basho.riak.client.convert.reflect.ClassUtil.getMethodValue;
import static com.basho.riak.client.convert.reflect.ClassUtil.setMethodValue;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.basho.riak.client.RiakLink;
import com.basho.riak.client.cap.BasicVClock;
import com.basho.riak.client.cap.VClock;
import com.basho.riak.client.convert.UsermetaField;
import com.basho.riak.client.convert.UsermetaMethod;
import com.basho.riak.client.query.indexes.RiakIndexes;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.LinkedList;

/**
 * Class that contains the Riak annotated fields for an annotated class
 * 
 * @author russell
 * 
 */
public class AnnotationInfo {

    private final Field riakKeyField;
    private final Method riakKeySetterMethod;
    private final Method riakKeyGetterMethod;
    private final List<UsermetaField> usermetaFields;
    private final List<UsermetaMethod> usermetaMethods;
    private final List<RiakIndexField> indexFields;
    private final List<RiakIndexMethod> indexMethods;
    private final Field riakLinksField;
    private final Method riakLinksGetterMethod;
    private final Method riakLinksSetterMethod;
    private final Field riakVClockField;
    private final Method riakVClockSetterMethod;
    private final Method riakVClockGetterMethod;
    private final Field riakTombstoneField;
    private final Method riakTombstoneSetterMethod;
    private final Method riakTombstoneGetterMethod;

    private AnnotationInfo(Builder builder)
    {
        this.riakKeyField = builder.riakKeyField;
        this.riakKeyGetterMethod = builder.riakKeyGetterMethod;
        this.riakKeySetterMethod = builder.riakKeySetterMethod;
        this.riakLinksField = builder.riakLinksField;
        this.riakLinksGetterMethod = builder.riakLinksGetterMethod;
        this.riakLinksSetterMethod = builder.riakLinksSetterMethod;
        this.riakVClockField = builder.riakVClockField;
        this.riakVClockGetterMethod = builder.riakVClockGetterMethod;
        this.riakVClockSetterMethod = builder.riakVClockSetterMethod;
        this.riakTombstoneField = builder.riakTombstoneField;
        this.riakTombstoneGetterMethod = builder.riakTombstoneGetterMethod;
        this.riakTombstoneSetterMethod = builder.riakTombstoneSetterMethod;
        this.usermetaFields = builder.usermetaFields;
        this.usermetaMethods = builder.usermetaMethods;
        this.indexFields = builder.indexFields;
        this.indexMethods = builder.indexMethods;
    }
    
    /**
     * Returns the key.
     * <p>
     * The @RiakKey annotation allows for any type to be used. this method
     * will call the object's toString() method to return a String.
     * </p>
     * @param obj the domain object
     * @return the String representation of the key
     */
    public <T> String getRiakKey(T obj) {
        
        Object key = null;
        if (riakKeyField != null)
        {
            key = getFieldValue(riakKeyField, obj);
        }
        else if (riakKeyGetterMethod != null)
        {
            key = getMethodValue(riakKeyGetterMethod, obj);
        }
        return key == null ? null : key.toString();
    }

    public <T> void setRiakKey(T obj, String key) {
        if (riakKeyField != null) {
            setFieldValue(riakKeyField, obj, key);
        } else if (riakKeySetterMethod != null) {
            setMethodValue(riakKeySetterMethod, obj, key);
        }
    }

    public <T> VClock getRiakVClock(T obj) {
        
        VClock vclock = null;
        
        // We allow the annotated field to be either an actual VClock, or
        // a byte array.
        
        if (riakVClockField != null) {
            if (riakVClockField.getType().isAssignableFrom(VClock.class)) {
                vclock = (VClock) getFieldValue(riakVClockField, obj);
            } else {
                vclock = new BasicVClock((byte[]) getFieldValue(riakVClockField, obj));
            }
        }
        else if (riakVClockGetterMethod != null)
        {
            if (riakVClockGetterMethod.getReturnType().isArray()) {
                vclock = new BasicVClock((byte[]) getMethodValue(riakVClockGetterMethod, obj));
            } else {
                vclock = (VClock) getMethodValue(riakVClockGetterMethod, obj);
            }
        }
        
        return vclock;
    }
    
    public <T> void setRiakVClock(T obj, VClock vclock) {
            
        // We allow the annotated field to be either an actual VClock, or
        // a byte array. This is enforced in the AnnotationScanner
        
        if (riakVClockField != null) {
        
            if (riakVClockField.getType().isAssignableFrom(VClock.class)) {
                setFieldValue(riakVClockField, obj, vclock);
            } else {
                setFieldValue(riakVClockField, obj, vclock.getBytes());
            }
        } else if (riakVClockSetterMethod != null) {
            Class<?> pType = riakVClockSetterMethod.getParameterTypes()[0];
            if (pType.isArray()) {
                setMethodValue(riakVClockSetterMethod, obj, vclock.getBytes());
            } else {
                setMethodValue(riakVClockSetterMethod, obj, vclock);
            }
        }
    }
    
    public <T> boolean getRiakTombstone(T obj)
    {
        boolean tombstone = false;
        if (riakTombstoneField != null) {
            tombstone = (Boolean)getFieldValue(riakTombstoneField, obj);
        } else if (riakTombstoneGetterMethod != null) {
            tombstone = (Boolean) getMethodValue(riakTombstoneGetterMethod, obj);
        }
        
        return tombstone;
    }
    
    public <T> void setRiakTombstone(T obj, Boolean isDeleted) {
        
        if (riakTombstoneField != null) {
            setFieldValue(riakTombstoneField, obj, isDeleted);
        } else if (riakTombstoneSetterMethod != null) {
            setMethodValue(riakTombstoneSetterMethod, obj, isDeleted);
        }
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" }) public <T> Map<String, String> getUsermetaData(T obj) {
        final Map<String, String> usermetaData = new LinkedHashMap<String, String>();

        for (UsermetaField uf : usermetaFields) {
            Field f = uf.getField();
            if (Map.class.isAssignableFrom(f.getType())) {
                Map<String,String> o = (Map<String,String>)getFieldValue(f, obj);
                if (o != null) {
                    usermetaData.putAll(o);
                }
            } else {
                Object o = getFieldValue(f, obj);
                String val = o == null ? null : o.toString();
                String key = uf.getUsermetaDataKey();
                // null is not a user meta datum
                if(o != null) {
                    usermetaData.put(key, val);
                }
            }
        }
        
        for (UsermetaMethod um : usermetaMethods) {
            Method m = um.getMethod();
            if (Map.class.isAssignableFrom(m.getReturnType())) {
                Map<String,String> o = (Map<String,String>)getMethodValue(m, obj);
                if (o != null) {
                    usermetaData.putAll(o);
                }
            } else if (!m.getReturnType().equals(Void.TYPE)) {
                Object o = getMethodValue(m, obj);
                String val = o == null ? null : o.toString();
                String key = um.getUsermetaDataKey();
                if (o != null)
                {
                    usermetaData.put(key, val);
                }
            }
        }
        
        return usermetaData;
    }

    public <T> void setUsermetaData(final Map<String, String> usermetaData, T obj) {
        // copy as we will modify
        final Map<String, String> localMetaCopy = new HashMap<String, String>(usermetaData);

        // set any individual annotated fields
        Field mapField = null;
        for (UsermetaField uf : usermetaFields) {
            Field f = uf.getField();
            if (Map.class.isAssignableFrom(f.getType())) {
                mapField = f;
            } else {
                if (localMetaCopy.containsKey(uf.getUsermetaDataKey())) {
                    setFieldValue(f, obj, localMetaCopy.get(uf.getUsermetaDataKey()));
                    localMetaCopy.remove(uf.getUsermetaDataKey());
                }
            }
        }

        Method mapSetter = null;
        for (UsermetaMethod um : usermetaMethods) {
            Method m = um.getMethod();
            if (m.getReturnType().equals(Void.TYPE)) {
                if (um.getUsermetaDataKey().isEmpty()) {
                    mapSetter = m;
                } else {
                    if (localMetaCopy.containsKey(um.getUsermetaDataKey())) {
                        setMethodValue(um.getMethod(), obj, localMetaCopy.get(um.getUsermetaDataKey()));
                        localMetaCopy.remove(um.getUsermetaDataKey());
                    }
                }
            }
        }
        
        if (mapSetter != null) {
            setMethodValue(mapSetter, obj, localMetaCopy);
        }
    
        // set a catch all map field
        if(mapField != null) {
            setFieldValue(mapField, obj, localMetaCopy);
        }  
    }

    /**
     * @return a {@link RiakIndexes} made of the values of the RiakIndex
     *         annotated fields and methods. For methods it is expected to be
     *         a Set&lt;Long&gt; or Set&lt;String&gt;
     */
    @SuppressWarnings("unchecked") public <T> RiakIndexes getIndexes(T obj) {
        final RiakIndexes riakIndexes = new RiakIndexes();

        for (RiakIndexField f : indexFields) {
            if (Set.class.isAssignableFrom(f.getType())) {
                final Type t = f.getField().getGenericType();
                if (t instanceof ParameterizedType) {
                    Class genericType = (Class)((ParameterizedType)t).getActualTypeArguments()[0];
                    if (String.class.equals(genericType)) {
                        riakIndexes.addBinSet(f.getIndexName(), (Set<String>)getFieldValue(f.getField(), obj)); 
                    } else if (Long.class.equals(genericType)) {                        
                        riakIndexes.addIntSet(f.getIndexName(), (Set<Long>)getFieldValue(f.getField(), obj));
                    } else if (Integer.class.equals(genericType)) {
                        // Supporting Integer as legacy. All new code should use Long
                        Set<Integer> iSet = (Set<Integer>) getFieldValue(f.getField(), obj);
                        Set<Long> lSet = new HashSet<Long>();
                        for (Integer i : iSet) {
                            lSet.add(i.longValue());
                        }
                        riakIndexes.addIntSet(f.getIndexName(), lSet);
                    }
                }
            } else {
                final Object val = getFieldValue(f.getField(), obj);
                // null is not an index value
                if (val != null) {
                    if (val instanceof String) {
                        riakIndexes.add(f.getIndexName(), (String) val);
                    } else if (val instanceof Long)  {
                        riakIndexes.add(f.getIndexName(), (Long) val);
                    } else if (val instanceof Integer) {
                        // Supporting int / Integer for legacy. New code should use long / Long
                        riakIndexes.add(f.getIndexName(), ((Integer) val).longValue());
                    }
                }
            }
        }

        for (RiakIndexMethod m : indexMethods) {
            if (Set.class.isAssignableFrom(m.getType())) {
                final Type t = m.getMethod().getGenericReturnType();
                if (t instanceof ParameterizedType) {
                    final Object val = getMethodValue(m.getMethod(), obj);
                    if (val != null) {
                        final Class<?> genericType = (Class<?>) ((ParameterizedType) t).getActualTypeArguments()[0];
                        if (String.class.equals(genericType)) {
                            riakIndexes.addBinSet(m.getIndexName(), (Set<String>) val);
                        } else if (Long.class.equals(genericType)) {
                            riakIndexes.addIntSet(m.getIndexName(), (Set<Long>) val);
                        } else if (Integer.class.equals(genericType)) {
                            // Supporting Integer as legacy. All new code should use Long
                            Set<Integer> iSet = (Set<Integer>) val;
                            Set<Long> lSet = new HashSet<Long>();
                            for (Integer i : iSet) {
                                lSet.add(i.longValue());
                            }
                            riakIndexes.addIntSet(m.getIndexName(), lSet);
                        }
                    }
                }
            } else if (!m.getType().equals(Void.TYPE)) {
                final Object val = getMethodValue(m.getMethod(), obj);
                // null is not an index value
                if (val != null) {
                    if (val instanceof String) {
                        riakIndexes.add(m.getIndexName(), (String) val);
                    } else if (val instanceof Long) {
                        riakIndexes.add(m.getIndexName(), (Long) val);
                    } else if (val instanceof Integer) {
                        riakIndexes.add(m.getIndexName(), ((Integer) val).longValue());
                    }
                }
            }
        }
        
        return riakIndexes;
    }

    /**
     * @param <T>
     * @param indexes
     *            the RiakIndexes to copy to the domain object
     * @param obj
     *            the domain object to set indexes on
     */
    public <T> void setIndexes(RiakIndexes indexes, T obj) {
        // copy the index values to the correct fields
        for (RiakIndexField f : indexFields) {
            Set<?> val = null;
            
            if (Set.class.isAssignableFrom(f.getType())) {
                final Type t = f.getField().getGenericType();
                if (t instanceof ParameterizedType) {
                    final Class<?> genericType = (Class<?>)((ParameterizedType)t).getActualTypeArguments()[0];
                    if (String.class.equals(genericType)) {
                        val = indexes.getBinIndex(f.getIndexName());
                    } else if (Integer.class.equals(genericType)) {
                        // Support Integer / int for legacy. New code should use Long / long
                        Set<Long> lSet = indexes.getIntIndex(f.getIndexName());
                        Set<Integer> iSet = new HashSet<Integer>();
                        for (Long l : lSet ) {
                            iSet.add(l.intValue());
                        }
                        val = iSet;
                    } else if (Long.class.equals(genericType)) {
                        val = indexes.getIntIndex(f.getIndexName());
                    }
                    
                }
                if (val != null && !val.isEmpty()) {
                    setFieldValue(f.getField(), obj, val); 
                }
            } else {
                if (Integer.class.equals(f.getType()) || int.class.equals(f.getType())) {
                    // Support Integer / int for legacy. New code should use Long / long
                    Set<Long> lSet = indexes.getIntIndex(f.getIndexName());
                    Set<Integer> iSet = new HashSet<Integer>();
                    for (Long l : lSet ) {
                        iSet.add(l.intValue());
                    }
                    val = iSet;
                } else if (String.class.equals(f.getType())) {
                    val = indexes.getBinIndex(f.getIndexName());
                } else if (Long.class.equals(f.getType()) || long.class.equals(f.getType())) {
                    val = indexes.getIntIndex(f.getIndexName());
                } 
            
                if (val != null && !val.isEmpty()) {
                    setFieldValue(f.getField(), obj, val.iterator().next()); // take the first value
                }
            }
        }
        
        for (RiakIndexMethod rim : indexMethods) {
            Set<?> val = null;
            Method m = rim.getMethod();
            if (m.getReturnType().equals(Void.TYPE)) {
                Type[] genericParameterTypes = m.getGenericParameterTypes();
                final Class<?> genericType = (Class<?>)((ParameterizedType)genericParameterTypes[0]).getActualTypeArguments()[0];
                if (String.class.equals(genericType)) {
                    val = indexes.getBinIndex(rim.getIndexName());
                } else if (Integer.class.equals(genericType)) {
                    // Support Integer / int for legacy. New code should use Long / long
                    Set<Long> lSet = indexes.getIntIndex(rim.getIndexName());
                    Set<Integer> iSet = new HashSet<Integer>();
                    for (Long l : lSet ) {
                        iSet.add(l.intValue());
                    }
                    val = iSet;
                } else if (Long.class.equals(genericType)) {
                    val = indexes.getIntIndex(rim.getIndexName());
                }
                
                if (val != null && !val.isEmpty()) {
                    setMethodValue(m, obj, val);
                }
            }
        }
    }

    @SuppressWarnings("unchecked") public <T> Collection<RiakLink> getLinks(T obj) {
        final Collection<RiakLink> links = new ArrayList<RiakLink>();
        Object o = null;
        if (riakLinksField != null) {
            o = getFieldValue(riakLinksField, obj);
        } else if (riakLinksGetterMethod != null) {
            o = getMethodValue(riakLinksGetterMethod, obj);
        }
        
        if (o != null && o instanceof Collection) {
            links.addAll((Collection<RiakLink>) o);
        }
        
        return links;
    }

    public <T> void setLinks(Collection<RiakLink> links, T obj) {
        if (riakLinksField != null) {
            setFieldValue(riakLinksField, obj, links);
        } else if (riakLinksSetterMethod != null) {
            setMethodValue(riakLinksSetterMethod, obj, links);
        }
    }
    
    public static class Builder {
        
        private Field riakKeyField;
        private Method riakKeySetterMethod;
        private Method riakKeyGetterMethod;
        private Field riakLinksField;
        private Method riakLinksGetterMethod;
        private Method riakLinksSetterMethod;
        private Field riakVClockField;
        private Method riakVClockSetterMethod;
        private Method riakVClockGetterMethod;
        private Field riakTombstoneField;
        private Method riakTombstoneSetterMethod;
        private Method riakTombstoneGetterMethod;
        private final List<UsermetaField> usermetaFields;
        private final List<UsermetaMethod> usermetaMethods;
        private final List<RiakIndexField> indexFields;
        private final List<RiakIndexMethod> indexMethods;
        
        
        /**
         * Constructs a builder for a new AnnotationInfo
         */
        public Builder() {}
        {
            usermetaFields = new LinkedList<UsermetaField>();
            usermetaMethods = new LinkedList<UsermetaMethod>();
            indexFields = new LinkedList<RiakIndexField>();
            indexMethods = new LinkedList<RiakIndexMethod>();
        }
        
        /**
         * Set the @RiakKey annotated field.
         * @param f the annotated field
         * @return a reference to this object
         */
        public Builder withRiakKeyField(Field f) 
        {
            this.riakKeyField = ClassUtil.checkAndFixAccess(f);
            return this;
        }
        
        /**
         * Set the @RiakKey annotated getter.
         * @param m the annotated method
         * @return a reference to this object
         */
        public Builder withRiakKeyGetter(Method m)
        {
            this.riakKeyGetterMethod = ClassUtil.checkAndFixAccess(m);
            return this;
        }
        
        /**
         * Set the @RiakKey annotated setter.
         * @param m the annotated setter
         * @return a reference to this object
         */
        public Builder withRiakKeySetter(Method m)
        {
            this.riakKeySetterMethod = ClassUtil.checkAndFixAccess(m);
            return this;
        }
        
        /**
         * Set the @RiakLinks annotated field.
         * @param f the annotated field
         * @return a reference to this object
         */
        public Builder withRiakLinksField(Field f)
        {
            validateRiakLinksField(f);
            this.riakLinksField = ClassUtil.checkAndFixAccess(f);
            return this;
        }
        
        /**
         * Set the @RiakLinks annotated getter.
         * @param m the annotated method
         * @return a reference to this object
         */
        public Builder withRiakLinksGetter(Method m)
        {
            validateRiakLinksMethod(m);
            this.riakLinksGetterMethod = ClassUtil.checkAndFixAccess(m);
            return this;
        }
        
        /**
         * Set the @RiakLinks annotated setter.
         * @param m the annotated method
         * @return a reference to this object
         **/
        public Builder withRiakLinksSetter(Method m)
        {
            validateRiakLinksMethod(m);
            this.riakLinksSetterMethod = ClassUtil.checkAndFixAccess(m);
            return this;
        }
      
        /**
         * Add a @RiakUsermeta annotated field.
         * @param f the annotated field.
         * @return a reference to this object.
         */
        public Builder addRiakUsermetaField(Field f)
        {
            this.usermetaFields.add(new UsermetaField(ClassUtil.checkAndFixAccess(f)));
            return this;
        }
        
        /**
         * Add a @RiakUsermeta annotated method
         * @param m the annotated method
         * @return a reference to this object
         */
        public Builder addRiakUsermetaMethod(Method m)
        {
            this.usermetaMethods.add(new UsermetaMethod(ClassUtil.checkAndFixAccess(m)));
            return this;
        }
        
        /**
         * Add a @RiakIndex annotated method.
         * @param m the annotated method
         * @return a reference to this object
         */
        public Builder addRiakIndexMethod(Method m)
        {
            this.indexMethods.add(new RiakIndexMethod(ClassUtil.checkAndFixAccess(m)));
            return this;
        }
        
        /**
         * Add a @RiakIndex annotated field.
         * @param f the annotated field
         * @return a reference to this object
         */
        public Builder addRiakIndexField(Field f)
        {
            this.indexFields.add(new RiakIndexField(ClassUtil.checkAndFixAccess(f)));
            return this;
        }
        
        /**
         * Set the @RiakVClock annotated field.
         * @param f the annotated field
         * @return a reference to this object
         */
        public Builder withRiakVClockField(Field f)
        {
            validateVClockField(f);
            this.riakVClockField = ClassUtil.checkAndFixAccess(f);
            return this;
        }
        
        /**
         * Set the @RiakVClock annotated setter method.
         * @param m the annotated method.
         * @return a reference to this object
         */
        public Builder withRiakVClockSetter(Method m)
        {
            validateVClockMethod(m);
            this.riakVClockSetterMethod = ClassUtil.checkAndFixAccess(m);
            return this;
        }
        
        /**
         * Set the @RiakVClock annotated getter method.
         * @param m the annotated method
         * @return a reference to this object.
         */
        public Builder withRiakVClockGetter(Method m)
        {
            validateVClockMethod(m);
            this.riakVClockGetterMethod = ClassUtil.checkAndFixAccess(m);
            return this;
        }
        
        public Builder withRiakTombstoneField(Field f)
        {
            validateTombstoneField(f);
            this.riakTombstoneField = ClassUtil.checkAndFixAccess(f);
            return this;
        }
        
        public Builder withRiakTombstoneSetterMethod(Method m)
        {
            validateTombstoneMethod(m);
            this.riakTombstoneSetterMethod = ClassUtil.checkAndFixAccess(m);
            return this;
        }
        
        public Builder withRiakTombstoneGetterMethod(Method m)
        {
            validateTombstoneMethod(m);
            this.riakTombstoneGetterMethod = ClassUtil.checkAndFixAccess(m);
            return this;
        }
        
        public AnnotationInfo build()
        {
            validateAnnotatedSet(riakVClockField, riakVClockGetterMethod, 
                                 riakVClockSetterMethod, "@RiakVClock");
            validateAnnotatedSet(riakTombstoneField, riakTombstoneGetterMethod,
                                 riakTombstoneSetterMethod, "@RiakTombstone");
            validateAnnotatedSet(riakLinksField, riakLinksGetterMethod,
                                 riakLinksSetterMethod, "@RiakLinks");
            validateAnnotatedSet(riakKeyField, riakKeyGetterMethod,
                                 riakKeySetterMethod, "@RiakKey");
            
            return new AnnotationInfo(this);
        }
        
        private void validateAnnotatedSet(Field f, Method getter, Method setter, String annotation)
        {
            if (f == null && (getter == null || setter == null))
            {
                if (getter != null && setter == null)
                {
                    throw new IllegalStateException("Getter present for " + annotation + " without setter.");
                }
                else if (setter != null && getter == null)
                {
                    throw new IllegalStateException("Setter present for " + annotation + " without getter.");
                }
            }
        }
        
        private void validateRiakLinksField(Field riakLinksField) 
        {
            
            if (riakLinksGetterMethod != null || riakLinksSetterMethod != null)
            {
                throw new IllegalArgumentException("@RiakLinks annotated method already set");
            }
            
            Type t = riakLinksField.getGenericType();
            if (t instanceof ParameterizedType)
            {
                ParameterizedType type = (ParameterizedType) t;
                if (type.getRawType().equals(Collection.class)) {

                    Type[] genericParams = type.getActualTypeArguments();
                    if (genericParams.length == 1 && genericParams[0].equals(RiakLink.class)) {
                        return;
                    }
                }
            }
            throw new IllegalArgumentException("@RiakLinks field must be Collection<RiakLink>");
        }
        
        private void validateRiakLinksMethod(Method m)
        {
            if (riakLinksField != null)
            {
                throw new IllegalArgumentException("@RiakLinks annotated field already set.");
            }
            if (m.getReturnType().equals(Void.TYPE))
            {
                // it's a setter, check the arg type
                Type[] genericParameterTypes = m.getGenericParameterTypes();
                Type t = genericParameterTypes[0];
                if (t instanceof ParameterizedType)
                {
                    ParameterizedType pType = (ParameterizedType)t;
                    if (pType.getRawType().equals(Collection.class))
                    {
                        Class<?> genericType = (Class<?>)pType.getActualTypeArguments()[0];
                        if (RiakLink.class.equals(genericType))
                        {
                            return;
                        }
                    }
                }
                
                throw new IllegalArgumentException("@RiakLinks setter must take Collection<RiakLink>");
            }
            else
            {
                // it's a getter, check return type
                Type t = m.getGenericReturnType();
                if (t instanceof ParameterizedType)
                {
                    ParameterizedType pType = (ParameterizedType)t;
                    if (pType.getRawType().equals(Collection.class))
                    {
                        Class<?> genericType = (Class<?>)pType.getActualTypeArguments()[0];
                        if (RiakLink.class.equals(genericType))
                        {
                            return;
                        }
                    }
                }
                throw new IllegalArgumentException("@RiakLinks getter must return Collection<RiakLink>");
                
            }
        }
        
        private void validateVClockField(Field f)
        {
            if (riakVClockGetterMethod != null || riakVClockSetterMethod != null)
            {
                throw new IllegalArgumentException("@RiakVClock annotated method already set.");
            }
            else if ( !(f.getType().isArray() && f.getType().getComponentType().equals(byte.class)) &&
                 !f.getType().isAssignableFrom(VClock.class)) 
            {
                throw new IllegalArgumentException("@RiakVClock field must be a VClock or byte[]");
            }
        }
        
        private void validateVClockMethod(Method m)
        {
            if (riakVClockField != null)
            {
                throw new IllegalArgumentException("@RiakVClock annotated field already set.");
            }
            else if (m.getReturnType().equals(Void.TYPE))
            {
                // It's a setter
                Class<?> pType = m.getParameterTypes()[0];
                if ( !(pType.isArray() && pType.getComponentType().equals(byte.class)) &&
                 !pType.isAssignableFrom(VClock.class)) 
                {
                   throw new IllegalArgumentException("@RiakVClock setter must take VClock or byte[]");
                }
            }
            else
            {
                Class<?> rType = m.getReturnType();
                if ( !(rType.isArray() && rType.getComponentType().equals(byte.class)) &&
                 !rType.isAssignableFrom(VClock.class)) 
                {
                   throw new IllegalArgumentException("@RiakVClock getter must return VClock or byte[]");
                }
            }
                
        }
        
        private void validateTombstoneField(Field f)
        {
            if (riakTombstoneGetterMethod != null || riakTombstoneSetterMethod != null)
            {
                throw new IllegalArgumentException("@RiakTombstone annotated method already set.");
            }
            else if (!f.getType().equals(Boolean.class) && !f.getType().equals(boolean.class))
            {
                throw new IllegalArgumentException("@RiakTombstone field must be Boolean or boolean");
            }
        }
        
        private void validateTombstoneMethod(Method m)
        {
            if (riakTombstoneField != null)
            {
                throw new IllegalArgumentException("@RiakTombstone annotated field already set");
            }
            else if (m.getReturnType().equals(Void.TYPE))
            {
                Class<?> pType = m.getParameterTypes()[0];
                if (!pType.equals(Boolean.class) && !pType.equals(boolean.class))
                {
                    throw new IllegalArgumentException("@RiakTombstone setter must take boolean or Boolean");
                }
            }
            else
            {
                Class<?> rType = m.getReturnType();
                if (!rType.equals(Boolean.class) && !rType.equals(boolean.class))
                {
                    throw new IllegalArgumentException("@RiakTombstone getter must return boolean or Boolean");
                }
            }
        }
        
        
    }
}
