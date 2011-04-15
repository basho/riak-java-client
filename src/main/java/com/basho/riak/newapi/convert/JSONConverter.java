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
package com.basho.riak.newapi.convert;

import static com.basho.riak.newapi.convert.ConversionUtil.getKey;

import java.io.IOException;
import java.io.StringWriter;

import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.basho.riak.newapi.RiakObject;
import com.basho.riak.newapi.bucket.Bucket;
import com.basho.riak.newapi.builders.RiakObjectBuilder;
import com.basho.riak.newapi.cap.VClock;

/**
 * Converts a RiakObject's value to an instance of T. T must have a field
 * annotated with {@link RiakKey}. RiakObject's value *must* be a JSON string.
 * 
 * @author russell
 * 
 */
public class JSONConverter<T> implements Converter<T> {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Class<T> clazz;
    private final Bucket bucket;
    private String defaultKey;

    public JSONConverter(Class<T> clazz, final Bucket bucket) {
        this.clazz = clazz;
        this.bucket = bucket;
    }

    /**
     * @param clazz
     * @param b
     * @param defaultKey
     */
    public JSONConverter(Class<T> clazz, Bucket b, String defaultKey) {
        this(clazz, b);
        this.defaultKey = defaultKey;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.newapi.convert.Converter#fromDomain(java.lang.Object,
     * VClock)
     */
    public RiakObject fromDomain(T domainObject, VClock vclock) throws ConversionException {
        try {
            String key = getKey(domainObject, this.defaultKey);

            if (key == null) {
                throw new NoKeySpecifedException(domainObject);
            }

            final StringWriter sw = new StringWriter();
            objectMapper.writeValue(sw, domainObject);

            return RiakObjectBuilder.newBuilder(bucket, key).withValue(sw.toString()).withVClock(vclock).build();
        } catch (JsonProcessingException e) {
            throw new ConversionException(e);
        } catch (IOException e) {
            throw new ConversionException(e);
        }

    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.basho.riak.newapi.convert.Converter#toDomain(com.basho.riak.newapi
     * .RiakObject)
     */
    public T toDomain(RiakObject riakObject) throws ConversionException {
        if (riakObject == null) {
            return null;
        }

        String json = riakObject.getValue();

        try {
            T domainObject = objectMapper.readValue(json, clazz);
            return domainObject;
        } catch (JsonProcessingException e) {
            throw new ConversionException(e);
        } catch (IOException e) {
            throw new ConversionException(e);
        }
    }

}
