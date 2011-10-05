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

import static com.basho.riak.client.convert.KeyUtil.getKey;
import static com.basho.riak.client.util.CharsetUtils.asString;
import static com.basho.riak.client.util.CharsetUtils.getCharset;

import java.io.IOException;
import java.util.Map;

import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakLink;
import com.basho.riak.client.builders.RiakObjectBuilder;
import com.basho.riak.client.cap.VClock;
import com.basho.riak.client.http.util.Constants;

/**
 * Converts a RiakObject's value to an instance of T. T must have a field
 * annotated with {@link RiakKey} or you must construct the converter with a key to use. RiakObject's value *must* be a JSON string.
 * 
 * <p>
 * At present user meta data and {@link RiakLink}s are not converted. This means
 * they are essentially lost in translation.
 * </p>
 * 
 * @author russell
 * 
 */
public class JSONConverter<T> implements Converter<T> {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Class<T> clazz;
    private final String bucket;
    private final UsermetaConverter<T> usermetaConverter;
    private String defaultKey;

    /**
     * Create a JSONConverter for creating instances of <code>clazz</code> from
     * JSON and instances of {@link IRiakObject} with a JSON paylaod from
     * instances of <code>clazz</code>
     * 
     * @param clazz the type to convert to/from
     * @param b the bucket
     */
    public JSONConverter(Class<T> clazz, String bucket) {
        this(clazz, bucket, null);
    }

    /**
     * Create a JSONConverter for creating instances of <code>clazz</code> from
     * JSON and instances of {@link IRiakObject} with a JSON paylaod from
     * instances of <code>clazz</code>
     * 
     * @param clazz the type to convert to/from
     * @param bucket the bucket
     * @param defaultKey
     *            for cases where <code>clazz</code> does not have a
     *            {@link RiakKey} annotated field, pass the key to use in this
     *            conversion.
     */
    public JSONConverter(Class<T> clazz, String bucket, String defaultKey) {
        this.clazz = clazz;
        this.bucket = bucket;
        this.defaultKey = defaultKey;
        this.usermetaConverter = new UsermetaConverter<T>();
        objectMapper.registerModule(new RiakJacksonModule());
    }

    /**
     * Converts <code>domainObject</code> to a JSON string and sets that as the
     * payload of a {@link IRiakObject}. Also set the <code>content-type</code>
     * to <code>application/json;charset=UTF-8</code>
     * 
     * @param domainObject
     *            to be converted
     * @param vclock
     *            the vector clock from Riak
     */
    public IRiakObject fromDomain(T domainObject, VClock vclock) throws ConversionException {
        try {
            String key = getKey(domainObject, this.defaultKey);

            if (key == null) {
                throw new NoKeySpecifedException(domainObject);
            }

            final byte[] value = objectMapper.writeValueAsBytes(domainObject);
            Map<String, String> usermetaData = usermetaConverter.getUsermetaData(domainObject);
            return RiakObjectBuilder.newBuilder(bucket, key)
                .withValue(value)
                .withVClock(vclock)
                .withUsermeta(usermetaData).
                withContentType(Constants.CTYPE_JSON_UTF8)
                .build();
        } catch (JsonProcessingException e) {
            throw new ConversionException(e);
        } catch (IOException e) {
            throw new ConversionException(e);
        }

    }

    /**
     * Converts the <code>value</code> of <code>riakObject</code> to an instance
     * of <code>T</code>.
     * <p>
     * Beware: at present links and user meta are not converted at present: this is on the way.
     * </p>
     * @param riakObject
     *            the {@link IRiakObject} to convert to instance of
     *            <code>T</code>. NOTE: <code>riakObject.getValue()</code> must be a
     *            JSON string. The charset from
     *            <code>riakObject.getContentType()</code> is used.
     */
    public T toDomain(IRiakObject riakObject) throws ConversionException {
        if (riakObject == null) {
            return null;
        }

        String json = asString(riakObject.getValue(), getCharset(riakObject.getContentType()));

        try {
            T domainObject = objectMapper.readValue(json, clazz);
            KeyUtil.setKey(domainObject, riakObject.getKey());
            usermetaConverter.populateUsermeta(riakObject.getMeta(), domainObject);
            return domainObject;
        } catch (JsonProcessingException e) {
            throw new ConversionException(e);
        } catch (IOException e) {
            throw new ConversionException(e);
        }
    }
}
