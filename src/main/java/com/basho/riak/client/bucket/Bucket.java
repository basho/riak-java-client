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
package com.basho.riak.client.bucket;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.convert.RiakKey;
import com.basho.riak.client.operations.DeleteObject;
import com.basho.riak.client.operations.FetchObject;
import com.basho.riak.client.operations.RiakOperation;
import com.basho.riak.client.operations.StoreObject;
import com.basho.riak.client.query.indexes.FetchIndex;
import com.basho.riak.client.query.indexes.RiakIndex;

/**
 * The primary interface for working with Key/Value data in Riak, a factory for key/value {@link RiakOperation}s.
 * <p>
 * Provides convenience methods for creating {@link RiakOperation}s for storing
 * <code>byte[]</code> and <code>String</code> data in Riak. Also provides
 * methods for creating {@link RiakOperation}s for storing Java Bean style POJOs
 * in Riak. A Bucket is a factory for {@link RiakOperation}s on Key/Value
 * data.
 * </p>
 * <p>
 * Gives access to all the {@link BucketProperties} that the underlying API
 * transport exposes. NOTE: soon this will be *all* the {@link BucketProperties}
 * </p>
 * <p>
 * Provides access to an {@link Iterable<String>} for the keys in the bucket.
 * </p>
 * 
 * @see StoreObject
 * @see FetchObject
 * @see DeleteObject
 * 
 * @author russell
 */
public interface Bucket extends BucketProperties {

    /**
     * Get this Buckets name.
     * @return the name of the bucket
     */
    String getName();

    /**
     * Creates a {@link StoreObject} that will store a new {@link IRiakObject}.
     * 
     * @param key the key to store the data under.
     * @param value the data as a byte[]
     * @return a {@link StoreObject}
     * @see StoreObject
     */
    StoreObject<IRiakObject> store(String key, byte[] value);

    /**
     * Creates a {@link StoreObject} that will store a new {@link IRiakObject}.
     * 
     * @param key the key to store the data under.
     * @param value the data as a string
     * @return a {@link StoreObject}
     * @see StoreObject
     */
    StoreObject<IRiakObject> store(String key, String value);

    /**
     * Creates a {@link StoreObject} for storing <code>o</code> of type
     * <code>T</code> on <code>execute()</code>. <code>T</code> must have
     * a field annotated with {@link RiakKey}.
     * 
     * @param <T> the Type of <code>o</code>
     * @param o the data to store
     * @return a {@link StoreObject}
     * @see StoreObject
     */
    <T> StoreObject<T> store(T o);

    /**
     * Creates a {@link StoreObject} for storing <code>o</code> of type
     * <code>T</code> at <code>key</code> on <code>execute()</code>.
     * 
     * @param <T> the Type of <code>o</code>
     * @param o the data to store
     * @param key the key
     * @return a {@link StoreObject}
     * @see StoreObject
     */
    <T> StoreObject<T> store(String key, T o);

    /**
     * Creates a {@link FetchObject} that returns the data at <code>key</code>
     * as an {@link IRiakObject} on <code>execute()</code>.
     * 
     * @param key the key
     * @return a {@link FetchObject}
     * @see FetchObject
     */
    FetchObject<IRiakObject> fetch(String key);

    /**
     * Creates a {@link FetchObject} operation that returns the data at
     * <code>key</code> as an instance of type <code>T</code> on
     * <code>execute()</code>.
     * 
     * @param <T>
     *            the Type to return
     * @param key
     *            the key under which the data is stored
     * @param type
     *            the Class of the type to return
     * @return a {@link FetchObject}
     * @see FetchObject
     */
    <T> FetchObject<T> fetch(String key, Class<T> type);

    /**
     * Creates a {@link FetchObject} operation that returns the data at
     * <code>o</code>'s annotated {@link RiakKey} field as an instance of type
     * <code>T</code> on <code>execute()</code>.
     * 
     * @param <T>
     *            the Type to return
     * @param o
     *            an instance ot <code>T</code> that has the key annotated with
     *            {@link RiakKey}
     * @return a {@link FetchObject}
     * @see FetchObject
     */
    <T> FetchObject<T> fetch(T o);

    /**
     * Creates a {@link DeleteObject} operation that will delete the data at
     * <code>o</code>'s {@link RiakKey} annotated field value on
     * <code>execute()</code>.
     * 
     * @param <T>
     *            the Type of <code>o</code>
     * @param o
     *            an instance of <code>T</code> with a value for the key in the
     *            field annotated by {@link RiakKey}
     * @return a {@link DeleteObject}
     * @see DeleteObject
     */
    <T> DeleteObject delete(T o);

    /**
     * Creates a {@link DeleteObject} operation that will delete the data at
     * <code>key</code> on <code>execute()</code>.
     * 
     * @param <T>
     *            the Type of <code>o</code>
     * @param o
     *            an instance of <code>T</code> with a value for the key in the
     *            field annotated by {@link RiakKey}
     * @return a {@link DeleteObject}
     * @see DeleteObject
     */
    DeleteObject delete(String key);

    /**
     * An {@link Iterable} view of the keys stored in this bucket.
     * @return an {@link Iterable} of Strings.
     * @throws RiakException
     */
    Iterable<String> keys() throws RiakException;

    /**
     * Creates a {@link FetchIndex} operation for the given index name and type
     * 
     * @param <T>
     *            the index type (currently String or Long)
     * @param index
     *            an index
     * @return a {@link FetchIndex} operation for further configuration and
     *         execution
     */
    <T> FetchIndex<T> fetchIndex(RiakIndex<T> index);
}
