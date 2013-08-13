/*
 * Copyright 2013 Basho Technologies Inc
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
package com.basho.riak.client.query.indexes;

import com.basho.riak.client.util.ByteArrayWrapper;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Models a Riak Secondary Index (2i).
 *
 * <p> Secondary Indexing (2i) in Riak gives developers the ability, at write
 * time, to tag an object stored in Riak with one or more queryable values.
 * Since the KV data is completely opaque to 2i, the user must tell 2i exactly
 * what attribute to index on and what its index value should be, via key/value
 * metadata. This is different from Search, which parses the data and builds
 * indexes based on a schema. Riak 2i currently requires the LevelDB or Memory
 * backend. 
 * </p> 
 * <p> A {@code RiakIndex} is made up of the index name, a type,
 * then one or more queryable index values. 
 * </p>
 * 
 * @riak.threadsafety This class is designed to be thread safe.
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 * @see <a
 * href="http://docs.basho.com/riak/1.3.0/tutorials/querying/Secondary-Indexes/">Secondary
 * Indexes in Riak</a>
 */
public abstract class RiakIndex<T> implements Iterable<T>
{

    private final RiakIndex.Name indexName;
    private Set<ByteArrayWrapper> values;

    @SuppressWarnings("unchecked")
    protected RiakIndex(Name name)
    {
        this.indexName = name;
        if (name.values != null)
        {
            // Java says this is unchecked even though ... it isn't
            this.values = name.values;
        }
        else
        {
            this.values = Collections.newSetFromMap(new ConcurrentHashMap<ByteArrayWrapper, Boolean>());
        }
    }

    /**
     * Returns the index type from its fully qualified name <p> There are two
     * types of Seconrady Indexes (2i) in Riak; "Integer" and "Binary". The
     * current server API distinguishes between them via a suffix ({@code _int}
     * and {@code _bin} respectively). This method takes a "fully qualified" 2i
     * name (e.g. "my_index_int") and returns an enum that represents the type.
     *
     * @param fullname a "fully qualified" 2i name ending with the suffix "_int"
     * or "_bin"
     * @return the {@link IndexType}
     * @throws IllegalArgumentException if the supplied index name does not have
     * a valid suffix.
     */
    public static IndexType typeFromFullname(String fullname)
    {
        int i = fullname.lastIndexOf('_');
        if (i != -1)
        {
            String suffix = fullname.substring(i + 1);
            for (IndexType t : IndexType.values())
            {
                if (t.suffix().equalsIgnoreCase(suffix))
                {
                    return t;
                }
            }
        }

        throw new IllegalArgumentException("Indexname does not end with valid suffix");
    }

    /**
     * Add a value to this secondary index.
     *
     * @param value the value to add
     * @return a reference to this object
     */
    public final RiakIndex<T> add(T value)
    {
        values.add(convert(value));
        return this;
    }

    /**
     * Add a set of values to this secondary index.
     *
     * @param values a collection of values to add
     * @return a reference to this object
     */
    public final RiakIndex<T> add(Collection<T> values)
    {
        for (T value : values)
        {
            add(value);
        }

        return this;
    }

    /**
     * Determine if this index contains a value
     *
     * @param value the value to check for
     * @return {@code true} if this index contains the value, {@code false}
     * otherwise.
     */
    public final boolean hasValue(T value)
    {
        return values.contains(convert(value));
    }

    /**
     * Remove a value from this index
     *
     * @param value the value to remvoe
     * @return {@code true} if the value was present and was removed,
     * {@code false} otherwise.
     */
    public final boolean remove(T value)
    {
        return values.remove(convert(value));
    }

    /**
     * Remove a set of values from this index
     *
     * @param values a collection of values to remove
     */
    public final void remove(Collection<T> values)
    {
        for (T value : values)
        {
            remove(value);
        }
    }

    @Override
    public final Iterator<T> iterator()
    {
        return new Iterator<T>()
        {
            private Iterator<ByteArrayWrapper> i = values.iterator();

            @Override
            public boolean hasNext()
            {
                return i.hasNext();
            }

            @Override
            public T next()
            {
                return convert(i.next());
            }

            @Override
            public void remove()
            {
                i.remove();
            }
        };
    }

    /**
     * Return the values in this index as raw bytes
     *
     * @return an unmodifiable view of the raw values in this index.
     */
    public final Set<ByteArrayWrapper> rawValues()
    {
        return Collections.unmodifiableSet(values);
    }

    /**
     * Return the values in this index
     *
     * @return an unmodifiable view of the values in this index.
     */
    public final Set<T> values()
    {
        Set<T> convertedValues = new HashSet<T>();
        for (ByteArrayWrapper baw : values)
        {
            convertedValues.add(convert(baw));
        }
        return Collections.unmodifiableSet(convertedValues);
    }

    /**
     * Get the type of this index
     *
     * @return the enum representing the type of this index
     */
    public final IndexType getType()
    {
        return indexName.getType();
    }

    /**
     * Get the short name of this index
     *
     * @return the name of this index without the type suffix
     */
    public final String getName()
    {
        return indexName.getName();
    }

    /**
     * Get the fully qualified name of this index
     *
     * @return the name of this index including the type suffix
     */
    public final String getFullname()
    {
        return indexName.getFullname();
    }

    /**
     * Convert a value to a ByteArrayWrapper <p> Index values are stored
     * internally as bytes. Concrete classes implement this method to convert
     * values to bytes. </p>
     *
     * @param value the value to convert
     * @return a ByteArrayWrapper containing the converted bytes
     */
    protected abstract ByteArrayWrapper convert(T value);

    /**
     * Convert bytes to a value type <p> Index values are stored internally as
     * bytes. Concrete classes implement this method to convert bytes to values.
     * </p>
     *
     * @param value the value to convert
     * @return a value of type T
     */
    protected abstract T convert(ByteArrayWrapper value);

    @Override
    public final int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((indexName.getFullname() == null) ? 0 : indexName.getFullname().hashCode());
        result = prime * result + ((indexName.getName() == null) ? 0 : indexName.getName().hashCode());
        return result;
    }

    @Override
    public final boolean equals(Object obj)
    {
        if (this == obj)
        {
            return true;
        }
        if (obj == null)
        {
            return false;
        }
        if (!(obj instanceof RiakIndex))
        {
            return false;
        }

        RiakIndex other = (RiakIndex) obj;

        if (indexName.getType() != other.getType())
        {
            return false;
        }
        else if (!indexName.getName().equals(other.indexName.getName()))
        {
            return false;
        }

        return true;
    }

    /**
     * This class serves two purposes; encapsulating the name and type of an
     * index as well as being a builder.
     *
     * @param <T> the subclass of RiakIndex this Name represents
     */
    public static abstract class Name<T>
    {
        protected String name;
        protected IndexType type;
        private Set<ByteArrayWrapper> values;
        
        protected Name(String name, IndexType type)
        {
            this.name = stripSuffix(name, type);
            this.type = type;
        }

        /**
         * If the index name has the suffix, strip it
         *
         * @param name
         * @return the name, stripped
         */
        private String stripSuffix(String name, IndexType type)
        {
            if (name.endsWith(type.suffix()))
            {
                return name.substring(0, name.indexOf(type.suffix()));
            }
            else
            {
                return name;
            }
        }

        /**
         * Get the short name of this index
         *
         * @return the name of this index without the type suffix
         */
        public final String getName()
        {
            return name;
        }

        /**
         * Get the fully qualified name of this index
         *
         * @return the name of this index including the type suffix
         */
        public final String getFullname()
        {
            return name + type.suffix();
        }

        /**
         * Get the type of this index
         *
         * @return the enum representing the type of this index
         */
        public final IndexType getType()
        {
            return type;
        }

        /**
         * Wrap an existing index
         *
         * @param otherIndex
         * @return a reference to this object
         */
        final public Name<T> wrap(RiakIndex<?> otherIndex)
        {
            values = otherIndex.values;
            return this;
        }

        /**
         * Copy the values from the supplied index into this one.
         *
         * @param otherIndex
         * @return a reference to this object
         */
        final public Name<T> copyFrom(RiakIndex<?> otherIndex)
        {
            values = Collections.newSetFromMap(new ConcurrentHashMap<ByteArrayWrapper, Boolean>());
            for (ByteArrayWrapper baw : otherIndex.values)
            {
                values.add(baw);
            }
            return this;
        }

        public abstract T createIndex();
    }
}
