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

import com.basho.riak.client.util.BinaryValue;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Abstract base class for modeling a Riak Secondary Index (2i).
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
 * <p>
 * {@code RiakIndex} instances are created and managed via the {@link RiakIndexes}
 * container.
 * </p>
 * @riak.threadsafety This class is designed to be thread safe.
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 * @see RiakIndexes
 * @see <a
 * href="http://docs.basho.com/riak/latest/dev/using/2i/">Using Secondary
 * Indexes in Riak</a>
 */
public abstract class RiakIndex<T> implements Iterable<T>
{
    private final Set<BinaryValue> values;
	private final Name<?> name;

    /**
     * Constructs a RiakIndex from the supplied RiakIndex.Name
     * @param name A {@link Name} to build this RiakIndex from.
     */
    protected RiakIndex(Name<?> name)
    {
        this.name = name;

        if (name.values != null)
        {
            this.values = name.values;
        }
        else
        {
            this.values = Collections.newSetFromMap(new ConcurrentHashMap<BinaryValue, Boolean>());
        }
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
     * Add a asSet of values to this secondary index.
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
     * Remove a value from this index.
     *
     * @param value the value to remove
     * @return a reference to this object
     */
    public final RiakIndex<T> remove(T value)
    {
        values.remove(convert(value));
        return this;
    }

    /**
     * Remove a asSet of values from this index
     *
     * @param values a collection of values to remove
     * @return a reference to this object
     */
    public final RiakIndex<T> remove(Collection<T> values)
    {
        for (T value : values)
        {
            remove(value);
        }
        return this;
    }

    /**
     * Remove all values from this index
     * @return a reference to this object
     */
    public final RiakIndex<T> removeAll()
    {
        values.clear();
        return this;
    }

    /**
     * Returns an iterator over the asSet of values in this index.
     * This iterator is a "weakly consistent" iterator that will never throw
     * {@link ConcurrentModificationException}, and guarantees to traverse elements
     * as they existed upon construction of the iterator, and may (but is not guaranteed to)
     * reflect any modifications subsequent to construction.
     * @return an Iterator.
     */
    @Override
    public final Iterator<T> iterator()
    {
        return new Iterator<T>()
        {
            private Iterator<BinaryValue> i = values.iterator();

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
     * Return the number of values in this index.
     * @return the number of values
     */
    public final int size()
    {
        return values.size();
    }

    /**
     * Determine if this index has any values.
     * @return {@code true} if this index has no values, {@code false} otherwise.
     */
    public final boolean isEmpty()
    {
        return values.isEmpty();
    }

    /**
     * Return the values in this index as raw bytes.
     *
     * @return an unmodifiable view of the raw values in this index.
     */
    public final Set<BinaryValue> rawValues()
    {
        return Collections.unmodifiableSet(values);
    }

    /**
     * Return the values in this index.
     * The returned {@code Set} is unmodifiable.
     * @return an unmodifiable view of the values in this index.
     */
    public final Set<T> values()
    {
        Set<T> convertedValues = new HashSet<T>();
        for (BinaryValue baw : values)
        {
            convertedValues.add(convert(baw));
        }
        return Collections.unmodifiableSet(convertedValues);
    }

    /**
     * Get the type of this index.
     *
     * @return the enum representing the type of this index
     */
    public final IndexType getType()
    {
        return name.getType();
    }

    /**
     * Get the index's name.
     *
     * @return the name of this index without the type suffix
     */
    public final String getName()
    {
        return name.getName();
    }

    /**
     * Get the fully qualified name of this index
     *
     * @return the name of this index including the type suffix
     */
    public final String getFullname()
    {
        return name.getName() + getType().suffix();
    }

	/**
	 * Get the <code>RiakIndex.Name</code>
	 *
	 * @return the index name
	 */
	public final Name<?> getIndexName()
	{
		return name;
	}

    /**
     * Convert a value to a BinaryValue.
     * <p> Index values are stored
     * internally as bytes. Concrete classes implement this method to convert
     * values to bytes. </p>
     *
     * @param value the value to convert
     * @return a BinaryValue containing the converted bytes
     */
    protected abstract BinaryValue convert(T value);

    /**
     * Convert bytes to a value type.
     * <p> Index values are stored internally as
     * bytes. Concrete classes implement this method to convert bytes to values.
     * </p>
     *
     * @param value the value to convert
     * @return a value of type T
     */
    protected abstract T convert(BinaryValue value);

    /**
     * Returns a hash code value for the object.
     * This method is supported for the benefit of hash tables such as those provided by HashMap.
     * @return a hash code value for this object.
     * @see RiakIndex#equals(java.lang.Object)
     */
    @Override
    public final int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((getFullname() == null) ? 0 : getFullname().hashCode());
        result = prime * result + ((getName() == null) ? 0 : getName().hashCode());
        return result;
    }

    /**
     * Indicates whether this RiakIndex is "equal to" another RiakIndex.
     * <p>
     * Only the name and index type of the RiakIndex are used to determine equality.
     * <p>
     *
     * @param obj a RiakIndex
     * @return true if this RiakIndex has the same name and type
     */
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

        if (getType() != other.getType())
        {
            return false;
        }
        else if (!getName().equals(other.getName()))
        {
            return false;
        }

        return true;
    }

		@Override
		public String toString()
		{
				return String.format("RiakIndex [name=%s, type=%s]", getName(), getType());
		}

	/**
     * Abstract base class used to encapsulate a {@code RiakIndex} name and type.
     *
     * This class serves two purposes; encapsulating the name and type of an
     * index as well as being a builder used with {@link RiakIndexes}.
     *
     * @param <T> the RiakIndex this Name encapsulates
     * @see RiakIndexes
     */
    public static abstract class Name<T extends RiakIndex>
    {
        protected final String name;
        protected final IndexType type;
        private volatile Set<BinaryValue> values;

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
        final Name<T> wrap(RiakIndex<?> otherIndex)
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
        final Name<T> copyFrom(RiakIndex<?> otherIndex)
        {
            values = Collections.newSetFromMap(new ConcurrentHashMap<BinaryValue, Boolean>());
            for (BinaryValue baw : otherIndex.values)
            {
                values.add(baw);
            }
            return this;
        }

        abstract T createIndex();
    }
}
