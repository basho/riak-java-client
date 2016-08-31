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
package com.basho.riak.client.core.query.crdt.types;

/**
 * Representation of the Riak HyperLogLog datatype.
 * <p>
 * Contains the estimated cardinality of the HLL.
 * </p>
 *
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.1
 */
public class RiakHll extends RiakDatatype
{
    private final long cardinality;

    public RiakHll(long cardinality)
    {
        this.cardinality = cardinality;
    }

    /**
     * Get the cardinality of the HyperLogLog as a Long.
     *
     * @return the estimated cardinality for this HyperLogLog.
     */
    @Override
    public Long view()
    {
        return this.cardinality;
    }

    @Override
    public String toString()
    {
        return "RiakHll{" + "cardinality=" + Long.toUnsignedString(cardinality) + '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        RiakHll that = (RiakHll) o;

        return cardinality == that.cardinality;

    }

    @Override
    public int hashCode()
    {
        return (int) (cardinality ^ (cardinality >>> 32));
    }
}
