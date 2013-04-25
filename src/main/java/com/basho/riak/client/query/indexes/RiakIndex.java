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

/**
 * Models a secondary index name.
 *
 * In Riak you must specify the type of the index with a suffix _bin for binary
 * and _int for integer. This class will do that for you.
 *
 * @author Russel Brown <russelldb at basho dot com>
 * @since 1.0
 */
public abstract class RiakIndex<T>
{

    private final String name;
    private final String fullname;

    /**
     * @param name
     * @param fullname
     */
    protected RiakIndex(String name)
    {
        this.name = stripSuffix(name);
        this.fullname = this.name + getSuffix();
    }

    /**
     * If the index has the suffix on, strip it
     *
     * @param name
     * @return the name, stripped
     */
    private String stripSuffix(String name)
    {
        if (name.endsWith(getSuffix()))
        {
            return name.substring(0, name.indexOf(getSuffix()));
        }
        return name;
    }

    /**
     * @return the name
     */
    public String getName()
    {
        return name;
    }

    /**
     * @return the fullname
     */
    public String getFullname()
    {
        return fullname;
    }

    protected abstract String getSuffix();

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((fullname == null) ? 0 : fullname.hashCode());
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        return result;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj)
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
        RiakIndex<?> other = (RiakIndex<?>) obj;
        if (fullname == null)
        {
            if (other.fullname != null)
            {
                return false;
            }
        }
        else if (!fullname.equals(other.fullname))
        {
            return false;
        }
        if (name == null)
        {
            if (other.name != null)
            {
                return false;
            }
        }
        else if (!name.equals(other.name))
        {
            return false;
        }
        return true;
    }
}
