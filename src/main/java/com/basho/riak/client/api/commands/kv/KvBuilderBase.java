package com.basho.riak.client.api.commands.kv;


import com.basho.riak.client.api.commands.RiakOption;
import com.basho.riak.client.core.query.Location;

import java.util.HashMap;
import java.util.Map;

public abstract class KvBuilderBase<ConstructedType>
{
    protected final Location location;
    protected final Map<RiakOption<?>, Object> options = new HashMap<>();

    protected KvBuilderBase(Location location)
    {
        if (location == null)
        {
            throw new IllegalArgumentException("Location cannot be null");
        }
        this.location = location;
    }

    /**
     * Add an optional setting for this command.
     * This will be passed along with the request to Riak to tell it how
     * to behave when servicing the request.
     *
     * @param option the option
     * @param value the value for the option
     * @return a reference to this object.
     */
    protected void addOption(RiakOption<?> option, Object value)
    {
        options.put(option, value);
    }

    public abstract ConstructedType build();
}
