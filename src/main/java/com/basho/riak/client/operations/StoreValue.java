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
package com.basho.riak.client.operations;

import com.basho.riak.client.cap.Quorum;
import com.basho.riak.client.cap.VClock;
import com.basho.riak.client.convert.Converter;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.operations.StoreOperation;
import com.basho.riak.client.util.BinaryValue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.basho.riak.client.convert.Converters.convert;
import com.basho.riak.client.query.Location;

public class StoreValue<V> extends RiakCommand<StoreValue.Response<V>>
{

    private final Location location;
    private final Map<StoreOption<?>, Object> options =
	    new HashMap<StoreOption<?>, Object>();
    private final V value;
    private final Converter<V> converter;
    private final VClock vClock;

    StoreValue(Builder<V> builder)
    {
        this.options.putAll(builder.options);
        this.location = builder.location;
        this.value = builder.value;
        this.converter = builder.converter;
	    this.vClock = builder.vClock;
    }

    @Override
    public Response<V> execute(RiakCluster cluster) throws ExecutionException, InterruptedException
    {

        StoreOperation.Builder builder = new StoreOperation.Builder(location);
        
        builder.withContent(converter.fromDomain(value));

        if (vClock != null)
        {
            builder.withVClock(vClock);
        }

        for (Map.Entry<StoreOption<?>, Object> opPair : options.entrySet())
        {

            RiakOption<?> option = opPair.getKey();

            if (option == StoreOption.TIMEOUT)
            {
                builder.withTimeout((Integer) opPair.getValue());
            }
            else if (option == StoreOption.RETURN_HEAD)
            {
                builder.withReturnHead((Boolean) opPair.getValue());
            }
            else if (option == StoreOption.ASIS)
            {
                builder.withAsis((Boolean) opPair.getValue());
            }
            else if (option == StoreOption.DW)
            {
                builder.withDw(((Quorum) opPair.getValue()).getIntValue());
            }
            else if (option == StoreOption.IF_NONE_MATCH)
            {
                builder.withIfNoneMatch((Boolean) opPair.getValue());
            }
            else if (option == StoreOption.IF_NOT_MODIFIED)
            {
                builder.withIfNotModified((Boolean) opPair.getValue());
            }
            else if (option == StoreOption.N_VAL)
            {
                builder.withNVal((Integer) opPair.getValue());
            }
            else if (option == StoreOption.PW)
            {
                builder.withPw(((Quorum) opPair.getValue()).getIntValue());
            }
            else if (option == StoreOption.SLOPPY_QUORUM)
            {
                builder.withSloppyQuorum((Boolean) opPair.getValue());
            }
            else if (option == StoreOption.W)
            {
                builder.withW(((Quorum) opPair.getValue()).getIntValue());
            }
            else if (option == StoreOption.RETURN_BODY)
            {
                builder.withReturnBody((Boolean) opPair.getValue());
            }

        }

        StoreOperation operation = builder.build();

        StoreOperation.Response response = cluster.execute(operation).get();
        List<V> converted = convert(converter, response.getObjectList());

	    BinaryValue returnedKey = response.getLocation().getKey();

        Location k = 
            new Location(location.getBucketName()).setKey(returnedKey)
                .setBucketType(location.getBucketType());
	    
        VClock clock = response.getVClock();

        return new Response<V>(converted, clock, k);

    }

    public static class Response<T>
    {

        private final Location key;
        private final VClock vClock;
        private final List<T> value;

        Response(List<T> value, VClock vClock, Location key)
        {
            this.value = value;
            this.vClock = vClock;
            this.key = key;
        }

        public boolean hasvClock()
        {
            return vClock != null;
        }

        public VClock getvClock()
        {
            return vClock;
        }

        public boolean hasValue()
        {
            return value != null;
        }

        public List<T> getValue()
        {
            return value;
        }

        public Location getLocation()
        {
            return key;
        }

    }

	public static class Builder<V>
	{

		private final Location location;
		private final Map<StoreOption<?>, Object> options =
			new HashMap<StoreOption<?>, Object>();
		private final V value;
		private Converter<V> converter;
		private VClock vClock;

		public Builder(Location location, V value)
		{
			this.location = location;
			this.value = value;
		}

		public Builder<V> withVectorClock(VClock vClock)
		{
			this.vClock = vClock;
			return this;
		}

		public <T> Builder<V> withOption(StoreOption<T> option, T value)
		{
			options.put(option, value);
			return this;
		}

		public Builder<V> withConverter(Converter<V> converter)
		{
			this.converter = converter;
			return this;
		}

		public StoreValue<V> build()
		{
			return new StoreValue<V>(this);
		}
	}
}
