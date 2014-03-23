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

import com.basho.riak.client.cap.ConflictResolver;
import com.basho.riak.client.cap.ConflictResolverFactory;
import com.basho.riak.client.cap.Quorum;
import com.basho.riak.client.cap.UnresolvedConflictException;
import com.basho.riak.client.cap.VClock;
import com.basho.riak.client.convert.Converter;
import com.basho.riak.client.convert.Converter.OrmExtracted;
import com.basho.riak.client.convert.ConverterFactory;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.operations.StoreOperation;
import com.basho.riak.client.util.BinaryValue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import com.basho.riak.client.query.Location;
import com.basho.riak.client.query.RiakObject;
import java.util.ArrayList;

public class StoreValue extends RiakCommand<StoreValue.Response>
{
    private final Location location;
    private final Map<StoreOption<?>, Object> options =
	    new HashMap<StoreOption<?>, Object>();
    private final Object value;
    private final VClock vClock;

    StoreValue(Builder builder)
    {
        this.options.putAll(builder.options);
        this.location = builder.location;
        this.value = builder.value;
	    this.vClock = builder.vClock;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Response execute(RiakCluster cluster) throws ExecutionException, InterruptedException
    {
        Converter converter = ConverterFactory.getInstance().getConverterForClass(value.getClass());
        OrmExtracted orm = converter.fromDomain(value, location, vClock);
        
        StoreOperation.Builder builder = 
            new StoreOperation.Builder(orm.getLocation())
                .withContent(orm.getRiakObject());
        
        if (orm.getVclock() != null)
        {
            builder.withVClock(orm.getVclock());
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
        
        BinaryValue returnedKey = response.getLocation().getKey();

        Location k = 
            new Location(orm.getLocation().getBucketName())
                .setKey(returnedKey)
                .setBucketType(orm.getLocation().getBucketType());
	    
        VClock clock = response.getVClock();

        return new Response(response.getObjectList(), clock, k);

    }

    private <V> Converter<V> getConverter(Class<V> clazz)
    {
        return ConverterFactory.getInstance().getConverterForClass(clazz);
    }
    
    public static class Response
    {

        private final Location location;
        private final VClock vClock;
        private final List<RiakObject> values;

        Response(List<RiakObject> values, VClock vClock, Location location)
        {
            this.values = values;
            this.vClock = vClock;
            this.location = location;
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
            return values != null;
        }

        public <T> T getValue(Class<T> clazz) throws UnresolvedConflictException
        {
            List<T> converted = convertValues(clazz);
            ConflictResolver<T> resolver = 
                ConflictResolverFactory.getInstance().getConflictResolverForClass(clazz);
            
            return resolver.resolve(converted);
        }
        
        public <V> List<V> getValues(Class<V> clazz)
        {
            return convertValues(clazz);
        }

        public Location getLocation()
        {
            return location;
        }

        private <T> List<T> convertValues(Class<T> clazz)
        {
            Converter<T> converter = ConverterFactory.getInstance().getConverterForClass(clazz);
            
            List<T> convertedValues = new ArrayList<T>(values.size());
            for (RiakObject ro : values)
            {
                convertedValues.add(converter.toDomain(ro, location, vClock));
            }
            
            return convertedValues;
        }
    }

	public static class Builder
	{

		private final Map<StoreOption<?>, Object> options =
			new HashMap<StoreOption<?>, Object>();
		private final Object value;
		private VClock vClock;
        private Location location;


        public Builder(Object value)
        {
            this.value = value;
        }
        
		public Builder(Location location, Object value)
		{
			this.location = location;
			this.value = value;
		}

		public Builder withVectorClock(VClock vClock)
		{
			this.vClock = vClock;
			return this;
		}

		public <T> Builder withOption(StoreOption<T> option, T value)
		{
			options.put(option, value);
			return this;
		}

		public StoreValue build()
		{
			return new StoreValue(this);
		}
	}
}
