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
import com.basho.riak.client.convert.ConverterFactory;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.operations.FetchOperation;
import com.basho.riak.client.query.Location;
import com.basho.riak.client.query.RiakObject;
import com.fasterxml.jackson.core.type.TypeReference;
import java.util.ArrayList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;


/**
 * Command used to fetch a value from Riak, referenced by it's key.
 */
public class FetchValue extends RiakCommand<FetchValue.Response>
{

	private final Location location;
	private final Map<FetchOption<?>, Object> options =
		new HashMap<FetchOption<?>, Object>();

	FetchValue(Builder builder)
	{
		this.location = builder.location;
		this.options.putAll(builder.options);
	}

	@Override
	Response execute(RiakCluster cluster) throws ExecutionException, InterruptedException
	{

		FetchOperation.Builder builder = new FetchOperation.Builder(location);

		for (Map.Entry<FetchOption<?>, Object> opPair : options.entrySet())
		{

			RiakOption<?> option = opPair.getKey();

			if (option == FetchOption.R)
			{
				builder.withR(((Quorum) opPair.getValue()).getIntValue());
			} else if (option == FetchOption.DELETED_VCLOCK)
			{
				builder.withReturnDeletedVClock((Boolean) opPair.getValue());
			} else if (option == FetchOption.TIMEOUT)
			{
				builder.withTimeout((Integer) opPair.getValue());
			} else if (option == FetchOption.HEAD)
			{
				builder.withHeadOnly((Boolean) opPair.getValue());
			} else if (option == FetchOption.BASIC_QUORUM)
			{
				builder.withBasicQuorum((Boolean) opPair.getValue());
			} else if (option == FetchOption.IF_MODIFIED)
			{
				VClock clock = (VClock) opPair.getValue();
				builder.withIfNotModified(clock.getBytes());
			} else if (option == FetchOption.N_VAL)
			{
				builder.withNVal((Integer) opPair.getValue());
			} else if (option == FetchOption.PR)
			{
				builder.withPr(((Quorum) opPair.getValue()).getIntValue());
			} else if (option == FetchOption.SLOPPY_QUORUM)
			{
				builder.withSloppyQuorum((Boolean) opPair.getValue());
			} else if (option == FetchOption.NOTFOUND_OK)
			{
				builder.withNotFoundOK((Boolean) opPair.getValue());
			}

		}

		FetchOperation operation = builder.build();

		FetchOperation.Response response = cluster.execute(operation).get();
        
		return new Response(response.isNotFound(), 
                                response.isUnchanged(),
                                response.getLocation(),
                                response.getObjectList(), 
                                response.getVClock());

	}

	/**
	 * A response from Riak including the vector clock.
	 *
	 */
	public static class Response
	{
        private final Location location;
		private final boolean notFound;
		private final boolean unchanged;
		private final VClock vClock;
		private final List<RiakObject> values;

		Response(boolean notFound, boolean unchanged, Location location, List<RiakObject> values, VClock vClock)
		{
			this.notFound = notFound;
			this.unchanged = unchanged;
			this.values = values;
			this.vClock = vClock;
            this.location = location;
		}

        public Location getLocation()
        {
            return location;
        }
        
		public boolean isNotFound()
		{
			return notFound;
		}

		public boolean isUnchanged()
		{
			return unchanged;
		}

		public boolean hasvClock()
		{
			return vClock != null;
		}

		public VClock getvClock()
		{
			return vClock;
		}

		public boolean hasValues()
		{
			return !values.isEmpty();
		}

		public <T> List<T> getValues(Class<T> clazz)
		{
			Converter<T> converter = ConverterFactory.getInstance().getConverter(clazz);
            return convertValues(converter);
		}

        public <T> T getValue(Class<T> clazz) throws UnresolvedConflictException
        {
            Converter<T> converter = ConverterFactory.getInstance().getConverter(clazz);
            List<T> convertedValues = convertValues(converter);
            
            ConflictResolver<T> resolver = 
                ConflictResolverFactory.getInstance().getConflictResolver(clazz);
            
            return resolver.resolve(convertedValues);
        }
        
        public <T> T getValue(TypeReference<T> typeReference) throws UnresolvedConflictException
        {
            Converter<T> converter = ConverterFactory.getInstance().getConverter(typeReference);
            List<T> convertedValues = convertValues(converter);
            
            ConflictResolver<T> resolver = 
                ConflictResolverFactory.getInstance().getConflictResolver(typeReference);
            
            return resolver.resolve(convertedValues);
        }
        
        public <T> List<T> getValues(TypeReference<T> typeReference)
        {
            Converter<T> converter = ConverterFactory.getInstance().getConverter(typeReference);
            return convertValues(converter);
        }
        
        private <T> List<T> convertValues(Converter<T> converter)
        {
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

		private final Location location;
		private final Map<FetchOption<?>, Object> options =
			new HashMap<FetchOption<?>, Object>();

		public Builder(Location location)
		{
			this.location = location;
		}

		/**
		 * Add an optional setting for this command. This will be passed along with the request to Riak to tell it how
		 * to behave when servicing the request.
		 *
		 * @param option
		 * @param value
		 * @param <U>
		 * @return
		 */
		public <U> Builder withOption(FetchOption<U> option, U value)
		{
			options.put(option, value);
			return this;
		}

		/**
		 * Build a {@link FetchValue} object
		 *
		 * @return a FetchValue command
		 */
		public FetchValue build()
		{
            return new FetchValue(this);
		}
	}
}
