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
package com.basho.riak.client.operations.kv;

import com.basho.riak.client.cap.Quorum;
import com.basho.riak.client.cap.VClock;
import com.basho.riak.client.convert.Converter;
import com.basho.riak.client.convert.Converter.OrmExtracted;
import com.basho.riak.client.convert.ConverterFactory;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.operations.StoreOperation;
import com.basho.riak.client.RiakCommand;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.operations.CoreFutureAdapter;
import com.basho.riak.client.operations.RiakOption;
import com.basho.riak.client.util.BinaryValue;

import java.util.HashMap;
import java.util.Map;

import com.basho.riak.client.query.Location;
import com.fasterxml.jackson.core.type.TypeReference;

 /*
 * @author Dave Rusek <drusek at basho dot com>
 * @since 2.0
 */
public final class StoreValue extends RiakCommand<StoreValue.Response, Location>
{
    private final Location location;
    private final Map<StoreOption<?>, Object> options =
	    new HashMap<StoreOption<?>, Object>();
    private final Object value;
    private final VClock vClock;
    private final TypeReference<?> typeReference;
    
    StoreValue(Builder builder)
    {
        this.options.putAll(builder.options);
        this.location = builder.location;
        this.value = builder.value;
	    this.vClock = builder.vClock;
        this.typeReference = builder.typeReference;
    }

    
    @SuppressWarnings("unchecked")
    @Override
    protected RiakFuture<Response, Location> executeAsync(RiakCluster cluster)
    {
       Converter converter;
        
        if (typeReference == null)
        {
            converter = ConverterFactory.getInstance().getConverter(value.getClass());
        }
        else
        {
            converter = ConverterFactory.getInstance().getConverter(typeReference);
        }
        
        final OrmExtracted orm = converter.fromDomain(value, location, vClock);
        
        RiakFuture<StoreOperation.Response, Location> coreFuture =
            cluster.execute(buildCoreOperation(orm));
        
        CoreFutureAdapter<Response, Location, StoreOperation.Response, Location> future = 
            new CoreFutureAdapter<Response, Location, StoreOperation.Response, Location>(coreFuture)
            {
                @Override
                protected Response convertResponse(StoreOperation.Response coreResponse)
                {
                    Location loc = orm.getLocation();
                        
                    if (coreResponse.hasGeneratedKey())
                    {
                        loc.setKey(coreResponse.getGeneratedKey());
                    }
                    
                    VClock clock = coreResponse.getVClock();

                    return new Response.Builder()
                        .withValues(coreResponse.getObjectList())
                        .withVClock(clock)
                        .withGeneratedKey(coreResponse.getGeneratedKey())
                        .withLocation(loc) // for ORM
                        .build();
                }

                @Override
                protected Location convertQueryInfo(Location coreQueryInfo)
                {
                    return coreQueryInfo;
                }
                
            };
        coreFuture.addListener(future);
        return future;
    }
    
    private StoreOperation buildCoreOperation(OrmExtracted orm)
    {
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

        return builder.build();
    }
    
    public static class Response extends KvResponseBase
    {
        private final BinaryValue generatedKey;
        
        Response(Init<?> builder)
        {
            super(builder);
            this.generatedKey = builder.generatedKey;
        }

        public boolean hasGeneratedKey()
        {
            return generatedKey != null;
        }
        
        public BinaryValue getGeneratedKey()
        {
            return generatedKey;
        }
        
        /**
         * @ExcludeFromJavadoc 
         */
        protected static abstract class Init<T extends Init<T>> extends KvResponseBase.Init<T>
        {
            private BinaryValue generatedKey;
            
            T withGeneratedKey(BinaryValue generatedKey)
            {
                this.generatedKey = generatedKey;
                return self();
            }
        }
        
        static class Builder extends Init<Builder>
        {

            @Override
            protected Builder self()
            {
                return this;
            }

            @Override
            Response build()
            {
                return new Response(this);
            }
        }
    }

	public static class Builder
	{

		private final Map<StoreOption<?>, Object> options =
			new HashMap<StoreOption<?>, Object>();
		private final Object value;
		private VClock vClock;
        private Location location;
        private TypeReference<?> typeReference;


        public Builder(Object value)
        {
            this.value = value;
        }
        
        public Builder(Object value, TypeReference<?> typeReference)
        {
            this.value = value;
            this.typeReference = typeReference;
        }
        
		public Builder withLocation(Location location)
        {
            this.location = location;
            return this;
        }
        
		public Builder withVectorClock(VClock vClock)
		{
			this.vClock = vClock;
			return this;
		}
        
        /**
         * Set the Riak-side timeout value.
         * <p>
         * By default, riak has a 60s timeout for operations. Setting
         * this value will override that default for this operation.
         * </p>
         * @param timeout the timeout in milliseconds to be sent to riak.
         * @return a reference to this object.
         */
        public Builder withTimeout(int timeout)
        {
            withOption(StoreOption.TIMEOUT, timeout);
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
