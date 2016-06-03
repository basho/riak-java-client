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
package com.basho.riak.client.api.commands.kv;

import com.basho.riak.client.api.cap.Quorum;
import com.basho.riak.client.api.cap.VClock;
import com.basho.riak.client.api.convert.Converter;
import com.basho.riak.client.api.convert.Converter.OrmExtracted;
import com.basho.riak.client.api.convert.ConverterFactory;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.operations.StoreOperation;
import com.basho.riak.client.api.RiakCommand;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.api.commands.CoreFutureAdapter;
import com.basho.riak.client.api.commands.RiakOption;
import com.basho.riak.client.core.util.BinaryValue;

import java.util.HashMap;
import java.util.Map;

import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.fasterxml.jackson.core.type.TypeReference;

/**
 * Command used to store a value in Riak.
 * <script src="https://google-code-prettify.googlecode.com/svn/loader/run_prettify.js"></script>
 * <p>
 * To store a value in Riak you supply a {@link com.basho.riak.client.core.query.Location}
 * and an object to store. The object may be an instance of {@link com.basho.riak.client.core.query.RiakObject}
 * or your own POJO. In the case of a POJO the default serialization uses the Jackson JSON library
 * to store your object as JSON in Riak.
 * <pre class="prettyprint">
 * {@code
 * Namespace ns = new Namespace("my_type","my_bucket");
 * Location loc = new Location(ns, "my_key");
 * RiakObject ro = new RiakObject();
 * ro.setValue(BinaryValue.create("This is my value"));
 * StoreValue sv = 
 *      new StoreValue.Builder(ro).withLocation(loc).build();
 * StoreValue.Response response = client.execute(sv);}</pre>
 * </p>
 * <p>
 * All operations can called async as well.
 * <pre class="prettyprint">
 * {@code
 * ...
 * RiakFuture<StoreValue.Response, Location> future = client.executeAsync(sv);
 * ...
 * future.await();
 * if (future.isSuccess())
 * { 
 *     ... 
 * }}</pre>
 * </p>
 * @author Dave Rusek <drusek at basho dot com>
 * @since 2.0
 */
public final class StoreValue extends RiakCommand<StoreValue.Response, Location>
{
    private final Namespace namespace;
    private final BinaryValue key;
    private final Map<Option<?>, Object> options =
        new HashMap<Option<?>, Object>();
    private final Object value;
    private final TypeReference<?> typeReference;
    private final VClock vclock;
    
    StoreValue(Builder builder)
    {
        this.options.putAll(builder.options);
        this.namespace = builder.namespace;
        this.key = builder.key;
        this.value = builder.value;
        this.typeReference = builder.typeReference;
        this.vclock = builder.vclock;
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
        
        final OrmExtracted orm = converter.fromDomain(value, namespace, key);
        
        // If there's no vector clock in the object, use one possibly given via
        // the builder.
        if (orm.getRiakObject().getVClock() == null)
        {
            orm.getRiakObject().setVClock(vclock);
        }
        
        RiakFuture<StoreOperation.Response, Location> coreFuture =
            cluster.execute(buildCoreOperation(orm));
        
        CoreFutureAdapter<Response, Location, StoreOperation.Response, Location> future = 
            new CoreFutureAdapter<Response, Location, StoreOperation.Response, Location>(coreFuture)
            {
                @Override
                protected Response convertResponse(StoreOperation.Response coreResponse)
                {
                    Namespace ns = orm.getNamespace();
                    BinaryValue key = orm.getKey();
                    if (coreResponse.hasGeneratedKey())
                    {
                        key = coreResponse.getGeneratedKey();
                    }
                    
                    Location loc = new Location(ns, key);
                    
                    return new Response.Builder()
                        .withValues(coreResponse.getObjectList())
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
        StoreOperation.Builder builder;
        
        if (orm.hasKey())
        {
            Location loc = new Location(orm.getNamespace(), orm.getKey());
            builder = new StoreOperation.Builder(loc);
        }
        else
        {
            builder = new StoreOperation.Builder(orm.getNamespace());
        }
        
        builder.withContent(orm.getRiakObject());
        
        for (Map.Entry<Option<?>, Object> opPair : options.entrySet())
        {

            RiakOption<?> option = opPair.getKey();

            if (option == Option.TIMEOUT)
            {
                builder.withTimeout((Integer) opPair.getValue());
            }
            else if (option == Option.RETURN_HEAD)
            {
                builder.withReturnHead((Boolean) opPair.getValue());
            }
            else if (option == Option.ASIS)
            {
                builder.withAsis((Boolean) opPair.getValue());
            }
            else if (option == Option.DW)
            {
                builder.withDw(((Quorum) opPair.getValue()).getIntValue());
            }
            else if (option == Option.IF_NONE_MATCH)
            {
                builder.withIfNoneMatch((Boolean) opPair.getValue());
            }
            else if (option == Option.IF_NOT_MODIFIED)
            {
                builder.withIfNotModified((Boolean) opPair.getValue());
            }
            else if (option == Option.N_VAL)
            {
                builder.withNVal((Integer) opPair.getValue());
            }
            else if (option == Option.PW)
            {
                builder.withPw(((Quorum) opPair.getValue()).getIntValue());
            }
            else if (option == Option.SLOPPY_QUORUM)
            {
                builder.withSloppyQuorum((Boolean) opPair.getValue());
            }
            else if (option == Option.W)
            {
                builder.withW(((Quorum) opPair.getValue()).getIntValue());
            }
            else if (option == Option.RETURN_BODY)
            {
                builder.withReturnBody((Boolean) opPair.getValue());
            }

        }

        return builder.build();
    }
    
    /**
    * Options For controlling how Riak performs the store operation.
    * <p>
    * These options can be supplied to the {@link StoreValue.Builder} to change
    * how Riak performs the operation. These override the defaults provided
    * by the bucket.
    * </p>
    * @author Dave Rusek <drusek at basho dot com>
    * @since 2.0
    * @see <a href="http://docs.basho.com/riak/latest/dev/advanced/cap-controls/">Replication Properties</a>
    */
   public final static class Option<T> extends RiakOption<T>
   {

       /**
        * Write Quorum.
        * How many replicas to write to before returning a successful response.
        */
       public static final Option<Quorum> W = new Option<Quorum>("W");
       /**
        * Durable Write Quorum.
        * How many replicas to commit to durable storage before returning a successful response.
        */
       public static final Option<Quorum> DW = new Option<Quorum>("DW");
       /**
        * Primary Write Quorum.
        * How many primary nodes must be up when the write is attempted.
        */
       public static final Option<Quorum> PW = new Option<Quorum>("PW");
       /**
        * If Not Modified.
        * Update the value only if the vclock in the supplied object matches the one in the database.
        */
       public static final Option<Boolean> IF_NOT_MODIFIED = new Option<Boolean>("IF_NOT_MODIFIED");
       /**
        * If None Match.
        * Store the value only if this bucket/key combination are not already defined.
        */
       public static final Option<Boolean> IF_NONE_MATCH = new Option<Boolean>("IF_NONE_MATCH");
       /**
        * Return Body.
        * Return the object stored in Riak. Note this will return all siblings.
        */
       public static final Option<Boolean> RETURN_BODY = new Option<Boolean>("RETURN_BODY");
       /**
        * Return Head.
        * Like {@link #RETURN_BODY} except that the value(s) in the object are blank to 
        * avoid returning potentially large value(s).
        */
       public static final Option<Boolean> RETURN_HEAD = new Option<Boolean>("RETURN_HEAD");
       /**
        * Timeout.
        * Sets the server-side timeout for this operation. The default in Riak is 60 seconds.
        */
       public static final Option<Integer> TIMEOUT = new Option<Integer>("TIMEOUT");

       public static final Option<Boolean> ASIS = new Option<Boolean>("ASIS");
       public static final Option<Boolean> SLOPPY_QUORUM = new Option<Boolean>("SLOPPY_QUORUM");
       public static final Option<Integer> N_VAL = new Option<Integer>("N_VAL");


       private Option(String name)
       {
           super(name);
       }
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

    /**
     * Used to construct a StoreValue command.
     */
    public static class Builder
    {

        private final Map<Option<?>, Object> options =
            new HashMap<Option<?>, Object>();
        private final Object value;
        private Namespace namespace;
        private BinaryValue key;
        private TypeReference<?> typeReference;
        private VClock vclock;

        

        /**
         * Construct a Builder for a StoreValue command.
         * <p>
         * Prior to storing the object, it's raw type (class) will
         * be used to retrieve a converter from the {@link com.basho.riak.client.api.convert.ConverterFactory}.
         * For anything other than a RiakObject this is the {@link com.basho.riak.client.api.convert.JSONConverter}
         * by default.
         * </p>
         * @param value The object to be stored in Riak. 
         */
        public Builder(Object value)
        {
            this.value = value;
        }
        
        /**
         * Construct a Builder for a StoreValue command.
         * <p>
         * Prior to storing the object, the supplied TypeReference will
         * be used to retrieve a converter from the {@link com.basho.riak.client.api.convert.ConverterFactory}.
         * For anything other than a RiakObject this is the {@link com.basho.riak.client.api.convert.JSONConverter}
         * by default.
         * </p>
         * @param value The object to be stored in Riak. 
         * @param typeReference the TypeReference for the object.
         */
        public Builder(Object value, TypeReference<?> typeReference)
        {
            this.value = value;
            this.typeReference = typeReference;
        }
        
        /**
         * Set the location to store the object.
         * <p>
         * When storing a RiakObject or a POJO that does not have annotations for
         * the bucket and key, a {@link com.basho.riak.client.core.query.Location} 
         * must be provided.
         * </p>
         * @param location the location to store the object in Riak.
         * @return a reference to this object.
         */
        public Builder withLocation(Location location)
        {
            this.namespace = location.getNamespace();
            this.key = location.getKey();
            return this;
        }
        
        /**
         * Set the namespace to store the object.
         * <p>
         * When storing a POJO that does not have an annotation for the 
         * bucket type and bucket, a {@link com.basho.riak.client.core.query.Namespace} 
         * must be supplied.
         * </p>
         * @param namespace The namespaec to store the object.
         * @return a reference to this object.
         */
        public Builder withNamespace(Namespace namespace)
        {
            this.namespace = namespace;
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
            withOption(Option.TIMEOUT, timeout);
            return this;
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
        public <T> Builder withOption(Option<T> option, T value)
        {
            options.put(option, value);
            return this;
        }

        /**
         * Set the vector clock.
         * <p>
         * When storing core Java types ({@code HashMap},
         * {@code ArrayList},{@code String}, etc) or non-annotated POJOs this
         * method allows you to specify the vector clock retrieved from a 
         * prior fetch operation. 
         * </p>
         * @param vclock The vector clock to send to Riak.
         * @return a reference to this object.
         */
        public Builder withVectorClock(VClock vclock)
        {
            this.vclock = vclock;
            return this;
        }
        
        /**
         * Construct the StoreValue command.
         * @return the new StoreValue command.
         */
        public StoreValue build()
        {
            return new StoreValue(this);
        }
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + (namespace != null ? namespace.hashCode() : 0);
        result = prime * result + (key != null ? key.hashCode() : 0);
        result = prime * result + options.hashCode();
        result = prime * result + (value != null ? value.hashCode() : 0);
        result = prime * result + (typeReference != null ? typeReference.hashCode() : 0);
        result = prime * result + (vclock != null ? vclock.hashCode() : 0);
        return result;
    }

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
        if (!(obj instanceof StoreValue))
        {
            return false;
        }

        final StoreValue other = (StoreValue) obj;
        if (this.namespace != other.namespace && (this.namespace == null || !this.namespace.equals(other.namespace)))
        {
            return false;
        }
        if (this.key != other.key && (this.key == null || !this.key.equals(other.key)))
        {
            return false;
        }
        if (this.options != other.options && (this.options == null || !this.options.equals(other.options)))
        {
            return false;
        }
        if (this.value != other.value && (this.value == null || !this.value.equals(other.value)))
        {
            return false;
        }
        if (this.typeReference != other.typeReference && (this.typeReference == null || !this.typeReference.equals(other.typeReference)))
        {
            return false;
        }
        if (this.vclock != other.vclock && (this.vclock == null || !this.vclock.equals(other.vclock)))
        {
            return false;
        }
        return true;
    }

    @Override
    public String toString()
    {
        return String.format("{namespace: %s, key: %s, options: %s, value: %s,"
                + " typeReference: %s, vclock: %s}", namespace, key, options,
                value, typeReference, vclock);
    }
}
