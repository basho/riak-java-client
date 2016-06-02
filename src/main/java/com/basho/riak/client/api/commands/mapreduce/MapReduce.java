/*
 * This file is provided to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.basho.riak.client.api.commands.mapreduce;

import com.basho.riak.client.api.RiakCommand;
import com.basho.riak.client.api.RiakException;
import com.basho.riak.client.api.convert.ConversionException;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.MapReduceOperation;
import com.basho.riak.client.api.commands.CoreFutureAdapter;
import com.basho.riak.client.core.query.functions.Function;
import com.basho.riak.client.core.util.BinaryValue;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;

/**
 * Base abstract class for all MapReduce commands.
 * <p> See <a
 * href="http://wiki.basho.com/MapReduce.html">Map/Reduce</a> for details. </p>
 * @author Dave Rusek <drusek at basho dot com>
 * @since 2.0
 */
public abstract class MapReduce extends RiakCommand<MapReduce.Response, BinaryValue>
{
	private final MapReduceSpec spec;

    @SuppressWarnings("unchecked")
	protected MapReduce(MapReduceInput input, Builder builder)
	{
		this.spec = new MapReduceSpec(input, builder.phases, builder.timeout);
	}

	@Override
	protected RiakFuture<Response, BinaryValue> executeAsync(RiakCluster cluster)
	{
		
        BinaryValue jobSpec;
		try
		{
			String spec = writeSpec();
            jobSpec = BinaryValue.create(spec);
		} catch (RiakException e)
		{
			throw new RuntimeException(e);
		}

		MapReduceOperation operation = new MapReduceOperation.Builder(jobSpec).build();

		final RiakFuture<MapReduceOperation.Response, BinaryValue> coreFuture = cluster.execute(operation);

		CoreFutureAdapter<Response, BinaryValue, MapReduceOperation.Response, BinaryValue> future =
				new CoreFutureAdapter<Response, BinaryValue, MapReduceOperation.Response, BinaryValue>(coreFuture)
				{
					@Override
					protected Response convertResponse(MapReduceOperation.Response coreResponse)
					{
						return new Response(coreResponse.getResults());
					}

					@Override
					protected BinaryValue convertQueryInfo(BinaryValue coreQueryInfo)
					{
						return coreQueryInfo;
					}
				};

		coreFuture.addListener(future);

		return future;

	}

	/**
	 * Creates the JSON string of the M/R job for submitting to the client
	 * <p/>
	 * Uses Jackson to write out the JSON string. I'm not very happy with this method, it is a candidate for change.
	 * <p/>
	 * TODO re-evaluate this method, look for something smaller and more elegant.
	 *
	 * @return a String of JSON
	 * @throws RiakException if, for some reason, we can't create a JSON string.
	 */
	String writeSpec() throws RiakException
	{

		final ByteArrayOutputStream out = new ByteArrayOutputStream();

		try
		{
			JsonGenerator jg = new JsonFactory().createGenerator(out, JsonEncoding.UTF8);

			ObjectMapper objectMapper = new ObjectMapper();
			SimpleModule specModule = new SimpleModule("SpecModule", Version.unknownVersion());
			specModule.addSerializer(LinkPhase.class, new LinkPhaseSerializer());
			specModule.addSerializer(FunctionPhase.class, new FunctionPhaseSerializer());
			specModule.addSerializer(BucketInput.class, new BucketInputSerializer());
			specModule.addSerializer(SearchInput.class, new SearchInputSerializer());
			specModule.addSerializer(BucketKeyInput.class, new BucketKeyInputSerializer());
			specModule.addSerializer(IndexInput.class, new IndexInputSerializer());
			objectMapper.registerModule(specModule);

			jg.setCodec(objectMapper);

			List<MapReducePhase> phases = spec.getPhases();
			phases.get(phases.size() - 1).setKeep(true);
			jg.writeObject(spec);

			jg.flush();

			return out.toString("UTF8");

		} catch (IOException e)
		{
			throw new RiakException(e);
		}
	}

    /**
     * Base abstract class for all MapReduce command builders.
     */
	protected static abstract class Builder<T extends Builder<T>>
	{

		protected final List<MapReducePhase> phases = new LinkedList<MapReducePhase>();
		protected Long timeout;

		/**
		 * Set the operations timeout
		 *
		 * @param timeout
		 * @return this
		 */
		public T timeout(long timeout)
		{
			this.timeout = timeout;
			return self();
		}

		/**
		 * Add {@link MapPhase} to the query
		 *
		 * @param phaseFunction the {@link Function}
		 * @param keep          keep the results and return them with the query results?
		 * @return a reference to this object.
		 */
		public T withMapPhase(Function phaseFunction, boolean keep)
		{
			synchronized (phases)
			{
				phases.add(new MapPhase(phaseFunction, keep));
			}

			return self();
		}

		/**
		 * Add a MapPhase
		 *
		 * @param phaseFunction the {@link Function}
		 * @param arg           an argument that will be passed to the phase verbatim (Object#toString)
		 * @param keep          if the result should be returned or merely provide input for the next phase.
		 * @return a reference to this object.
		 */
		public T withMapPhase(Function phaseFunction, Object arg, boolean keep)
		{
			synchronized (phases)
			{
				phases.add(new MapPhase(phaseFunction, arg, keep));
			}

			return self();
		}

		/**
		 * Add a MapPhase
		 *
		 * @param phaseFunction the {@link Function}
		 * @param arg           an argument that will be passed to the phase verbatim (Object#toString)
		 * @return a reference to this object.
		 */
		public T withMapPhase(Function phaseFunction, Object arg)
		{
			synchronized (phases)
			{
				phases.add(new MapPhase(phaseFunction, arg));
			}

			return self();
		}

		/**
		 * Add a MapPhase
		 *
		 * @param phaseFunction the {@link Function}
		 * @return a reference to this object.
		 */
		public T withMapPhase(Function phaseFunction)
		{
			synchronized (phases)
			{
				phases.add(new MapPhase(phaseFunction));
			}

			return self();
		}

		/**
		 * Add {@link ReducePhase} to the query
		 *
		 * @param phaseFunction the {@link Function}
		 * @param keep          keep the results and return them with the query results?
		 * @return a reference to this object.
		 */
		public T withReducePhase(Function phaseFunction, boolean keep)
		{
			synchronized (phases)
			{
				phases.add(new ReducePhase(phaseFunction, keep));
			}

			return self();
		}

		/**
		 * Add a {@link ReducePhase}
		 *
		 * @param phaseFunction the {@link Function}
		 * @param arg           an argument that will be passed to the phase verbatim (Object#toString)
		 * @param keep          if the result should be returned or merely provide input for the next phase.
		 * @return a reference to this object.
		 */
		public T withReducePhase(Function phaseFunction, Object arg, boolean keep)
		{
			synchronized (phases)
			{
				phases.add(new ReducePhase(phaseFunction, arg, keep));
			}

			return self();
		}

		/**
		 * Add a {@link ReducePhase}
		 *
		 * @param phaseFunction the {@link Function}
		 * @param arg           an argument that will be passed to the phase verbatim
		 * @return a reference to this object.
		 */
		public T withReducePhase(Function phaseFunction, Object arg)
		{
			synchronized (phases)
			{
				phases.add(new ReducePhase(phaseFunction, arg));
			}

			return self();
		}

		/**
		 * Add a {@link ReducePhase}
		 *
		 * @param phaseFunction
		 * @return a reference to this object.
		 */
		public T withReducePhase(Function phaseFunction)
		{
			synchronized (phases)
			{
				phases.add(new ReducePhase(phaseFunction));
			}

			return self();
		}

		/**
		 * Add a Link Phase that points to <code>bucket</code> / <code>tag</code> .
		 *
		 * @param bucket the bucket at the end of the link (or "_" or "" for wildcard)
		 * @param tag    the tag (or ("_", or "" for wildcard)
		 * @param keep   to keep the result of this phase and return it at the end of the operation
         * @return a reference to this object.
		 */
		public T withLinkPhase(String bucket, String tag, boolean keep)
		{
			synchronized (phases)
			{
				phases.add(new LinkPhase(bucket, tag, keep));
			}

			return self();
		}

		/**
		 * Create a Link Phase that points to <code>bucket</code> / <code>tag</code> <code>keep</code> will be
		 * <code>false</code>
		 *
		 * @param bucket the bucket at the end of the link (or "_" or "" for wildcard)
		 * @param tag    the tag (or ("_", or "" for wildcard)
         * @return a reference to this object.
		 */
		public T withLinkPhase(String bucket, String tag)
		{
			synchronized (phases)
			{
				phases.add(new LinkPhase(bucket, tag));
			}

			return self();
		}

		protected abstract T self();

	}

    /**
     * Response from a MapReduce command.
     */
	public static class Response
	{

		private final Map<Integer, ArrayNode> results;

		public Response(Map<Integer, ArrayNode> results)
		{
			this.results = results;
		}

        public boolean hasResultForPhase(int i)
        {
            return results.containsKey(i);
        }
        
        public ArrayNode getResultForPhase(int i)
        {
            return results.get(i);
        }
        
        public ArrayNode getResultsFromAllPhases()
        {
            return flattenResults();
        }
        
        public <T> Collection<T> getResultsFromAllPhases(Class<T> resultType)
        {
            ArrayNode flat = flattenResults();
            ObjectMapper mapper = new ObjectMapper();
            try
            {
                return mapper.readValue(flat.toString(), mapper.getTypeFactory().constructCollectionType(Collection.class, resultType));
            }
            catch (IOException ex)
            {
                throw new ConversionException("Could not convert Mapreduce response",ex);
            }
        }
        
        private ArrayNode flattenResults()
        {
            final JsonNodeFactory factory = JsonNodeFactory.instance;
            ArrayNode flatArray = factory.arrayNode();
            for (Map.Entry<Integer,ArrayNode> entry : results.entrySet())
            {
                flatArray.addAll(entry.getValue());
            }
            return flatArray;
        }
		
	}
}