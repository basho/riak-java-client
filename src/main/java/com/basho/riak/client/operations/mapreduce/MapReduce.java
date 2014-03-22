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
package com.basho.riak.client.operations.mapreduce;

import com.basho.riak.client.RiakException;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.operations.MapReduceOperation;
import com.basho.riak.client.operations.RiakCommand;
import com.basho.riak.client.query.functions.Function;
import com.basho.riak.client.util.BinaryValue;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * An operation for defining and runnig a Map/Reduce query on Riak. <p/> <p> See <a
 * href="http://wiki.basho.com/MapReduce.html">Map/Reduce</a> for details. </p>
 */
public abstract class MapReduce extends RiakCommand<MapReduce.Response>
{

	private final String JSON_CONTENT_TYPE = "application/json";

	private final Collection<MapReducePhase> phases = new ArrayList<MapReducePhase>();
	private final Long timeout;

	protected MapReduce(Builder builder)
	{
		this.phases.addAll(builder.phases);
		this.timeout = builder.timeout;
	}

	@Override
	public Response execute(RiakCluster cluster) throws ExecutionException, InterruptedException
	{
		try
		{
			BinaryValue jobSpec = BinaryValue.create(writeSpec());

            System.out.println(jobSpec);
            
			MapReduceOperation operation = new MapReduceOperation.Builder(jobSpec, JSON_CONTENT_TYPE).build();

			MapReduceOperation.Response output = cluster.execute(operation).get();

			return new Response(output.getResults());

		} catch (RiakException e)
		{
			throw new ExecutionException(e);
		}
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
			jg.setCodec(new ObjectMapper());

			jg.writeStartObject();

			jg.writeFieldName("inputs");
			writeInput(jg);

			jg.writeFieldName("query");
			jg.writeStartArray();

			writePhases(jg);

			jg.writeEndArray();
			if (timeout != null)
			{
				jg.writeNumberField("timeout", timeout);
			}

			jg.writeEndObject();
			jg.flush();

			return out.toString("UTF8");
		} catch (IOException e)
		{
			throw new RiakException(e);
		}
	}

	void writeFunctionPhase(FunctionPhase phase, JsonGenerator jg) throws IOException
	{
		writeFunction(phase.getPhaseFunction(), jg);
		if (phase.getArg() != null)
		{
			jg.writeObjectField("arg", phase.getArg());
		}
	}

	void writePhases(JsonGenerator jg) throws IOException
	{
		int cnt = 0;
		synchronized (phases)
		{
			final int lastPhase = phases.size();
			for (MapReducePhase phase : phases)
			{
				cnt++;
				jg.writeStartObject();
				jg.writeFieldName(phase.getType().toString());
				jg.writeStartObject();

				switch (phase.getType())
				{
					case MAP:
					case REDUCE:
						FunctionPhase fphase = (FunctionPhase) phase;
						writeFunctionPhase(fphase, jg);
						break;
					case LINK:
						jg.writeStringField("bucket", ((LinkPhase) phase).getBucket());
						jg.writeStringField("tag", ((LinkPhase) phase).getTag());
						break;
				}

				//the final phase results should be returned, unless specifically set otherwise
				if (cnt == lastPhase)
				{
					jg.writeBooleanField("keep", isKeepResult(true, phase.isKeep()));
				} else
				{
					jg.writeBooleanField("keep", isKeepResult(false, phase.isKeep()));
				}

				jg.writeEndObject();
				jg.writeEndObject();
			}
		}
	}

	void writeFunction(Function function, JsonGenerator jg) throws IOException
	{

		jg.writeStringField("language", function.isJavascript() ? "javascript" : "erlang");

		if (function.isJavascript())
		{
			if (function.isNamed())
			{
				jg.writeStringField("name", function.getName());
			} else if (function.isStored())
			{
				jg.writeStringField("bucket", function.getBucket());
				jg.writeStringField("key", function.getKey());
			} else if (function.isAnonymous())
			{
				jg.writeStringField("source", function.getSource());
			} else
			{
				throw new IllegalStateException("Cannot determine function type");
			}
		} else if (!function.isJavascript())
		{
			jg.writeStringField("module", function.getModule());
			jg.writeStringField("function", function.getFunction());
		}

	}

	/**
	 * Decide if a map/reduce phase result should be kept (returned) or not.
	 *
	 * @param isLastPhase    is the phase being considered the last phase in an m/r job?
	 * @param phaseKeepValue the Boolean value from a {@link MapPhase} (null|true|false)
	 * @return <code>phaseKeepValue</code> if not null, otherwise <code>true</code> if <code>isLastPhase</code> is true,
	 * false otherwise.
	 */
	boolean isKeepResult(boolean isLastPhase, Boolean phaseKeepValue)
	{
		if (phaseKeepValue != null)
		{
			return phaseKeepValue;
		} else
		{
			return isLastPhase;
		}
	}

	/**
	 * Override to write the input specification of the M/R job.
	 *
	 * @param jsonGenerator a Jackson {@link JsonGenerator} to write the input spec to
	 * @throws IOException
	 */
	protected abstract void writeInput(JsonGenerator jsonGenerator) throws IOException;

	protected static abstract class Builder<T extends Builder<T>>
	{

		protected final Collection<MapReducePhase> phases = new LinkedList<MapReducePhase>();
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
		 * @return this
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
		 * @return this
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
		 * @return this
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
		 * @return this
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
		 * @return this
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
		 * @return this
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
		 * @return this
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
		 * @return this
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

	public static class Response implements Iterable<BinaryValue>
	{

		private final List<BinaryValue> output;

		public Response(List<BinaryValue> output)
		{
			this.output = output;
		}

		@Override
		public Iterator<BinaryValue> iterator()
		{
			return output.iterator();
		}

		public List<BinaryValue> getOutput()
		{
			return output;
		}
	}
}