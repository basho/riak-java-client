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
package com.basho.riak.client.query;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;

import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.operations.RiakOperation;
import com.basho.riak.client.query.functions.Function;
import com.basho.riak.client.query.serialize.FunctionToJson;
import com.basho.riak.client.raw.ErlangTermErrorParser;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.query.MapReduceSpec;
import com.basho.riak.pbc.RiakError;

/**
 * An operation for defining and runnig a Map/Reduce query on Riak.
 * 
 * <p>
 * See <a href="http://wiki.basho.com/MapReduce.html">Map/Reduce</a> for details.
 * </p>
 * @author russell
 * 
 * @see IRiakClient#mapReduce()
 * @see IRiakClient#mapReduce(String)
 */
public abstract class MapReduce implements RiakOperation<MapReduceResult> {

    private final RawClient client;

    private Collection<MapReducePhase> phases = new LinkedList<MapReducePhase>();
    private Long timeout;

    /**
     * Create the MapRedcue operation with the {@link RawClient} to delegate to.
     * 
     * @param client
     *            a {@link RawClient}
     * 
     * @see IRiakClient#mapReduce()
     * @see IRiakClient#mapReduce(String)
     */
    public MapReduce(RawClient client) {
        this.client = client;
    }

    /**
     * Run the Map/Reduce job against the {@link RawClient} the operation was
     * constructed with.
     * 
     * @return a {@link MapReduceResult} containing the results of the query.
     * @throws RiakException
     * @throws InvalidMapReduceException
     */
    public MapReduceResult execute() throws RiakException {
        validate();
        final String strSpec = writeSpec();
        MapReduceSpec spec = new MapReduceSpec(strSpec);
        try {
            return client.mapReduce(spec);
        } catch(RiakError e) {
            throw ErlangTermErrorParser.parseErlangError(e.getMessage());
        } catch (IOException e) {
            throw new RiakException(e);
        }
    }

    /**
     * Check that this map/reduce job is valid
     * @throws InvalidMapReduceException
     */
    protected void validate() {
        if(phases.isEmpty()) {
            throw new NoPhasesException();
        }
    }

    /**
     * Creates the JSON string of the M/R job for submitting to the
     * {@link RawClient}
     * 
     * Uses Jackson to write out the JSON string. I'm not very happy with this
     * method, it is a candidate for change.
     * 
     * TODO re-evaluate this method, look for something smaller and more elegant.
     * 
     * @return a String of JSON
     * @throws RiakException
     *             if, for some reason, we can't create a JSON string.
     */
    private String writeSpec() throws RiakException {

        final ByteArrayOutputStream out = new ByteArrayOutputStream();

        try {
            JsonGenerator jg = new JsonFactory().createJsonGenerator(out, JsonEncoding.UTF8);
            jg.setCodec(new ObjectMapper());

            jg.writeStartObject();

            jg.writeFieldName("inputs");
            writeInput(jg);

            jg.writeFieldName("query");
            jg.writeStartArray();

            writePhases(jg);

            jg.writeEndArray();
            if (timeout != null) {
                jg.writeNumberField("timeout", timeout);
            }

            jg.writeEndObject();
            jg.flush();

            return out.toString("UTF8");
        } catch (IOException e) {
            throw new RiakException(e);
        }
    }

    /**
     * Write the collection of phases to the json output generator
     * @param jg
     *            a {@link JsonGenerator}
     */
    private void writePhases(JsonGenerator jg) throws IOException {
        writeMapReducePhases(jg);
    }

    /**
     * @param jg
     */
    private void writeMapReducePhases(JsonGenerator jg) throws IOException {
        int cnt = 0;
        synchronized (phases) {
            final int lastPhase = phases.size();
            for (MapReducePhase phase : phases) {
                cnt++;
                jg.writeStartObject();
                jg.writeFieldName(phase.getType().toString());
                jg.writeStartObject();

                switch (phase.getType()) {
                case MAP:
                case REDUCE:
                    MapPhase mapPhase = (MapPhase)phase;
                    FunctionToJson.newWriter(mapPhase.getPhaseFunction(), jg).write();
                    if(mapPhase.getArg() != null) {
                        jg.writeObjectField("arg", mapPhase.getArg());
                    }
                    break;
                case LINK:
                    jg.writeStringField("bucket", ((LinkPhase) phase).getBucket());
                    jg.writeStringField("tag", ((LinkPhase) phase).getTag());
                    break;
                }

                //the final phase results should be returned, unless specifically set otherwise
                if(cnt == lastPhase) {
                    jg.writeBooleanField("keep", isKeepResult(true, phase.isKeep()));
                } else {
                    jg.writeBooleanField("keep", isKeepResult(false, phase.isKeep()));
                }

                jg.writeEndObject();
                jg.writeEndObject();
            }
        }
    }

	/**
	 * Decide if a map/reduce phase result should be kept (returned) or not.
	 *
	 * @param isLastPhase
	 *            is the phase being considered the last phase in an m/r job?
	 * @param phaseKeepValue
	 *            the Boolean value from a {@link MapPhase} (null|true|false)
	 * @return <code>phaseKeepValue</code> if not null, otherwise
	 *         <code>true</code> if <code>isLastPhase</code> is true, false
	 *         otherwise.
	 */
	private boolean isKeepResult(boolean isLastPhase, Boolean phaseKeepValue) {
		if (phaseKeepValue != null) {
			return phaseKeepValue;
		} else {
			return isLastPhase;
		}
	}

    /**
     * Set the operations timeout
     * @param timeout
     * @return this
     */
    public MapReduce timeout(long timeout) {
        this.timeout = timeout;
        return this;
    }

    /**
     * Add {@link MapPhase} to the query
     * 
     * @param phaseFunction
     *            the {@link Function}
     * @param keep
     *            keep the results and return them with the query results?
     * @return this
     */
    public MapReduce addMapPhase(Function phaseFunction, boolean keep) {
        synchronized (phases) {
            phases.add(new MapPhase(phaseFunction, keep));
        }

        return this;
    }

    /**
     * Add a MapPhase
     * 
     * @param phaseFunction
     *            the {@link Function}
     * @param arg
     *            an argument that will be passed to the phase verbatim
     *            (Object#toString)
     * @param keep
     *            if the result should be returned or merely provide input for
     *            the next phase.
     * @return this
     */
    public MapReduce addMapPhase(Function phaseFunction, Object arg, boolean keep) {
        synchronized (phases) {
            phases.add(new MapPhase(phaseFunction, arg, keep));
        }

        return this;
    }

    /**
     * Add a MapPhase
     * 
     * @param phaseFunction
     *            the {@link Function}
     * @param arg
     *            an argument that will be passed to the phase verbatim
     *            (Object#toString)
     * @return this
     */
    public MapReduce addMapPhase(Function phaseFunction, Object arg) {
        synchronized (phases) {
            phases.add(new MapPhase(phaseFunction, arg));
        }

        return this;
    }

    /**
     * Add a MapPhase
     * 
     * @param phaseFunction
     *            the {@link Function}
     * @return this
     */
    public MapReduce addMapPhase(Function phaseFunction) {
        synchronized (phases) {
            phases.add(new MapPhase(phaseFunction));
        }

        return this;
    }

    /**
     * Add {@link ReducePhase} to the query
     * 
     * @param phaseFunction
     *            the {@link Function}
     * @param keep
     *            keep the results and return them with the query results?
     * @return this
     */
    public MapReduce addReducePhase(Function phaseFunction, boolean keep) {
        synchronized (phases) {
            phases.add(new ReducePhase(phaseFunction, keep));
        }

        return this;
    }

    /**
     * Add a {@link ReducePhase}
     * 
     * @param phaseFunction
     *            the {@link Function}
     * @param arg
     *            an argument that will be passed to the phase verbatim
     *            (Object#toString)
     * @param keep
     *            if the result should be returned or merely provide input for
     *            the next phase.
     * @return this
     */
    public MapReduce addReducePhase(Function phaseFunction, Object arg, boolean keep) {
        synchronized (phases) {
            phases.add(new ReducePhase(phaseFunction, arg, keep));
        }

        return this;
    }

    /**
     * Add a {@link ReducePhase}
     * 
     * @param phaseFunction
     *            the {@link Function}
     * @param arg
     *            an argument that will be passed to the phase verbatim
     * @return this
     */
    public MapReduce addReducePhase(Function phaseFunction, Object arg) {
        synchronized (phases) {
            phases.add(new ReducePhase(phaseFunction, arg));
        }

        return this;
    }

    /**
     * Add a {@link ReducePhase}
     * 
     * @param phaseFunction
     * @return this
     */
    public MapReduce addReducePhase(Function phaseFunction) {
        synchronized (phases) {
            phases.add(new ReducePhase(phaseFunction));
        }

        return this;
    }

    /**
     * Add a Link Phase that points to <code>bucket</code> / <code>tag</code>
     * .
     * 
     * @param bucket
     *            the bucket at the end of the link (or "_" or "" for wildcard)
     * @param tag
     *            the tag (or ("_", or "" for wildcard)
     * @param keep
     *            to keep the result of this phase and return it at the end of
     *            the operation
     */
    public MapReduce addLinkPhase(String bucket, String tag, boolean keep) {
        synchronized (phases) {
            phases.add(new LinkPhase(bucket, tag, keep));
        }

        return this;
    }

    /**
     * Create a Link Phase that points to <code>bucket</code> / <code>tag</code>
     * <code>keep</code> will be <code>false</code>
     * 
     * @param bucket
     *            the bucket at the end of the link (or "_" or "" for wildcard)
     * @param tag
     *            the tag (or ("_", or "" for wildcard)
     */
    public MapReduce addLinkPhase(String bucket, String tag) {
        synchronized (phases) {
            phases.add(new LinkPhase(bucket, tag));
        }

        return this;
    }

    /**
     * Override to write the input specification of the M/R job.
     * 
     * @param jsonGenerator a Jackson {@link JsonGenerator} to write the input spec to
     * @throws IOException
     */
    protected abstract void writeInput(JsonGenerator jsonGenerator) throws IOException;
}