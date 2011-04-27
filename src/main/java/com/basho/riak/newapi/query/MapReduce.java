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
package com.basho.riak.newapi.query;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;

import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;

import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.query.MapReduceSpec;
import com.basho.riak.newapi.RiakException;
import com.basho.riak.newapi.operations.RiakOperation;
import com.basho.riak.newapi.query.functions.Function;
import com.basho.riak.newapi.query.serialize.FunctionToJson;

/**
 * @author russell
 * 
 */
public abstract class MapReduce implements RiakOperation<MapReduceResult> {

    private final RawClient client;

    private Collection<MapReducePhase> phases = new LinkedList<MapReducePhase>();
    private Long timeout;

    /**
     * @param client
     */
    public MapReduce(RawClient client) {
        this.client = client;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.RiakOperation#execute()
     */
    public MapReduceResult execute() throws RiakException {
        final String strSpec = writeSpec();
        MapReduceSpec spec = new MapReduceSpec(strSpec);
        try {
            return client.mapReduce(spec);
        } catch (IOException e) {
            throw new RiakException(e);
        }
    }

    /**
     * Creates the JSON string
     * 
     * @return a String of JSON
     * @throws RiakException
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
        synchronized (phases) {
            for (MapReducePhase phase : phases) {
                jg.writeStartObject();
                jg.writeFieldName(phase.getType().toString());
                jg.writeStartObject();

                switch (phase.getType()) {
                case MAP:
                case REDUCE:
                    FunctionToJson.newWriter(((MapPhase) phase).getPhaseFunction(), jg).write();
                    break;
                case LINK:
                    jg.writeStringField("bucket", ((LinkPhase) phase).getBucket());
                    jg.writeStringField("tag", ((LinkPhase) phase).getTag());
                    break;
                }

                jg.writeBooleanField("keep", phase.isKeep());
                jg.writeEndObject();
                jg.writeEndObject();
            }
        }
    }

    public MapReduce timeout(long timeout) {
        this.timeout = timeout;
        return this;
    }

    public MapReduce addMapPhase(Function phaseFunction, boolean keep) {
        synchronized (phases) {
            phases.add(new MapPhase(phaseFunction, keep));
        }

        return this;
    }

    public MapReduce addMapPhase(Function phaseFunction, Object arg, boolean keep) {
        synchronized (phases) {
            phases.add(new MapPhase(phaseFunction, arg, keep));
        }

        return this;
    }

    public MapReduce addMapPhase(Function phaseFunction, Object arg) {
        synchronized (phases) {
            phases.add(new MapPhase(phaseFunction, arg));
        }

        return this;
    }

    public MapReduce addMapPhase(Function phaseFunction) {
        synchronized (phases) {
            phases.add(new MapPhase(phaseFunction));
        }

        return this;
    }

    public MapReduce addReducePhase(Function phaseFunction, boolean keep) {
        synchronized (phases) {
            phases.add(new ReducePhase(phaseFunction, keep));
        }

        return this;
    }

    public MapReduce addReducePhase(Function phaseFunction, Object arg, boolean keep) {
        synchronized (phases) {
            phases.add(new ReducePhase(phaseFunction, arg, keep));
        }

        return this;
    }

    public MapReduce addReducePhase(Function phaseFunction, Object arg) {
        synchronized (phases) {
            phases.add(new ReducePhase(phaseFunction, arg));
        }

        return this;
    }

    public MapReduce addReducePhase(Function phaseFunction) {
        synchronized (phases) {
            phases.add(new ReducePhase(phaseFunction));
        }

        return this;
    }

    public MapReduce addLinkPhase(String bucket, String tag, boolean keep) {
        synchronized (phases) {
            phases.add(new LinkPhase(bucket, tag, keep));
        }

        return this;
    }

    public MapReduce addLinkPhase(String bucket, String tag) {
        synchronized (phases) {
            phases.add(new LinkPhase(bucket, tag));
        }

        return this;
    }

    protected abstract void writeInput(JsonGenerator jsonGenerator) throws IOException;
}