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
package com.basho.riak.client.http.request;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.basho.riak.client.http.RiakClient;
import com.basho.riak.client.http.RiakObject;
import com.basho.riak.client.http.mapreduce.LinkFunction;
import com.basho.riak.client.http.mapreduce.MapReduceFunction;
import com.basho.riak.client.http.mapreduce.filter.MapReduceFilter;
import com.basho.riak.client.http.response.MapReduceResponse;
import com.basho.riak.client.http.response.RiakIORuntimeException;
import com.basho.riak.client.http.response.RiakResponseRuntimeException;

/**
 * Builds a map/reduce job description and submits it Uses the same chained
 * method metaphor as StringBuilder or StringBuffer
 */
public class MapReduceBuilder {

    private static enum Types {
        MAP, REDUCE, LINK
    }

    private String bucket = null;
    private String search = null;
    private Map<String, Set<String>> objects = new LinkedHashMap<String, Set<String>>();
    private List<MapReduceFilter> keyFilters = new ArrayList<MapReduceFilter>();
    private List<MapReducePhase> phases = new LinkedList<MapReducePhase>();
    private int timeout = -1;
    private RiakClient riak = null;

    /**
     * @param riak
     *            RiakClient instance which is pointing to the map/reduce URL
     */
    public MapReduceBuilder(RiakClient riak) {
        this.riak = riak;
    }

    public MapReduceBuilder() { /* nop */ }

    /**
     * The {@link RiakClient} to which this map reduce job will be submitted to
     * when {@link MapReduceBuilder#submit()} is called.
     */
    public RiakClient getRiakClient() {
        return riak;
    }

    public MapReduceBuilder setRiakClient(RiakClient client) {
        riak = client;
        return this;
    }

    /**
     * Gets the name of the Riak bucket the map/reduce job will process
     */
    public String getBucket() {
        return bucket;
    }

    /**
     * Sets the name of the Riak bucket the map/reduce job will process
     * 
     * @throws IllegalStateException
     *             - If objects have already been added to the job
     */
    public MapReduceBuilder setBucket(String newBucket) {
        if (objects.size() > 0)
            throw new IllegalStateException("Cannot map/reduce over buckets and objects");
        bucket = newBucket;
        return this;
    }

    /**
     * Gets the search query the map/reduce job will process
     */
    public String getSearch() {
        return search;
    }

    /**
     * Sets the search query the map/reduce job will process
     * 
     * @throws IllegalStateException
     *             - If objects or bucket has already been added
     */
    public MapReduceBuilder setSearch(String search) {
        if (objects.size() > 0)
            throw new IllegalStateException("Cannot map/reduce over objects and search");
        if (this.keyFilters.size() > 0)
            throw new IllegalStateException("Cannot combine keyfilters and search");
        this.search = search;
        return this;
    }

    /**
     * Adds a Riak object (bucket name/key pair) to the map/reduce job as inputs
     * 
     * @throws IllegalStateException
     *             - If a bucket name has already been set on the job
     */
    public void addRiakObject(String bucket, String key) {
        if (search != null)
            throw new IllegalStateException("Cannot map/reduce over objects and search");
        if (this.bucket != null)
            throw new IllegalStateException("Cannot map/reduce over buckets and objects");
        Set<String> keys = objects.get(bucket);
        if (keys == null) {
            keys = new LinkedHashSet<String>();
            objects.put(bucket, keys);
        }
        keys.add(key);
    }

    /**
     * Removes a Riak object (bucket name/key pair) for the job's input list
     */
    public void removeRiakObject(String bucket, String key) {
        Set<String> keys = objects.get(bucket);
        if (keys != null) {
            keys.remove(key);
            if (keys.size() == 0) {
                objects.remove(bucket);
            }
        }
    }

    /**
     * Returns a copy of the Riak objects on the input list for a map/reduce job
     */
    public Map<String, Set<String>> getRiakObjects() {
        return new HashMap<String, Set<String>>(objects);
    }

    /**
     * Sets a collection of Riak object (bucket name/key pair) as the map/reduce
     * job as inputs
     * 
     * @throws IllegalStateException
     *             - If a bucket name has already been set on the job
     */
    public MapReduceBuilder setRiakObjects(Map<String, Set<String>> objects) {
        if (search != null)
            throw new IllegalStateException("Cannot map/reduce over objects and search");
        if (bucket != null)
            throw new IllegalStateException("Cannot map/reduce over buckets and objects");

        if (objects == null) {
            clearRiakObjects();
        } else {
            this.objects = new HashMap<String, Set<String>>(objects);
        }

        return this;
    }

    public MapReduceBuilder setRiakObjects(Collection<RiakObject> objects) {
        if (search != null)
            throw new IllegalStateException("Cannot map/reduce over objects and search");
        if (bucket != null)
            throw new IllegalStateException("Cannot map/reduce over buckets and objects");

        clearRiakObjects();
        if (objects != null) {
            for (RiakObject o : objects) {
                addRiakObject(o.getBucket(), o.getKey());
            }
        }

        return this;
    }

    /**
     * Remove all Riak objects from the input list
     */
    public void clearRiakObjects() {
        objects.clear();
    }

    /**
     * How long the map/reduce job is allowed to execute Time is in milliseconds
     */
    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    /**
     * Gets the currently assigned timeout
     */
    public int getTimeout() {
        return timeout;
    }

    /**
     * Adds a map phase to the job
     * 
     * @param function
     *            function to run for the phase
     * 
     * @param keep
     *            should the server keep and return the results
     * @return current MapReduceBuilder instance. This is done so multiple calls
     *         to map, reduce, and link can be chained together a la
     *         StringBuffer
     */
    public MapReduceBuilder keyFilter(MapReduceFilter... filters) {
        if (search != null)
            throw new IllegalStateException("Cannot map/reduce over objects and search");
       for(MapReduceFilter filter: filters) {
          this.keyFilters.add(filter);
       }
       return this;
    }

    /**
     * Adds a map phase to the job
     * 
     * @param function
     *            function to run for the phase
     * 
     * @param keep
     *            should the server keep and return the results
     * @return current MapReduceBuilder instance. This is done so multiple calls
     *         to map, reduce, and link can be chained together a la
     *         StringBuffer
     */
    public MapReduceBuilder map(MapReduceFunction function, boolean keep) {
       return this.map(function, null, keep);
    }


    /**
     * Adds a map phase to the job
     * 
     * @param function
     *            function to run for the phase
     * 
     * @param arg
     *            Static argument to pass to the function. Should be an
     *            object easily converted to JSON
     *            
     * @param keep
     *            should the server keep and return the results
     * @return current MapReduceBuilder instance. This is done so multiple calls
     *         to map, reduce, and link can be chained together a la
     *         StringBuffer
     */
    public MapReduceBuilder map(MapReduceFunction function, Object arg, boolean keep) {
        this.addPhase(MapReduceBuilder.Types.MAP, function, arg, keep);
        return this;
    }
    
    /**
     * Adds a reduce phase to the job
     * 
     * @param function
     *            function to run for the phase
     *            
     * @param keep
     *            should the server keep and return the results
     * @return current MapReduceBuilder instance. This is done so multiple calls
     *         to map, reduce, and link can be chained together a la
     *         StringBuffer
     */
    public MapReduceBuilder reduce(MapReduceFunction function, boolean keep) {
       return this.reduce(function, null, keep);
    }


    /**
     * Adds a reduce phase to the job
     * 
     * @param function
     *            function to run for the phase
     *            
     * @param arg
     *            Static argument to pass to the function. Should be an
     *            object easily converted to JSON
     *            
     * @param keep
     *            should the server keep and return the results
     * @return current MapReduceBuilder instance. This is done so multiple calls
     *         to map, reduce, and link can be chained together a la
     *         StringBuffer
     */
    public MapReduceBuilder reduce(MapReduceFunction function, Object arg, boolean keep) {
        this.addPhase(MapReduceBuilder.Types.REDUCE, function, arg, keep);
        return this;
    }

    /**
     * Adds a link phase to the job
     * 
     * @param bucket
     *            bucket to link walk
     * @param keep
     *            should the server keep and return the results
     * @return current MapReduceBuilder instance. This is done so multiple calls
     *         to map, reduce, and link can be chained together a la
     *         StringBuffer
     * 
     *         Pointing at a bucket without specifying a link tag will follow
     *         all links pointing to objects in the bucket
     */
    public MapReduceBuilder link(String bucket, boolean keep) {
        this.addPhase(MapReduceBuilder.Types.LINK, new LinkFunction(bucket), keep);
        return this;
    }

    /**
     * Adds a link phase to the job
     * 
     * @param bucket
     *            bucket to link walk
     * @param tag
     *            link tag to match
     * @param keep
     *            should the server keep and return the results
     * @return current MapReduceBuilder instance. This is done so multiple calls
     *         to map, reduce, and link can be chained together a la
     *         StringBuffer
     */
    public MapReduceBuilder link(String bucket, String tag, boolean keep) {
        this.addPhase(MapReduceBuilder.Types.LINK, new LinkFunction(bucket, tag), keep);
        return this;
    }

    /**
     * Submits the job to the Riak server
     * 
     * @param meta
     *            Extra metadata to attach to the request such as HTTP headers
     *            or query parameters.
     * 
     * @return {@link MapReduceResponse} containing job results
     * 
     * @throws IllegalStateException
     *             If this job has not been associated with a Riak instance by
     *             calling {@link MapReduceBuilder#setRiakClient(RiakClient)}
     * @throws RiakIORuntimeException
     *             If an error occurs during communication with the Riak server.
     * @throws RiakResponseRuntimeException
     *             If the Riak server returns a malformed response.
     */
    public MapReduceResponse submit(RequestMeta meta) {
        if (riak == null)
            throw new IllegalStateException("Cannot perform map reduce without a RiakClient");
        return riak.mapReduce(toJSON().toString(), meta);
    }

    public MapReduceResponse submit() throws JSONException {
        return submit(null);
    }

    /**
     * Builds the JSON representation of a map/reduce job
     */
    public JSONObject toJSON() {
        JSONObject job = new JSONObject();
        JSONArray query = new JSONArray();
        
        for (MapReducePhase phase : phases) {
            renderPhase(phase, query);
        }
        buildInputs(job);
        try {
            job.put("query", query);
        } catch (JSONException e) {
            throw new RuntimeException("Can always map a string to a valid JSONArray");
        }
        if (timeout > 0) {
            try {
                job.put("timeout", timeout);
            } catch (JSONException e) {
                throw new RuntimeException("Can always map a string to an int");
            }
        }
        return job;
    }
    
    private MapReduceBuilder addPhase(Types phaseType, MapReduceFunction function, boolean keep) {
       return addPhase(phaseType, function, null, keep);
    }

    private MapReduceBuilder addPhase(Types phaseType, MapReduceFunction function, Object arg, boolean keep) {
       MapReducePhase phase = new MapReducePhase();
       phase.type = phaseType;
       phase.function = function;
       phase.arg = arg;
       phase.keep = keep;
       phases.add(phase);
       return this;
    }
    
    private JSONArray buildFilters(List<MapReduceFilter> filterList) {
        JSONArray filters = new JSONArray();
        for(MapReduceFilter filter: filterList) {
            filters.put(filter.toJson());
        }
        return filters;
    }
    
    private void buildInputs(JSONObject job) {
        if (search != null) {
            try {
                JSONObject jobInputs = new JSONObject();
                jobInputs.put("module", "riak_search");
                jobInputs.put("function", "mapred_search");
                JSONArray jobArgs = new JSONArray();
                jobArgs.put(bucket);
                jobArgs.put(search);
                jobInputs.put("arg", jobArgs);
                job.put("inputs", jobInputs);
            } catch (JSONException e) {
                throw new RuntimeException("Can always assemble a query");
            }
        } else if (bucket != null) {
            if (keyFilters.size() > 0) {
                try {
                    JSONObject jobInputs = new JSONObject();
                    jobInputs.put("bucket", bucket);
                    jobInputs.put("key_filters", buildFilters(this.keyFilters));
                    job.put("inputs", jobInputs);
                } catch (JSONException e) {
                    throw new RuntimeException("Can always map a collection of MapReduceFilter objects to a JSONArray");
                }
            } else {
                try {
                    job.put("inputs", bucket);
                } catch (JSONException e) {
                    throw new RuntimeException("Can always map a string to a string");
                }
            }
        } else {
            JSONArray inputs = new JSONArray();
            for (String bucket : objects.keySet()) {
                Set<String> keys = objects.get(bucket);
                for (String key : keys) {
                    String[] pair = { bucket, key };
                    inputs.put(pair);
                }
            }
            try {
                job.put("inputs", inputs);
            } catch (JSONException e) {
                throw new RuntimeException("Can always map a string to a valid JSONArray");
            }
        }
    }

    private void renderPhase(MapReducePhase phase, JSONArray query) {
        JSONObject phaseJson = new JSONObject();
        JSONObject functionJson = phase.function.toJson();
        try {
            functionJson.put("keep", phase.keep);
        } catch (JSONException e) {
            throw new RuntimeException("Can always map a string to a boolean");
        }
        try {
           if (phase.arg != null) {
              functionJson.put("arg", phase.arg);
           }
        } catch (JSONException e) {
           throw new RuntimeException("Cannot convert phase arg to JSON");
        }
        String type = null;
        switch (phase.type) {
        case MAP:
            type = "map";
            break;
        case REDUCE:
            type = "reduce";
            break;
        case LINK:
            type = "link";
            break;
        }
        try {
            phaseJson.put(type, functionJson);
        } catch (JSONException e) {
            throw new RuntimeException("Can always map a string to a valid JSONObject");
        }
        query.put(phaseJson);
    }

    private class MapReducePhase {
        Types type;
        MapReduceFunction function;
        Object arg;
        boolean keep;
    }

}
