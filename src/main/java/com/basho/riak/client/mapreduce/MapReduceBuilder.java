package com.basho.riak.client.mapreduce;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.methods.PostMethod;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.basho.riak.client.RiakClient;

/**
 * Builds a map/reduce job description and submits it
 * Uses the same chained method metaphor as StringBuilder
 * or StringBuffer
 */
public class MapReduceBuilder {
   
   private static enum Types {
      MAP,
      REDUCE
   }
   
   private String bucket = null;
   private List<String[]> objects = new LinkedList<String[]>();
   private List<MapReducePhase> phases = new LinkedList<MapReducePhase>();
   private int timeout = -1;
   
   public MapReduceBuilder() {
   }
   
   /**
    * Gets the name of the Riak bucket the map/reduce job
    * will process
    */
   public String getBucket() {
      return bucket;
   }
   
   /**
    * Sets the name of the Riak bucket the map/reduce job
    * will process
    * @throws IllegalStateException - If objects have already been added to the job
    */
   public void setBucket(String newBucket) {
      if (this.phases.size() > 0) {
         throw new IllegalStateException("Cannot map/reduce over buckets and objects");
      }
      this.bucket = newBucket;
   }
   
   /**
    * Adds a Riak object (bucket name/key pair) to the map/reduce job as
    * inputs
    * @throws IllegalStateException - If a bucket name has already been set on the job
    */
   public void addRiakObject(String bucket, String key) {
      if (this.bucket != null) {
         throw new IllegalStateException("Cannot map/reduce over buckets and objects");
      }
      String[] pair = {bucket, key};
      if (!this.objects.contains(pair)) {
         this.objects.add(pair);
      }
   }
   
   /**
    * Removes a Riak object (bucket name/key pair) for the job's input list
    */
   public void removeRiakObject(String bucket, String key) {
      String[] pair = {bucket, key};
      this.objects.remove(pair);
   }
   
   /**
    * Returns a copy of the Riak objects on the input list for
    * a map/reduce job
    */
   public List<String[]> getRiakObjects() {
      return new LinkedList<String[]>(this.objects);
   }

   /**
    * Remove all Riak objects from the input list
    */
   public void clearRiakObjects() {
      this.objects.clear();
   }
   
   /**
    * How long the map/reduce job is allowed to execute
    * Time is in milliseconds
    */
   public void setTimeout(int timeout) {
      this.timeout = timeout;
   }
   
   /**
    * Gets the currently assigned timeout
    */
   public int getTimeout() {
      return this.timeout;
   }
   
   /**
    * Adds a map phase to the job
    * @param function function to run for the phase
    * @param keep should the server keep and return the results
    * @return current MapReduceBuilder instance. This is done so
    * multiple calls to map, reduce, and link can be chained together
    * a la StringBuffer
    */
   public MapReduceBuilder map(MapReduceFunction function, boolean keep) {
      this.addPhase(MapReduceBuilder.Types.MAP, function, keep);
      return this;
   }

   /**
    * Adds a reduce phase to the job
    * @param function function to run for the phase
    * @param keep should the server keep and return the results
    * @return current MapReduceBuilder instance. This is done so
    * multiple calls to map, reduce, and link can be chained together
    * a la StringBuffer
    */
   public MapReduceBuilder reduce(MapReduceFunction function, boolean keep) {
      this.addPhase(MapReduceBuilder.Types.REDUCE, function, keep);
      return this;
   }

   /**
    * Submits the job to the Riak server
    * @param riakClient RiakClient instance which is pointing to the map/reduce URL
    * @return MapReduceResponse - job results
    * @throws HttpException
    * @throws IOException
    * @throws JSONException
    */
   public MapReduceResponse submit(RiakClient riakClient) throws HttpException, IOException, JSONException {
      PostMethod method = riakClient.sendJob(this.toJSON().toString());
      return new MapReduceResponse(method.getStatusCode(), method.getStatusText(),
            method.getResponseBodyAsString());
   }
   
   private MapReduceBuilder addPhase(Types phaseType, MapReduceFunction function, boolean keep) {
      MapReducePhase phase = new MapReducePhase();
      phase.type = phaseType;
      phase.function = function;
      phase.keep = keep;
      phases.add(phase);
      return this;
   }

   
   private JSONObject toJSON() throws JSONException {
      JSONObject job = new JSONObject();
      JSONArray query = new JSONArray();
      for(MapReducePhase phase : this.phases) {
         renderPhase(phase, query);
      }
      buildInputs(job);
      job.put("query", query);
      if (this.timeout > 0) {
         job.put("timeout", this.timeout);
      }
      return job;
   }
   
   private void buildInputs(JSONObject job) throws JSONException {
      if (this.bucket != null) {
         job.put("inputs", this.bucket);
      }
      else {
         job.put("inputs", this.objects);
      }
   }
   
   private void renderPhase(MapReducePhase phase, JSONArray query) throws JSONException {
      JSONObject phaseJson = new JSONObject();
      JSONObject functionJson = phase.function.toJson();
      functionJson.put("keep", phase.keep);
      if (phase.type.equals(Types.MAP)) {
         phaseJson.put("map", functionJson);
      }
      else {
         phaseJson.put("reduce", functionJson);
      }
      query.put(phaseJson);
   }
   
   private class MapReducePhase {
      Types type;
      MapReduceFunction function;
      boolean keep;
   }

}
