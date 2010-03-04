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
import com.basho.riak.client.RiakObject;
import com.basho.riak.client.mapreduce.response.MapReduceResponse;

public class MapReduceBuilder {
   
   private static enum Types {
      MAP,
      REDUCE
   }
   
   private String bucket = null;
   private List<String[]> objects = new LinkedList<String[]>();
   private List<MapReducePhase> phases = new LinkedList<MapReducePhase>();
   private int timeout = -1;
   
   public String getBucket() {
      return bucket;
   }
   
   public void setBucket(String newBucket) {
      this.bucket = newBucket;
   }
   
   public void addRiakObject(String bucket, String key) {
      if (this.bucket != null) {
         throw new IllegalStateException("Cannot map/reduce over buckets and objects");
      }
      String[] pair = {bucket, key};
      if (!this.objects.contains(pair)) {
         this.objects.add(pair);
      }
   }
   
   public void removeRiakObject(String bucket, String key) {
      String[] pair = {bucket, key};
      this.objects.remove(pair);
   }
   
   public List<String[]> getRiakObjects() {
      return new LinkedList<String[]>(this.objects);
   }
   
   public void clearRiakObjects() {
      this.objects.clear();
   }
   
   public void setTimeout(int timeout) {
      this.timeout = timeout;
   }
   
   public int getTimeout() {
      return this.timeout;
   }
   
   public MapReduceBuilder map(MapReduceFunction function, boolean keep) {
      this.addPhase(MapReduceBuilder.Types.MAP, function, keep);
      return this;
   }
   
   public MapReduceBuilder reduce(MapReduceFunction function, boolean keep) {
      this.addPhase(MapReduceBuilder.Types.REDUCE, function, keep);
      return this;
   }

      
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
