package com.basho.riak.client.operations;

import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.query.filter.KeyFilter;
import com.basho.riak.client.query.functions.Function;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class BucketMapReduce extends RiakCommand<BucketMapReduce.Response>
{

	public BucketMapReduce(Builder builder)
	{

	}

	@Override
	public Response execute(RiakCluster cluster) throws ExecutionException, InterruptedException
	{
		return null;
	}
	
	public static class Response
	{

	}
	
	public static class Builder 
	{

		private final List<KeyFilter> filters = new ArrayList<KeyFilter>();
		private final List<Phase> mapPhase = new ArrayList<Phase>();
		private final List<Phase> reducePhase = new ArrayList<Phase>();

		public Builder addKeyFilter(KeyFilter filter)
		{
			this.filters.add(filter);
			return this;
		}

		public BucketMapReduce build()
		{
			return new BucketMapReduce(this);
		}
		
	}

	private static class Phase
	{
	 	public final Function function;
		public final boolean isKeep;

		private Phase(Function function, boolean isKeep)
		{
			this.function = function;
			this.isKeep = isKeep;
		}
	}
}
