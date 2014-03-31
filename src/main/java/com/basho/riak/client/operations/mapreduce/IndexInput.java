package com.basho.riak.client.operations.mapreduce;

import com.basho.riak.client.query.Location;

public class IndexInput implements MapReduceInput
{
	private final Location bucket;
	private final String index;
	private final IndexCriteria criteria;

	public IndexInput(Location bucket, String index, IndexCriteria criteria)
	{
		this.bucket = bucket;
		this.index = index;
		this.criteria = criteria;
	}

	public Location getBucket()
	{
		return bucket;
	}

	public String getIndex()
	{
		return index;
	}

	public IndexCriteria getCriteria()
	{
		return criteria;
	}

	static interface IndexCriteria
	{
	}

	static class RangeCriteria<V> implements IndexCriteria
	{

		private final V begin;
		private final V end;

		public RangeCriteria(V begin, V end)
		{
			this.begin = begin;
			this.end = end;
		}

		public V getBegin()
		{
			return begin;
		}

		public V getEnd()
		{
			return end;
		}

	}

	static class MatchCriteria<V> implements IndexCriteria
	{

		private final V value;

		public MatchCriteria(V value)
		{
			this.value = value;
		}

		public V getValue()
		{
			return value;
		}

	}
}
