package com.basho.riak.client.operations.mapreduce;

import com.basho.riak.client.query.functions.Function;

class PhaseFunction
{

	private final Function function;
	private final boolean keep;

	public PhaseFunction(Function function, boolean keep)
	{
		this.function = function;
		this.keep = keep;
	}

	public Function getFunction()
	{
		return function;
	}

	public boolean isKeep()
	{
		return keep;
	}

}
