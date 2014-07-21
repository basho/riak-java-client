package com.basho.riak.client.api.commands.mapreduce;

import com.basho.riak.client.core.query.functions.Function;

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
