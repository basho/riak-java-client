package com.basho.riak.client.operations.mapreduce;

import com.basho.riak.client.query.functions.Function;

class FunctionPhase extends MapReducePhase
{
	private final Function phaseFunction;
	private final Boolean keep;
	private final Object arg; // TODO object? you sure?

	/**
	 * Create a Phase containing a function
	 *
	 * @param phaseFunction the {@link Function}
	 * @param arg           an argument that will be passed to the phase verbatim
	 *                      (Object#toString)
	 * @param keepResult    if the result should be returned or merely provide input for
	 *                      the next phase.
	 */
	public FunctionPhase(PhaseType type, Function phaseFunction, Object arg, Boolean keepResult)
	{
		super(type);
		this.phaseFunction = phaseFunction;
		this.arg = arg;
		this.keep = keepResult;
	}

	/**
	 * @return the phaseFunction
	 */
	public Function getPhaseFunction()
	{
		return phaseFunction;
	}

	/**
	 * @return the keep
	 */
	public Boolean isKeep()
	{
		return keep;
	}

	/**
	 * @return the arg
	 */
	public Object getArg()
	{
		return arg;
	}

}
