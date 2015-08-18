package com.basho.riak.client.api.commands.mapreduce;

import com.basho.riak.client.core.query.functions.Function;

/**
 *  A MapReducePhase that runs a known function for the computation.
 */
class FunctionPhase extends MapReducePhase
{
    final PhaseFunction phaseFunction;
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
    public FunctionPhase(PhaseType type, Function phaseFunction, Object arg, boolean keepResult)
    {
        super(type);
        this.phaseFunction = new PhaseFunction(phaseFunction, keepResult);
        this.arg = arg;
    }

    /**
     * Gets the function to run during the phase.
     * @return the phaseFunction
     */
    public Function getFunction()
    {
        return phaseFunction.getFunction();
    }

    /**
     * Gets the keep option for the phase.
     * @return the keep
     */
    public Boolean isKeep()
    {
        return phaseFunction.isKeep();
    }

    /**
     * Gets the arg (if any), to pass into the function.
     * @return the arg
     */
    public Object getArg()
    {
        return arg;
    }



}
