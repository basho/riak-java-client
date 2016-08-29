package com.basho.riak.client.api.commands.mapreduce;

import com.basho.riak.client.core.query.functions.Function;

/**
 * A Phase-Function model. Groups a {@see Function} and a keep flag together.
 */
class PhaseFunction
{

    private final Function function;
    private final boolean keep;

    /**
     * Create a new PhaseFunction.
     * @param function the Function to use for this phase
     * @param keep the option to keep the Function's output for the final result set, or just for the next phase.
     */
    public PhaseFunction(Function function, boolean keep)
    {
        this.function = function;
        this.keep = keep;
    }

    /**
     * Gets the Function.
     * @return the Function
     */
    public Function getFunction()
    {
        return function;
    }

    /**
     * Gets the keep flag.
     * @return the keep flag
     */
    public boolean isKeep()
    {
        return keep;
    }

}
