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
package com.basho.riak.client.api.commands.mapreduce;

import com.basho.riak.client.core.query.functions.Function;

/**
 * A reduce phase of a MapReduce job spec. Just a tag class.
 *
 * @author russell
 */
class ReducePhase extends FunctionPhase
{

    /**
     * Create a Reduce Phase
     *
     * @param phaseFunction the function to run for this reduce phase
     * @param arg any arguments to pass to this reduce phase
     * @param keepResult the option to keep the results of this reduce phase for the final result set
     */
    public ReducePhase(Function phaseFunction, Object arg, boolean keepResult)
    {
        super(PhaseType.REDUCE, phaseFunction, arg, keepResult);
    }

    /**
     * Create a Reduce Phase
     *
     * @param phaseFunction the function to run for this reduce phase
     * @param arg any arguments to pass to this reduce phase
     */
    public ReducePhase(Function phaseFunction, Object arg)
    {
        this(phaseFunction, arg, false);
    }

    /**
     * Create a Reduce Phase
     *
     * @param phaseFunction the function to run for this reduce phase
     * @param keep the option to keep the results of this reduce phase for the final result set
     */
    public ReducePhase(Function phaseFunction, boolean keep)
    {
        this(phaseFunction, null, keep);
    }

    /**
     * Create a Reduce Phase
     *
     * @param phaseFunction the function to run for this reduce phase
     */
    public ReducePhase(Function phaseFunction)
    {
        this(phaseFunction, null, false);
    }

}
