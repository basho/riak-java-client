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
 * A Map Phase of a Map/Reduce job spec.
 *
 * @author russell
 */
class MapPhase extends FunctionPhase
{
    /**
     * Create a MapPhase
     *
     * @param phaseFunction the {@link Function}
     * @param arg           an argument that will be passed to the phase verbatim
     *                      (Object#toString)
     * @param keepResult    if the result should be returned or merely provide input for
     *                      the next phase.
     */
    public MapPhase(Function phaseFunction, Object arg, boolean keepResult)
    {
        super(PhaseType.MAP, phaseFunction, arg, keepResult);
    }

    /**
     * Create a MapPhase
     *
     * @param phaseFunction the {@link Function}
     * @param arg           an argument that will be passed to the phase verbatim
     *                      (Object#toString)
     */
    public MapPhase(Function phaseFunction, Object arg)
    {
        this(phaseFunction, arg, false);
    }

    /**
     * Create a MapPhase
     *
     * @param phaseFunction the {@link Function}
     */
    public MapPhase(Function phaseFunction)
    {
        this(phaseFunction, null, false);
    }

    /**
     * Create a MapPhase
     *
     * @param phaseFunction the {@link Function}
     * @param keep          if the result should be returned or merely provide input for
     *                      the next phase.
     */
    public MapPhase(Function phaseFunction, boolean keep)
    {
        this(phaseFunction, null, keep);
    }
}
