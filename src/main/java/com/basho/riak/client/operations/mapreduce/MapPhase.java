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
package com.basho.riak.client.operations.mapreduce;

import com.basho.riak.client.query.functions.Function;

/**
 * A Map Phase of a Map/Reduce job spec.
 * 
 * @author russell
 * 
 */
public class MapPhase implements MapReducePhase {

    private final Function phaseFunction;
    private final Boolean keep;
    private final Object arg; // TODO object? you sure?

    /**
     * Create a MapPhase
     * 
     * @param phaseFunction
     *            the {@link Function}
     * @param arg
     *            an argument that will be passed to the phase verbatim
     *            (Object#toString)
     * @param keepResult
     *            if the result should be returned or merely provide input for
     *            the next phase.
     * 
     * @see MapReduce#addMapPhase(Function, Object, boolean)
     */
    public MapPhase(Function phaseFunction, Object arg, boolean keepResult) {
        this.phaseFunction = phaseFunction;
        this.arg = arg;
        this.keep = keepResult;
    }

    /**
     * Create a MapPhase
     * 
     * @param phaseFunction
     *            the {@link Function}
     * @param arg
     *            an argument that will be passed to the phase verbatim
     *            (Object#toString)
     * @see MapReduce#addMapPhase(Function, Object)
     */
    public MapPhase(Function phaseFunction, Object arg) {
        this.phaseFunction = phaseFunction;
        this.arg = arg;
        this.keep = null;
    }
    
    /**
     * Create a MapPhase
     * 
     * @param phaseFunction
     *            the {@link Function}
     * 
     * @see MapReduce#addMapPhase(Function)
     */
    public MapPhase(Function phaseFunction) {
        this.phaseFunction = phaseFunction;
        this.arg = null;
        this.keep = null;
    }
    
    /**
     * Create a MapPhase
     * 
     * @param phaseFunction
     *            the {@link Function}
     * @param keepResult
     *            if the result should be returned or merely provide input for
     *            the next phase.
     * 
     * @see MapReduce#addMapPhase(Function, Object, boolean)
     */
    public MapPhase(Function phaseFunction, boolean keep) {
        this.phaseFunction = phaseFunction;
        this.arg = null;
        this.keep = keep;
    }

    /**
     * @return the phaseFunction
     */
    public Function getPhaseFunction() {
        return phaseFunction;
    }

    /**
     * @return the keep
     */
    public Boolean isKeep() {
        return keep;
    }

    /**
     * @return the arg
     */
    public Object getArg() {
        return arg;
    }
    
    public static MapPhase map(Function function, Object arg, boolean keep) {
        return new MapPhase(function, arg, keep);
    }

    /* (non-Javadoc)
     * @see com.basho.riak.newapi.query.MapReducePhase#getType()
     */
    public PhaseType getType() {
        return PhaseType.MAP;
    }
}
