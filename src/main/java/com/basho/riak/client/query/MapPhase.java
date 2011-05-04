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
package com.basho.riak.client.query;

import com.basho.riak.client.query.functions.Function;

/**
 * A Map Phase of a Map/Reduce job spec.
 * 
 * @author russell
 * 
 */
public class MapPhase implements MapReducePhase {

    private final Function phaseFunction;
    private final boolean keep;
    private final Object arg; // TODO object? you sure?

    /**
     * @param phaseFunction
     * @param arg
     * @param keepResult
     */
    public MapPhase(Function phaseFunction, Object arg, boolean keepResult) {
        this.phaseFunction = phaseFunction;
        this.arg = arg;
        this.keep = keepResult;
    }

    /**
     * @param phaseFunction
     * @param arg
     */
    public MapPhase(Function phaseFunction, Object arg) {
        this.phaseFunction = phaseFunction;
        this.arg = arg;
        this.keep = false;
    }
    
    /**
     * @param phaseFunction
     * @param arg
     */
    public MapPhase(Function phaseFunction) {
        this.phaseFunction = phaseFunction;
        this.arg = null;
        this.keep = false;
    }
    
    /**
     * @param phaseFunction
     * @param arg
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
    public boolean isKeep() {
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
