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
package com.basho.riak.client.query.functions;

import org.codehaus.jackson.annotate.JsonProperty;

/**
 * An M/R function arg that mandates that Riak only run the reduce phase once,
 * when all maps are finished.
 * 
 * @author russell
 * 
 */
public class ReducePhaseOnlyOne {
    private final boolean reducePhaseOnlyOne;

    /**
     * @param reducePhaseOnlyOne
     */
    public ReducePhaseOnlyOne(boolean reducePhaseOnlyOne) {
        this.reducePhaseOnlyOne = reducePhaseOnlyOne;
    }

    /**
     * @return the reducePhaseOnlyOne
     */
    @JsonProperty("reduce_phase_only_1") public boolean isReducePhaseOnlyOne() {
        return reducePhaseOnlyOne;
    }
}
