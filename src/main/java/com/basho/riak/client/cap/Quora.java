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
package com.basho.riak.client.cap;

/**
 * An enum that models the set of symbolic quora.
 * 
 * @author russell
 * 
 */
public enum Quora {
    ALL(Integer.MIN_VALUE, "all"), ONE(Integer.MIN_VALUE + 1, "one"), QUORUM(Integer.MIN_VALUE + 2, "quorum"), DEFAULT(
            Integer.MIN_VALUE + 3, "default"), INTEGER(Integer.MIN_VALUE + 4, "int");

    private final int value;
    private final String name;

    private Quora(int value, String name) {
        this.value = value;
        this.name = name;
    }

    /**
     * @return the value
     */
    public int getValue() {
        return value;
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * Attempt to return a {@link Quora} for the given <code>name</code>
     * @param name
     * @return a {@link Quora} for <code>name</name>
     * @throws IllegalArgumentException is <code>name</code> is not a valid Quora.
     */
    public static Quora fromString(String name) {
        for(Quora q : values()) {
            if(q.getName().equals(name)) {
                return q;
            }
        }
        throw new IllegalArgumentException(name + " is not a known value for a Quorum");
    }
}
