/*
 * Copyright 2013 Basho Technologies Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.basho.riak.client.api.cap;

import java.io.Serializable;

/**
 * Access the opaque Riak vector clock as either a String or array of bytes.
 *
 * @author Russel Brown <russelldb at basho dot com>
 * @since 1.0
 */
public interface VClock extends Serializable
{
    /**
     * Get the bytes that make up this VClock.
     * @return a copy of this vector clocks bytes
     */
    byte[] getBytes();

    /**
     * Get the string representation of this VClock.
     * @return a UTF-8 String of this vector clocks bytes
     */
    String asString();
}
