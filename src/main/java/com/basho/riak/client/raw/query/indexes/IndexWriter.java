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
package com.basho.riak.client.raw.query.indexes;

import java.io.IOException;

/**
 * Provide {@link IndexQuery}s with a way to write themselves.
 * 
 * Specifically, it provides a way to pass some output capturing object to the
 * concrete {@link IndexQuery} type as each {@link IndexQuery} knows best how to
 * 'render' itself. This interface provides a way for objects to adapt
 * themselves to queries
 * 
 * @author russell
 * 
 */
public interface IndexWriter {

    void write(String bucket, String index, String value) throws IOException;

    void write(String bucket, String index, String from, String to) throws IOException;

    void write(String bucket, String index, int value) throws IOException;

    void write(String bucket, String index, int from, int to) throws IOException;
}
