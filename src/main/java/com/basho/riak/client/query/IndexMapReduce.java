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

import java.io.IOException;

import org.codehaus.jackson.JsonGenerator;

import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.query.indexes.IndexQuery;
import com.basho.riak.client.raw.query.indexes.IndexWriter;

/**
 * A {@link MapReduce} operation that takes a 2i index query as input
 * 
 * @author russell
 * 
 */
public class IndexMapReduce extends MapReduce {

    private static final String BUCKET = "bucket";
    private static final String INDEX = "index";
    private static final String KEY = "key";
    private static final String START = "start";
    private static final String END = "end";

    private final IndexQuery indexQuery;

    /**
     * @param client
     */
    public IndexMapReduce(RawClient client, IndexQuery indexQuery) {
        super(client);
        this.indexQuery = indexQuery;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.basho.riak.client.query.MapReduce#writeInput(org.codehaus.jackson
     * .JsonGenerator)
     */
    @Override protected void writeInput(final JsonGenerator jsonGenerator) throws IOException {
        jsonGenerator.writeStartObject();

        IndexWriter e = new IndexWriter() {

            private void writeCommon(String bucket, String index) throws IOException {
                jsonGenerator.writeStringField(BUCKET, bucket);
                jsonGenerator.writeStringField(INDEX, index);
            }

            public void write(String bucket, String index, long from, long to) throws IOException {
                writeCommon(bucket, index);
                jsonGenerator.writeNumberField(START, from);
                jsonGenerator.writeNumberField(END, to);
            }

            public void write(String bucket, String index, long value) throws IOException {
                writeCommon(bucket, index);
                jsonGenerator.writeNumberField(KEY, value);
            }

            public void write(String bucket, String index, String from, String to) throws IOException {
                writeCommon(bucket, index);
                jsonGenerator.writeStringField(START, from);
                jsonGenerator.writeStringField(END, to);
            }

            public void write(String bucket, String index, String value) throws IOException {
                writeCommon(bucket, index);
                jsonGenerator.writeStringField(KEY, value);
            }
        };

        indexQuery.write(e);
        jsonGenerator.writeEndObject();
    }
}
