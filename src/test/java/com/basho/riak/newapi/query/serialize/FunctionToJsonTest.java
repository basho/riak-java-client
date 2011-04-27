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
package com.basho.riak.newapi.query.serialize;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.codehaus.jackson.JsonGenerator;
import org.junit.Test;

import com.basho.riak.newapi.query.functions.Function;
import com.basho.riak.newapi.query.functions.JSBucketKeyFunction;
import com.basho.riak.newapi.query.functions.JSSourceFunction;
import com.basho.riak.newapi.query.functions.NamedErlangFunction;
import com.basho.riak.newapi.query.functions.NamedJSFunction;

/**
 * @author russell
 * 
 */
public class FunctionToJsonTest {

    /**
     * Tests that the static factory method returns a writer appropriate to the
     * function type passed.
     * 
     * @throws Exception
     */
    @Test public void correctWriterForFunctionType() throws Exception {
        final JsonGenerator jsonGenerator = mock(JsonGenerator.class);

        FunctionWriter fw = FunctionToJson.newWriter(new NamedErlangFunction("mod", "string"), jsonGenerator);
        assertTrue(fw instanceof NamedErlangFunctionWriter);
        fw.write();
        verify(jsonGenerator, atLeastOnce()).writeStringField(any(String.class), any(String.class));
        reset(jsonGenerator);

        fw = FunctionToJson.newWriter(new JSBucketKeyFunction("b", "k"), jsonGenerator);
        assertTrue(fw instanceof JSBucketKeyFunctionWriter);
        fw.write();
        verify(jsonGenerator, atLeastOnce()).writeStringField(any(String.class), any(String.class));
        reset(jsonGenerator);

        fw = FunctionToJson.newWriter(new JSSourceFunction("function(x) { alert(\"mooooo!\"); }"), jsonGenerator);
        assertTrue(fw instanceof JSSourceFunctionWriter);
        fw.write();
        verify(jsonGenerator, atLeastOnce()).writeStringField(any(String.class), any(String.class));
        reset(jsonGenerator);

        fw = FunctionToJson.newWriter(new NamedJSFunction("Riak.mapJson"), jsonGenerator);
        assertTrue(fw instanceof NamedJSFunctionWriter);
        fw.write();
        verify(jsonGenerator, atLeastOnce()).writeStringField(any(String.class), any(String.class));

        try {
            fw = FunctionToJson.newWriter(new Function() {}, jsonGenerator);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // NO-OP
        }
    }

}
