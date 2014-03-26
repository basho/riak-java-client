/*
 * Copyright 2013 Basho Technologies Inc
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
package com.basho.riak.client.operations;

 /*
 * @author Dave Rusek <drusek at basho dot com>
 * @since 2.0
 */
public final class SearchOption<T> extends RiakOption<T>
{

    public static enum Operation
    {
        AND("and"), OR("or");

        final String opStr;

        Operation(String opStr)
        {
            this.opStr = opStr;
        }
    }

    public static SearchOption<Operation> DEFAULT_OPERATION = new SearchOption<Operation>("DEFAULT_OPERATION");
    public static SearchOption<String> DEFAULT_FIELD = new SearchOption<String>("DEFAULT_FIELD");

    private SearchOption(String name)
    {
        super(name);
    }


}
