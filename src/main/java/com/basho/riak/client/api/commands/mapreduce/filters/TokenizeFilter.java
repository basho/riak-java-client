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
package com.basho.riak.client.api.commands.mapreduce.filters;


/**
 * Filter in keys that match the <code>tokenNum</code> token of the key when tokenized by <code>separator</code>
 *
 * @author russell
 */
public class TokenizeFilter extends KeyFilter implements KeyTransformFilter
{

    private static final String NAME = "tokenize";
    private final String separator;
    private final int tokens;

    /**
     * @param separator
     * @param tokens
     */
    public TokenizeFilter(String separator, int tokens)
    {
        super(NAME);
        this.separator = separator;
        this.tokens = tokens;
    }

    public String getSeparator()
    {
        return separator;
    }

    public int getTokens()
    {
        return tokens;
    }

}
