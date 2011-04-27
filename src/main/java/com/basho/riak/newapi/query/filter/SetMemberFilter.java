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
package com.basho.riak.newapi.query.filter;

import java.util.Set;

import org.json.JSONException;

public class SetMemberFilter implements KeyFilter {

    private static final String NAME = "set_member";
    private final Object[] filter;

    
    public SetMemberFilter(String...setMembers) {
        filter = new String[setMembers.length + 1];
        int cnt = 0;
        filter[cnt] = NAME;

        for (String setMember : setMembers) {
            filter[++cnt] = setMember;
        }
    }
    
    public SetMemberFilter(Set<String> setMembers) {
        filter = new String[setMembers.size() + 1];
        int cnt = 0;
        filter[cnt] = NAME;

        for (String setMember : setMembers) {
            filter[++cnt] = setMember;
        }
    }

    public SetMemberFilter(int[] setMembers) {
        filter = new Object[setMembers.length + 1];
        int cnt = 0;
        filter[cnt] = NAME;

        for (int setMember : setMembers) {
            filter[++cnt] = setMember;
        }
    }

    public SetMemberFilter(double[] setMembers) throws JSONException {
        filter = new Object[setMembers.length + 1];
        int cnt = 0;
        filter[cnt] = NAME;

        for (double setMember : setMembers) {
            filter[++cnt] = setMember;
        }
    }

    public Object[] asArray() {
        return filter.clone();
    }
}
