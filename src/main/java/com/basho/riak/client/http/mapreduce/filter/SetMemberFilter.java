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
package com.basho.riak.client.http.mapreduce.filter;

import java.util.List;

import org.json.JSONException;
import org.json.JSONArray;

public class SetMemberFilter implements MapReduceFilter {
    private static final String NAME = "set_member";
    private MapReduceFilter.Types type = MapReduceFilter.Types.FILTER;
    private JSONArray args = new JSONArray();
    
    public SetMemberFilter(List<String> setMembers) {
        args.put("set_member");
        for(String setMember: setMembers) {
            args.put(setMember);
        }
    }
    
    public SetMemberFilter(JSONArray setMembers) throws JSONException {
        args.put("set_member");
        for(int i=0; i<setMembers.length(); i++) {
            args.put(setMembers.get(i));
        }
    }
    
    public SetMemberFilter(int[] setMembers) {
        args.put("set_member");
        for(int setMember: setMembers) {
            args.put(setMember);
        }
    }

    public SetMemberFilter(double[] setMembers) throws JSONException {
        args.put("set_member");
        for(double setMember: setMembers) {
            args.put(setMember);
        }
    }

    public JSONArray toJson() {
        return args;
    }
}
