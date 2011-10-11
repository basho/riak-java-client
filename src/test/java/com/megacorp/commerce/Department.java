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
package com.megacorp.commerce;

import java.util.Collection;

import org.codehaus.jackson.annotate.JsonProperty;

import com.basho.riak.client.RiakLink;
import com.basho.riak.client.convert.RiakKey;
import com.basho.riak.client.convert.RiakLinks;

/**
 * @author russell
 * 
 */
public class Department {

    @RiakKey private final String deptId;

    private String name;
    @RiakLinks private Collection<RiakLink> employees;

    /**
     * @param deptId
     */
    public Department(@JsonProperty("deptId") String deptId) {
        this.deptId = deptId;
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @param name
     *            the name to set
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return the employees
     */
    public Collection<RiakLink> getEmployees() {
        return employees;
    }

    /**
     * @param employees
     *            the employees to set
     */
    public void setEmployees(Collection<RiakLink> employees) {
        this.employees = employees;
    }

    /**
     * @return the deptId
     */
    public String getDeptId() {
        return deptId;
    }

}
