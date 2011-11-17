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
package com.basho.riak.test.util;

import junit.framework.Assert;

/**
 * Riak HTTP server munges the content-type value (lowercase it, removes the
 * space between the field separator and value). However, 2 content-types are
 * still semantically equal if they're equal at lower case with spaces removed.
 * This does that.
 * 
 * @author russell
 * 
 */
public class ContentTypesEqual {

    public static void equal(String expected, String actual) {
        if (expected == null && actual == null) {
            return;
        }
        if (expected == null || actual == null) {
            Assert.assertEquals(expected, actual);
        }
        expected = expected.toLowerCase().replaceAll("\\s", "");
        actual = actual.toLowerCase().replaceAll("\\s", "");

        Assert.assertEquals(expected, actual);
    }

}
