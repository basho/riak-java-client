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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.basho.riak.client.convert.RiakIndex;
import com.basho.riak.client.convert.RiakKey;
import com.basho.riak.client.convert.RiakUsermeta;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * An other example entity, this time with user meta data
 * 
 * @author russell
 * 
 */
public class Customer {

    @RiakKey private final String userId;
    private String username;

    
    @JsonProperty @RiakIndex(name = "email") private String emailAddress;
    @JsonProperty @RiakIndex(name = "shoe-size") private int shoeSize;

    @RiakUsermeta(key = "language-pref") private String languageCode;

    @RiakUsermeta private final Map<String, String> preferences = new HashMap<String, String>();

    /**
     * @param userId
     */
    public Customer(@JsonProperty("userId") String userId) {
        this.userId = userId;
    }

    /**
     * @return the userId
     */
    public synchronized String getUserId() {
        return userId;
    }

    /**
     * @return the username
     */
    public String getUsername() {
        return username;
    }

    /**
     * @param username
     *            the username to set
     */
    public void setUsername(String username) {
        this.username = username;
    }

    /**
     * @return the emailAddress
     */
    public String getEmailAddress() {
        return emailAddress;
    }

    /**
     * @param emailAddress
     *            the emailAddress to set
     */
    public void setEmailAddress(String emailAddress) {
        this.emailAddress = emailAddress;
    }

    /**
     * @return the shoeSize
     */
    public int getShoeSize() {
        return shoeSize;
    }

    /**
     * @param shoeSize
     *            the shoeSize to set
     */
    public void setShoeSize(int shoeSize) {
        this.shoeSize = shoeSize;
    }

    /**
     * @return the languageCode
     */
    public String getLanguageCode() {
        return languageCode;
    }

    /**
     * @param languageCode
     *            the languageCode to set
     */
    public void setLanguageCode(String languageCode) {
        this.languageCode = languageCode;
    }

    public void addPreference(String pref, String value) {
        synchronized (preferences) {
            preferences.put(pref, value);
        }
    }

    public String getPreference(String pref) {
        synchronized (preferences) {
            return preferences.get(pref);
        }
    }

    public Map<String, String> getPreferences() {
        return Collections.unmodifiableMap(preferences);
    }
}
