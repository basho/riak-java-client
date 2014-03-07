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
package com.basho.riak.client.convert;

import com.basho.riak.client.RiakLink;
import com.basho.riak.client.cap.VClock;
import java.util.Collection;
import java.util.Map;
import java.util.Set;


/**
 * @author Brian Roach <roach at basho dot com>
 * 
 */
public class ConversionUtilTest {

    protected static final String META_KEY_ONE = "metaKeyOne";
    
    protected static final class PojoWithAnnotatedFields<T>
    {
        @RiakKey
        T key;
     
        @RiakVClock
        VClock vclock;
        
        @RiakUsermeta(key = META_KEY_ONE) 
        String metaItemOne;
        
        @RiakUsermeta
        Map<String,String> usermeta;
        
        @RiakIndex(name = "favorite_languages") 
        public Set<String> languages;

        @RiakIndex(name = "lucky_language")
        public String luckyLanguage;
        
        @RiakIndex(name = "lucky_int")
        public int luckyInt;
        
        @RiakIndex(name = "lucky_integer")
        public Integer luckyInteger;
        
        @RiakIndex(name = "integers")
        public Set<Integer> integers;
        
        @RiakIndex(name = "lucky_long")
        public long luckyLong;
        
        @RiakIndex(name = "lucky_longlong")
        public Long luckyLongLong; 
        
        @RiakIndex(name = "longs")
        public Set<Long> longs;
        
        @RiakTombstone
        boolean tombstone;
        
        @RiakLinks
        Collection<RiakLink> links;
        
        
    }
    
    protected static final class PojoWithAnnotatedMethods<T>
    {
        private T key;
        private Map<String,String> usermeta;
        private String metaItemOne;
        private Set<Long> longs;
        private Set<Integer> integers;
        private Set<String> strings;
        private VClock vClock;
        private boolean tombstone;
        private Collection<RiakLink> links;
        
        @RiakKey
        public T getKey()
        {
            return this.key;
        }
        
        @RiakKey
        public void setKey(T key)
        {
            this.key = key;
        }
        
        @RiakVClock
        public VClock getVClock()
        {
            return this.vClock;
        }
        
        @RiakVClock
        public void setVClock(VClock vclock)
        {
            this.vClock = vclock;
        }
        
        @RiakUsermeta
        public Map<String, String> getUsermeta()
        {
            return this.usermeta;
        }
        
        @RiakUsermeta
        public void setUsermeta(Map<String,String> usermeta)
        {
            this.usermeta = usermeta;
        }
        
        @RiakUsermeta(key = META_KEY_ONE)
        public String getMetaItemOne() 
        {
            return this.metaItemOne;
        }
        
        @RiakUsermeta(key = META_KEY_ONE)
        public void setMetaItemOne(String item)
        {
            this.metaItemOne = item;
        }
        
        @RiakIndex(name = "longs")
        public Set<Long> getLongs() {
          return longs;
        }
        
        @RiakIndex(name = "longs")
        public void setLongs(Set<Long> longs) {
            this.longs = longs;
        }

        @RiakIndex(name = "integers")
        public Set<Integer> getIntegers() {
            return integers;
        }
        
        @RiakIndex(name = "integers")
        public void setIntegers(Set<Integer> ints)
        {
            this.integers = ints;
        }
        
        @RiakIndex(name = "strings") 
        public Set<String> getStrings() {
            return strings;
        }
        
        @RiakIndex(name = "strings") 
        public void setStrings(Set<String> strings) {
            this.strings = strings;
        }
        
        @RiakTombstone
        public Boolean getTombstone() {
            return this.tombstone;
        }
        
        @RiakTombstone
        public void setTombstone(Boolean tombstone) {
            this.tombstone = tombstone;
        }
        
        @RiakLinks
        public Collection<RiakLink> getLinks()
        {
            return this.links;
        }
        
        @RiakLinks
        public void setLinks(Collection<RiakLink> links)
        {
            this.links = links;
        }
    }
}
