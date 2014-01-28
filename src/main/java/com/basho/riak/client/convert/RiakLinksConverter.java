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

import com.basho.riak.client.convert.reflect.AnnotationHelper;
import com.basho.riak.client.query.links.RiakLink;

import java.util.Collection;
import java.util.List;

/**
 * Handles the copying of RiakLinks from domain objects -> IRiakObject and back
 * 
 * @author russell
 * @param <T>
 * 
 */
public class RiakLinksConverter<T> {

    private final AnnotationHelper annotationHelper = AnnotationHelper.getInstance();

    public List<RiakLink> getLinks(T domainObject) {
        return annotationHelper.getLinks(domainObject);
    }

    public T populateLinks(Collection<RiakLink> links, T domainObject) {
        return annotationHelper.setLinks(links, domainObject);
    }
}
