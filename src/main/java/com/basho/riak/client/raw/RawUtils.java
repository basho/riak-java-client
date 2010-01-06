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
package com.basho.riak.client.raw;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.basho.riak.client.RiakLink;
import com.basho.riak.client.util.Constants;
import com.basho.riak.client.util.LinkHeader;
import com.basho.riak.client.util.Multipart;

/**
 * Utility functions specific to the Raw interface
 */
public class RawUtils {

    /**
     * Parse a link header into a {@link RiakLink}. See {@link LinkHeader}.
     * 
     * @param header
     *            The HTTP Link header value.
     * @return List of {@link RiakLink} objects constructed from the links in
     *         header in order.
     */
    public static List<RiakLink> parseLinkHeader(String header) {
        List<RiakLink> links = new ArrayList<RiakLink>();
        Map<String, Map<String, String>> parsedLinks = LinkHeader.parse(header);
        for (String url : parsedLinks.keySet()) {
            RiakLink link = parseOneLink(url, parsedLinks.get(url));
            if (link != null) {
                links.add(link);
            }
        }
        return links;
    }

    /**
     * Create a {@link RiakLink} object from a single parsed link from the Link
     * header
     * 
     * @param url
     *            The link URL
     * @param params
     *            The link parameters
     * @return {@link RiakLink} object
     */
    private static RiakLink parseOneLink(String url, Map<String, String> params) {
        String tag = params.get(Constants.RAW_LINK_TAG);
        if (tag != null) {
            String[] parts = url.split("/");
            if (parts.length >= 2)
                return new RiakLink(parts[parts.length - 2], parts[parts.length - 1], tag);
        }
        return null;
    }

    /**
     * Extract only the user-specified metadata headers from a header set: all
     * headers prefixed with X-Riak-Meta-. The prefix is removed before
     * returning.
     * 
     * @param headers
     *            The full HTTP header set from the response
     * @return Map of all headers prefixed with X-Riak-Meta- with prefix
     *         removed.
     */
    public static Map<String, String> parseUsermeta(Map<String, String> headers) {
        Map<String, String> usermeta = new HashMap<String, String>();
        if (headers != null) {
            for (String header : headers.keySet()) {
                if (header != null && header.toLowerCase().startsWith(Constants.HDR_USERMETA_PREFIX)) {
                    usermeta.put(header.substring(Constants.HDR_USERMETA_PREFIX.length()), headers.get(header));
                }
            }
        }
        return usermeta;
    }

    /**
     * Convert a multipart/mixed document to a list of {@link RawObject}s.
     * 
     * @param bucket
     *            original object's bucket
     * @param key
     *            original object's key
     * @param docHeaders
     *            original document's headers
     * @param docBody
     *            original document's body
     * @return List of RawObjects represented by the multipart document
     */
    public static List<RawObject> parseMultipart(String bucket, String key, Map<String, String> docHeaders,
                                                 String docBody) {

        String vclock = null;

        if (docHeaders != null) {
            vclock = docHeaders.get(Constants.HDR_VCLOCK);
        }

        List<Multipart.Part> parts = Multipart.parse(docHeaders, docBody);
        List<RawObject> objects = new ArrayList<RawObject>();
        if (parts != null) {
            for (Multipart.Part part : parts) {
                Map<String, String> headers = part.getHeaders();
                List<RiakLink> links = parseLinkHeader(headers.get(Constants.HDR_LINK));
                Map<String, String> usermeta = parseUsermeta(headers);
                String location = headers.get(Constants.HDR_LOCATION);
                String partBucket = bucket;
                String partKey = key;

                if (location != null) {
                    String[] locationParts = location.split("/");
                    if (locationParts.length >= 2) {
                        partBucket = locationParts[locationParts.length - 2];
                        partKey = locationParts[locationParts.length - 1];
                    }
                }

                RawObject o = new RawObject(partBucket, partKey, part.getBody(),
                                            headers.get(Constants.HDR_CONTENT_TYPE), links, usermeta, vclock,
                                            headers.get(Constants.HDR_LAST_MODIFIED), headers.get(Constants.HDR_ETAG));
                objects.add(o);
            }
        }
        return objects;
    }

}
