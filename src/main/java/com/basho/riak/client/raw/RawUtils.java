package com.basho.riak.client.raw;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.basho.riak.client.RiakLink;
import com.basho.riak.client.util.Constants;
import com.basho.riak.client.util.LinkHeader;
import com.basho.riak.client.util.Multipart;

public class RawUtils {
    
    public static Collection<RiakLink> parseLinkHeader(String header) {
        Collection<RiakLink> links = new ArrayList<RiakLink>();
        Map<String, Map<String, String>> parsedLinks = LinkHeader.parse(header);
        for (String url : parsedLinks.keySet()) {
            RiakLink link = parseOneLink(url, parsedLinks.get(url));
            if (link != null) {
                links.add(link);
            }
        }
        return links;
    }

    private static RiakLink parseOneLink(String url, Map<String, String> params) {
        String tag = params.get(Constants.RAW_LINK_TAG);
        if (tag != null) {
            String[] parts = url.split("/");
            if (parts.length >= 2)
                return new RiakLink(parts[parts.length - 1], parts[parts.length - 2], tag);
        }
        return null;
    }

    public static Map<String, String> parseUsermeta(Map<String, String> headers) {
        Map<String, String> usermeta = new HashMap<String, String>();
        for (String header : headers.keySet()) {
            if (header.startsWith(Constants.HDR_USERMETA_PREFIX)) {
                usermeta.put(header.substring(Constants.HDR_USERMETA_PREFIX.length()), headers.get(header));
            }
        }
        return usermeta;
    }

    public static List<RawObject> parseMultipart(String bucket, String key, Map<String, String> docHeaders, String docBody) {

        String vclock = docHeaders.get(Constants.HDR_VCLOCK);

        List<Multipart.Part> parts = Multipart.parse(docHeaders, docBody);
        List<RawObject> objects = new ArrayList<RawObject>();
        if (parts != null) {
            for (Multipart.Part part : parts) {
                Map<String, String> headers = part.getHeaders();
                Collection<RiakLink> links = parseLinkHeader(headers.get(Constants.HDR_LINK));
                Map<String, String> usermeta = parseUsermeta(headers);
                RawObject o = new RawObject(bucket, key, docBody, links, usermeta,
                                            headers.get(Constants.HDR_CONTENT_TYPE), vclock,
                                            headers.get(Constants.HDR_LAST_MODIFIED), headers.get(Constants.HDR_ETAG));
                objects.add(o);
            }
        }
        return objects;
    }

}
