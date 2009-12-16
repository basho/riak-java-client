package com.basho.riak.client.raw;

import java.util.Collection;
import java.util.Map;

import com.basho.riak.client.BasicClient;
import com.basho.riak.client.RiakConfig;
import com.basho.riak.client.RiakLink;
import com.basho.riak.client.RiakObject;
import com.basho.riak.client.request.RequestMeta;
import com.basho.riak.client.request.RiakWalkSpec;
import com.basho.riak.client.util.ClientUtils;
import com.basho.riak.client.util.Constants;

public class RawClient extends BasicClient {
    
    public RawClient(RiakConfig config) {
        super(config);
    }

    public RawClient(String url) { 
        super(new RiakConfig(url));
    }

    @Override
    public RawStoreResponse store(RiakObject object, RequestMeta meta) {
        if (meta == null) meta = new RequestMeta();
        Collection<RiakLink> links = object.getLinks();
        Map<String, String> usermeta = object.getUsermeta();
        StringBuilder linkHeader = new StringBuilder(); 
        String vclock = object.getVclock();

        if (links != null)
            for (RiakLink link : links)
                linkHeader
                    .append("<")
                    .append(ClientUtils.getPathFromUrl(getConfig().getUrl()))
                    .append("/").append(link.getBucket())
                    .append("/").append(link.getKey())
                    .append(">; ")
                    .append(Constants.RAW_LINK_TAG).append("=\"").append(link.getTag()).append("\"")
                    .append(",");
        if (linkHeader.length() > 0)
            meta.put(Constants.HDR_LINK, linkHeader.toString());
        if (usermeta != null)
            for (String name : usermeta.keySet())
                meta.put(Constants.HDR_USERMETA_PREFIX + name, usermeta.get(name));
        if (vclock != null)
            meta.put(Constants.HDR_VCLOCK, vclock);

        return new RawStoreResponse(super.store(object, meta));
    }
    @Override
    public RawStoreResponse store(RiakObject object) {
        return store(object, null);
    }

    @Override
    public RawFetchResponse fetchMeta(String bucket, String key, RequestMeta meta) {
        return new RawFetchResponse(super.fetchMeta(bucket, key, meta));
    }
    @Override
    public RawFetchResponse fetchMeta(String bucket, String key) {
        return fetchMeta(bucket, key, null);
    }

    @Override
    public RawFetchResponse fetch(String bucket, String key, RequestMeta meta) {
        return new RawFetchResponse(super.fetch(bucket, key, meta));
    }
    @Override
    public RawFetchResponse fetch(String bucket, String key) {
        return fetch(bucket, key, null);
    }

    @Override
    public RawWalkResponse walk(String bucket, String key, String walkSpec, RequestMeta meta) {
        if (meta == null) meta = new RequestMeta();
        meta.put(Constants.HDR_ACCEPT, Constants.CTYPE_MULTIPART_MIXED);
        return new RawWalkResponse(super.walk(bucket, key, walkSpec, meta));
    }
    @Override
    public RawWalkResponse walk(String bucket, String key, String walkSpec) {
        return walk(bucket, key, walkSpec, null);
    }
    @Override
    public RawWalkResponse walk(String bucket, String key, RiakWalkSpec walkSpec) {
        return walk(bucket, key, walkSpec.toString(), null);
    }
}
