package com.basho.riak.client;

import java.io.InputStream;
import java.util.Collection;
import java.util.Map;


public interface RiakObject {

    public String getBucket();

    public String getKey();

    public String getValue();

    public Collection<RiakLink> getLinks();

    public Map<String, String> getUsermeta();

    public String getContentType();

    public String getVclock();

    public String getLastmod();

    public String getVtag();

    public String getEntity();

    public InputStream getEntityStream();

    public long getEntityStreamLength();
}