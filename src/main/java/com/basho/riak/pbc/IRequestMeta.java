package com.basho.riak.pbc;

import com.basho.riak.client.cap.Quorum;
import com.google.protobuf.ByteString;

/**
 * PBC model of request meta data (<code>w, dw, content-type, returnBody?</code>)
 */
public interface IRequestMeta {

	public abstract void preparePut(RPB.RpbPutReq.Builder builder);

	public abstract IRequestMeta returnBody(boolean ret);

	public abstract IRequestMeta w(int w);
    
    public abstract IRequestMeta w(Quorum w);

	public abstract IRequestMeta dw(int dw);
    
    public abstract IRequestMeta dw(Quorum dw);
    
    public abstract IRequestMeta pw(int pw);
    
    public abstract IRequestMeta pw(Quorum pw);

	public abstract IRequestMeta contentType(String contentType);

	public abstract ByteString getContentType();

}