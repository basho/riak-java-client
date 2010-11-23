package com.basho.riak.pbc;

import com.google.protobuf.ByteString;

public interface IRequestMeta {

	public abstract void preparePut(RPB.RpbPutReq.Builder builder);

	public abstract IRequestMeta returnBody(boolean ret);

	public abstract IRequestMeta w(int w);

	public abstract IRequestMeta dw(int dw);

	public abstract IRequestMeta contentType(String contentType);

	public abstract ByteString getContentType();

}