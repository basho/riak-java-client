package com.basho.riak.client.plain;

import com.basho.riak.client.response.RiakIORuntimeException;

public class RiakIOException extends Exception {

    private static final long serialVersionUID = 2179229841757644538L;

    public RiakIOException(RiakIORuntimeException e) {
        super(e.getMessage(), e.getCause());
    }
}
