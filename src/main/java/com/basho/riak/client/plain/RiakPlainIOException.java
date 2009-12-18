package com.basho.riak.client.plain;

import com.basho.riak.client.response.RiakIOException;

public class RiakPlainIOException extends Exception {

    private static final long serialVersionUID = 2179229841757644538L;

    public RiakPlainIOException(RiakIOException e) {
        super(e.getMessage(), e.getCause());
    }
}
