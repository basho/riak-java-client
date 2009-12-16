package com.basho.riak.client;

public class RiakException extends RuntimeException {

    private static final long serialVersionUID = -3451479917953961929L;

    public RiakException() {
        super();
    }

    public RiakException(String message, Throwable cause) {
        super(message, cause);
    }

    public RiakException(String message) {
        super(message);
    }

    public RiakException(Throwable cause) {
        super(cause);
    }

}
