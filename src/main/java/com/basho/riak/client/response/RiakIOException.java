package com.basho.riak.client.response;

import java.io.IOException;

/**
 * Thrown when an error occurs during communication with the Riak server.
 */
public class RiakIOException extends RuntimeException {

    private static final long serialVersionUID = -3451479917953961929L;

    public RiakIOException() {
        super();
    }

    public RiakIOException(String message, IOException cause) {
        super(message, cause);
    }

    public RiakIOException(String message) {
        super(message);
    }

    public RiakIOException(Throwable cause) {
        super(cause);
    }

}
