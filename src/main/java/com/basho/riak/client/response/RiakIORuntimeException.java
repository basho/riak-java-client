package com.basho.riak.client.response;

import java.io.IOException;

/**
 * Thrown when an error occurs during communication with the Riak server.
 */
public class RiakIORuntimeException extends RuntimeException {

    private static final long serialVersionUID = -3451479917953961929L;

    public RiakIORuntimeException() {
        super();
    }

    public RiakIORuntimeException(String message, IOException cause) {
        super(message, cause);
    }

    public RiakIORuntimeException(String message) {
        super(message);
    }

    public RiakIORuntimeException(Throwable cause) {
        super(cause);
    }

}
