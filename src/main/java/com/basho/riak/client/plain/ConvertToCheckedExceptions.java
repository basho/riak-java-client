package com.basho.riak.client.plain;

import com.basho.riak.client.RiakClient;
import com.basho.riak.client.response.RiakExceptionHandler;
import com.basho.riak.client.response.RiakIORuntimeException;
import com.basho.riak.client.response.RiakResponseRuntimeException;
import com.basho.riak.client.util.ClientUtils;

/**
 * Converts unchecked exceptions RiakIORuntimeException and
 * RiakResponseRuntimeException to checked exceptions RiakIOException and
 * RiakRuntimeException. Be careful that everywhere calling a {@link RiakClient}
 * with this {@link RiakExceptionHandler} installed contains the appropriate
 * throws declaration.
 */
public class ConvertToCheckedExceptions implements RiakExceptionHandler {

    public void handle(RiakIORuntimeException e) {
        ClientUtils.throwChecked(new RiakIOException(e));
    }

    public void handle(RiakResponseRuntimeException e) {
        ClientUtils.throwChecked(new RiakResponseException(e));
    }

}