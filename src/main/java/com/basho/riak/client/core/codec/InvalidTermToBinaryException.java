package com.basho.riak.client.core.codec;

import com.ericsson.otp.erlang.OtpErlangDecodeException;

import java.io.IOException;

public class InvalidTermToBinaryException extends IOException
{
    public InvalidTermToBinaryException(String errorMessage, OtpErlangDecodeException cause)
    {
        super(errorMessage, cause);
    }

    public InvalidTermToBinaryException(String errorMessage)
    {
        super(errorMessage);
    }
}
