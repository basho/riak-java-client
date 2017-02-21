package com.basho.riak.client.api;

public class ListException extends Exception
{
    public ListException()
    {
        super("Bucket and key list operations are expensive and should not be used in production.");
    }

    public ListException(Throwable cause)
    {
        super("Bucket and key list operations are expensive and should not be used in production.", cause);
    }
}
