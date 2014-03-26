package com.basho.riak.client;

public class RiakException extends Exception
{
	public RiakException()
	{
		super();
	}

	public RiakException(String message)
	{
		super(message);
	}

	public RiakException(String message, Throwable cause)
	{
		super(message, cause);
	}

	public RiakException(Throwable cause)
	{
		super(cause);
	}

}
