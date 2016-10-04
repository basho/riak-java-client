package com.basho.riak.client.core.query.indexes;

/**
 * Collection of Built-in Riak Secondary Index Names
 */
public class IndexNames
{
    /*
        The $bucket index name, used to fetch all the keys in a certain bucket.
     */
    public static final String BUCKET = "$bucket";

    /*
        The $key index name, used to fetch a range of keys in a certain bucket.
     */
    public static final String KEY = "$key";
}
