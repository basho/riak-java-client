package com.basho.riak.client.core.query.timeseries;

import com.basho.riak.client.core.util.BinaryValue;

/**
 * @author Sergey Galkin <srggal at gmail dot com>
 * @since 2.0.3
 */
public interface ICell
{
    /**
     * Indicates whether this Cell contains a Varchar value.
     * @return true if it contains a Varchar value, false otherwise.
     */
    boolean hasVarcharValue();

    /**
     * Indicates whether this Cell contains a valid signed 64-bit long integer value ({@link Long}).
     * @return true if it contains a java @{link Long} value, false otherwise.
     */
    boolean hasLong();

    /**
     * Indicates whether this Cell contains a Timestamp value.
     * Please note that as of Riak TimeSeries Beta 1, any timestamp values returned from Riak will appear
     * in the Integer value, instead of the Timestamp value.
     * Please use @{link #hasLong()} instead of @{link #hasTimestamp()} to check if a value exists until further notice.
     * @return true if it contains a Timestamp value, false otherwise.
     */
    boolean hasTimestamp();

    /**
     * Indicates whether this Cell contains a Boolean value.
     * @return true if it contains a Boolean value, false otherwise.
     */
    boolean hasBoolean();

    /**
     * Indicates whether this Cell contains a Double value.
     * @return true if it contains a Double value, false otherwise.
     */
    boolean hasDouble();

    /**
     * Returns the Varchar value, decoded to a UTF8 String.
     * @return The Varchar value, decoded to a UTF8 String.
     */
    String getVarcharAsUTF8String();

    /**
     * Returns the raw Varchar value as a BinaryValue object.
     * @return The raw Varchar value as a BinaryValue object.
     */
    BinaryValue getVarcharValue();

    /**
     * Returns the "Integer" value, as a Long.
     * @return The integer value, as a Java long.
     */
    long getLong();

    /**
     * Returns the the "Double" value.
     * @return The double value.
     */
    double getDouble();

    /**
     * Returns the raw "Timestamp" value.
     * Please note that as of Riak TimeSeries Beta 1, any timestamp values returned from Riak will appear
     * in the "Integer" register, instead of the Timestamp register.
     * Please use @{link #getLong()} instead of @{link #getTimestamp()} to fetch a value until further notice.
     * @see #getLong()
     * @return The timestamp value.
     */
    long getTimestamp();

    /**
     * Returns the raw "Boolean" value.
     * @return The boolean value.
     */
    boolean getBoolean();
}
