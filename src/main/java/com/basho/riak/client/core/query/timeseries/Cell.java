package com.basho.riak.client.core.query.timeseries;

import com.basho.riak.client.core.util.BinaryValue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.util.Calendar;
import java.util.Date;

/**
 * Holds a piece of data for a Time Series @{link Row}.
 * A cell can hold 9 different types of raw data:
 * <ol>
 *     <li><b>BinaryValue</b>s, which can hold byte arrays. Commonly used to store encoded strings.</li>
 *     <li><b>Integer</b>s, which can hold any signed 64-bit integers.</li>
 *     <li><b>Double</b>s, which can hold any 64-bit floating point numbers.</li>
 *     <li><b>Timestamp</b>s, which can hold any unix/epoch timestamp. Millisecond resolution is required.</li>
 *     <li><b>Boolean</b>s, which can hold a true/false value. </li>
 * </ol>
 *
 * Please note that as of Riak TimeSeries Beta 1, any timestamp values returned from Riak will appear
 * in the Integer value, instead of the Timestamp value.
 *
 * Please use @{link #getLong()} instead of @{link #getTimestamp()},
 * and @{link #hasLong()} instead of @{link #hasTimestamp()} to fetch/check a timestamp value until further notice.
 *
 * To store a timestamp, please use the provided constructors for Date/Calendar,
 * or the static method to create one from a known timestamp, these work correctly.
 *
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0.3
 */
public class Cell
{
    Cell() {}

    /**
     * Creates a new "BinaryValue" Cell, based on the UTF8 binary encoding of the provided String.
     * @param value The string to encode and store.
     */
    public Cell(String value)
    {
        this.binaryValue = BinaryValue.createFromUtf8(value);
    }

    /**
     * Creates a new "BinaryValue" cell from the provided BinaryValue.
     * @param value The BinaryValue to store.
     */
    public Cell(BinaryValue value)
    {
        this.binaryValue = value;
    }

    /**
     * Creates a new "Integer" Cell from the provided long.
     * @param value The long to store.
     */
    public Cell(long value)
    {
        this.integerValue = value;
        this.isIntegerCell = true;
    }

    /**
     * Creates a new double cell.
     * @param value The double to store.
     */
    public Cell(double value)
    {
        this.doubleValue = value;
        this.isDoubleCell = true;
    }

    /**
     * Creates a new "Boolean" Cell from the provided boolean.
     * @param value The boolean to store.
     */
    public Cell(boolean value)
    {
        this.booleanValue = value;
        this.isBooleanCell = true;
    }

    /**
     * Creates a new "Timestamp" Cell from the provided Calendar, by fetching the current time in milliseconds.
     * @param value The Calendar to fetch the timestamp from.
     */
    public Cell(Calendar value)
    {
        this.timestampValue = value.getTimeInMillis();
        this.isTimestampCell = true;
    }

    /**
     * Creates a new "Timestamp" Cell from the provided Date, by fetching the current time in milliseconds.
     * @param value The Date to fetch the timestamp from.
     */
    public Cell(Date value)
    {
        this.timestampValue = value.getTime();
        this.isTimestampCell = true;
    }

    protected BinaryValue binaryValue;
    protected long integerValue;
    protected long timestampValue;
    protected boolean booleanValue;
    protected double doubleValue;

    private boolean isIntegerCell = false;
    private boolean isTimestampCell = false;
    private boolean isBooleanCell = false;
    private boolean isDoubleCell = false;

    /**
     * Indicates whether this Cell contains a String/BinaryValue value.
     * @return true if it contains a String value, false otherwise.
     */
    public boolean hasString()
    {
        return hasBinaryValue();
    }

    /**
     * Indicates whether this Cell contains a BinaryValue value.
     * @return true if it contains a BinaryValue value, false otherwise.
     */
    public boolean hasBinaryValue()
    {
        return this.binaryValue != null;
    }

    /**
     * Indicates whether this Cell contains a valid signed 64-bit long integer value ({@link Long}).
     * @return true if it contains a java @{link Long} value, false otherwise.
     */
    public boolean hasLong()
    {
        return this.isIntegerCell;
    }

    /**
     * Indicates whether this Cell contains a Timestamp value.
     * Please note that as of Riak TimeSeries Beta 1, any timestamp values returned from Riak will appear
     * in the Integer value, instead of the Timestamp value.
     * Please use @{link #hasLong()} instead of @{link #hasTimestamp()} to check if a value exists until further notice.
     * @return true if it contains a Timestamp value, false otherwise.
     */
    public boolean hasTimestamp()
    {
        return this.isTimestampCell;
    }

    /**
     * Indicates whether this Cell contains a Boolean value.
     * @return true if it contains a Boolean value, false otherwise.
     */
    public boolean hasBoolean()
    {
        return this.isBooleanCell;
    }

    /**
     * Indicates whether this Cell contains a Double value.
     * @return true if it contains a Double value, false otherwise.
     */
    public boolean hasDouble() { return this.isDoubleCell; }

    /**
     * Returns the BinaryValue value, decoded to a UTF8 String.
     * @return The BinaryValue value, decoded to a UTF8 String.
     */
    public String getUtf8String()
    {
        return this.binaryValue.toStringUtf8();
    }

    /**
     * Returns the raw BinaryValue value.
     * @return The raw BinaryValue value.
     */
    public BinaryValue getBinaryValue()
    {
        return this.binaryValue;
    }

    /**
     * Returns the "Integer" value, as a Long.
     * @return The integer value, as a Java long.
     */
    public long getLong()
    {
        return this.integerValue;
    }

    /**
     * Returns the the "Double" value.
     * @return The double value.
     */
    public double getDouble()
    {
        return this.doubleValue;
    }

    /**
     * Returns the raw "Timestamp" value.
     * Please note that as of Riak TimeSeries Beta 1, any timestamp values returned from Riak will appear
     * in the "Integer" register, instead of the Timestamp register.
     * Please use @{link #getLong()} instead of @{link #getTimestamp()} to fetch a value until further notice.
     * @see #getLong()
     * @return The timestamp value.
     */
    public long getTimestamp()
    {
        return timestampValue;
    }

    /**
     * Returns the raw "Boolean" value.
     * @return The boolean value.
     */
    public boolean getBoolean()
    {
        return booleanValue;
    }

    /**
     * Creates a new "Timestamp" cell from the provided raw value.
     * @param value The epoch timestamp, including milliseconds.
     * @return The new timestamp Cell.
     */
    public static Cell newTimestamp(long value)
    {
        Cell cell = new Cell();
        cell.timestampValue = value;
        cell.isTimestampCell = true;
        return cell;
    }
    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder("Cell{ ");

        if (this.hasBinaryValue())
        {
            final String value = this.getUtf8String();
            if (value.length() > 32)
            {
                sb.append(value.substring(0,32));
                sb.append("...");
            }
            else
            {
                sb.append(value);
            }
        }
        else if (this.hasLong())
        {
            sb.append(this.getLong());
        }
        else if (this.hasDouble())
        {
            sb.append(this.getDouble());
        }
        else if (this.hasTimestamp())
        {
            sb.append(this.getTimestamp());
        }
        else if (this.hasBoolean())
        {
            sb.append(this.getBoolean());
        }

        sb.append(" }");
        return sb.toString();
    }
}


