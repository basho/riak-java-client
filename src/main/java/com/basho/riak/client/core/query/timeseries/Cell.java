package com.basho.riak.client.core.query.timeseries;

import com.basho.riak.client.core.util.BinaryValue;

import java.util.Calendar;
import java.util.Date;

/**
 * Holds a piece of data for a Time Series @{link Row}.
 * A cell can hold 5 different types of raw data:
 * <ol>
 *     <li><b>Varchar</b>s, which can hold byte arrays. Commonly used to store encoded strings.</li>
 *     <li><b>Integer</b>s, which can hold any signed 64-bit integers.</li>
 *     <li><b>Double</b>s, which can hold any 64-bit floating point numbers.</li>
 *     <li><b>Timestamp</b>s, which can hold any unix/epoch timestamp. Millisecond resolution is required.</li>
 *     <li><b>Boolean</b>s, which can hold a true/false value. </li>
 * </ol>
 *
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0.3
 */
public class Cell
{
    Cell() {}

    /**
     * Creates a new "Varchar" Cell, based on the UTF8 binary encoding of the provided String.
     * @param value The string to encode and store.
     */
    public Cell(String value)
    {
        this.varcharValue = BinaryValue.createFromUtf8(value);
    }

    /**
     * Creates a new "Varchar" cell from the provided BinaryValue.
     * @param value The BinaryValue to store.
     */
    public Cell(BinaryValue value)
    {
        this.varcharValue = value;
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

    protected BinaryValue varcharValue;
    protected long integerValue;
    protected long timestampValue;
    protected boolean booleanValue;
    protected double doubleValue;

    private boolean isIntegerCell = false;
    private boolean isTimestampCell = false;
    private boolean isBooleanCell = false;
    private boolean isDoubleCell = false;

    /**
     * Indicates whether this Cell contains a Varchar value.
     * @return true if it contains a Varchar value, false otherwise.
     */
    public boolean hasVarcharValue()
    {
        return this.varcharValue != null;
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
     * Returns the Varchar value, decoded to a UTF8 String.
     * @return The Varchar value, decoded to a UTF8 String.
     */
    public String getVarcharAsUTF8String()
    {
        return this.varcharValue.toStringUtf8();
    }

    /**
     * Returns the raw Varchar value as a BinaryValue object.
     * @return The raw Varchar value as a BinaryValue object.
     */
    public BinaryValue getVarcharValue()
    {
        return this.varcharValue;
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

        if (this.hasVarcharValue())
        {
            final String value = this.getVarcharAsUTF8String();
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


