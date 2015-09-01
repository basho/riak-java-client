package com.basho.riak.client.core.query.timeseries;

import com.basho.riak.client.core.util.BinaryValue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Calendar;
import java.util.Date;

/**
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0.3
 */
public class Cell
{
    Cell() {}

    public Cell(String value)
    {
        this.binaryValue = BinaryValue.createFromUtf8(value);
    }

    public Cell(BinaryValue value)
    {
        this.binaryValue = value;
    }

    public Cell(int value)
    {
        this.integerValue = (long) value;
        this.isIntegerCell = true;
    }

    public Cell(long value)
    {
        this.integerValue = value;
        this.isIntegerCell = true;
    }

    public Cell(float value)
    {
        this.numericValue = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putFloat(value).array();
    }

    public Cell(double value)
    {
        this.numericValue = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putDouble(value).array();
    }

    public Cell(boolean value)
    {
        this.booleanValue = value;
        this.isBooleanCell = true;
    }

    public Cell(Calendar value)
    {
        this.timestampValue = value.getTimeInMillis();
        this.isTimestampCell = true;
    }

    public Cell(Date value)
    {
        this.timestampValue = value.getTime();
        this.isTimestampCell = true;
    }

    protected BinaryValue binaryValue;
    protected long integerValue;
    protected byte[] numericValue;
    protected long timestampValue;
    protected boolean booleanValue;
    protected byte[][] setValue;
    protected byte[] mapValue;

    private boolean isIntegerCell = false;
    private boolean isTimestampCell = false;
    private boolean isBooleanCell = false;

    public boolean hasString()
    {
        return hasBinaryValue();
    }

    public boolean hasBinaryValue()
    {
        return this.binaryValue != null;
    }

    public boolean hasInt()
    {
        return this.isIntegerCell &&
               this.integerValue < Integer.MAX_VALUE &&
               this.integerValue > Integer.MIN_VALUE;
    }

    public boolean hasLong()
    {
        return this.isIntegerCell;
    }

    public boolean hasRawNumericValue()
    {
        return this.numericValue != null && this.numericValue.length > 0;
    }

    public boolean hasFloat()
    {
        return hasRawNumericValue() && this.numericValue.length == 4;
    }

    public boolean hasDouble()
    {
        return hasRawNumericValue() && this.numericValue.length == 8;
    }

    public boolean hasTimestamp()
    {
        return this.isTimestampCell;
    }

    public boolean hasBoolean()
    {
        return this.isBooleanCell;
    }

    public boolean hasSet()
    {
        return this.setValue != null;
    }

    public boolean hasMap()
    {
        return this.mapValue != null;
    }

    public String getUtf8String()
    {
        return this.binaryValue.toStringUtf8();
    }

    public BinaryValue getBinaryValue()
    {
        return this.binaryValue;
    }

    public long getLong()
    {
        return this.integerValue;
    }

    public int getInt()
    {
        return (int) this.integerValue;
    }

    public byte[] getRawNumeric()
    {
        return this.numericValue;
    }

    public float getFloat()
    {
        return ByteBuffer.wrap(this.numericValue).order(ByteOrder.BIG_ENDIAN).getFloat();
    }

    public double getDouble()
    {
        return ByteBuffer.wrap(this.numericValue).order(ByteOrder.BIG_ENDIAN).getDouble();
    }

    public long getTimestamp()
    {
        return timestampValue;
    }

    public boolean getBoolean()
    {
        return booleanValue;
    }

    public byte[][] getSet()
    {
        return setValue;
    }

    public byte[] getMap()
    {
        return mapValue;
    }

    public static Cell newNumeric(byte[] value)
    {
        Cell cell = new Cell();
        cell.numericValue = value;
        return cell;
    }

    public static Cell newTimestamp(long value)
    {
        Cell cell = new Cell();
        cell.timestampValue = value;
        cell.isTimestampCell = true;
        return cell;
    }

    public static Cell newSet(byte[][] value)
    {
        Cell cell = new Cell();
        cell.setValue = value;
        return cell;
    }

    public static Cell newMap(byte[] value)
    {
        Cell cell = new Cell();
        cell.mapValue = value;
        return cell;
    }
}


