package com.basho.riak.client.core.query.timeseries;

import com.basho.riak.client.core.util.BinaryValue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Date;

/**
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0.3
 */
public class Cell
{
    public static final Charset ASCII_Charset = Charset.forName("US-ASCII");

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
        byte[] rawFloat = Float.toString(value).getBytes(ASCII_Charset);
        this.numericValue = ByteBuffer.allocate(rawFloat.length).order(ByteOrder.BIG_ENDIAN).put(rawFloat).array();
    }

    public Cell(double value)
    {
        byte[] rawDouble = Double.toString(value).getBytes(ASCII_Charset);
        this.numericValue = ByteBuffer.allocate(rawDouble.length).order(ByteOrder.BIG_ENDIAN).put(rawDouble).array();
    }

    public Cell(boolean value)
    {
        this.booleanValue = value;
        this.isBooleanCell = true;
    }

    public Cell(Calendar value)
    {
        this.timestampValue = value.getTimeInMillis() / 1000L;
        this.isTimestampCell = true;
    }

    public Cell(Date value)
    {
        this.timestampValue = value.getTime() / 1000L;
        this.isTimestampCell = true;
    }

    // Someday, when we only support JDK8+...
//    public Cell(ZonedDateTime value)
//    {
//        this.timestampValue = value.toEpochSecond();
//        this.isTimestampCell = true;
//    }
//
//    public Cell(OffsetDateTime value)
//    {
//        this.timestampValue = value.toEpochSecond();
//        this.isTimestampCell = true;
//    }
//
//    public Cell(Instant value)
//    {
//        this.timestampValue = value.getEpochSecond();
//        this.isTimestampCell = true;
//    }

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

    public boolean hasNumeric() { return this.numericValue != null && this.numericValue.length > 0; }

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

    public String getRawNumericString()
    {
        return new String(this.getRawNumeric(), ASCII_Charset);
    }

    public float getFloat()
    {
        return Float.parseFloat(getRawNumericString());
    }

    public double getDouble()
    {
        return Double.parseDouble(getRawNumericString());
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

    public static Cell newNumeric(String value)
    {
        Cell cell = new Cell();
        byte[] rawNumeric = value.getBytes(ASCII_Charset);
        cell.numericValue = ByteBuffer.allocate(rawNumeric.length).order(ByteOrder.BIG_ENDIAN).put(rawNumeric).array();
        return cell;
    }

    public static Cell newRawNumeric(byte[] value)
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


