package com.basho.riak.client.core.query.timeseries;

import com.basho.riak.client.core.util.BinaryValue;
import com.basho.riak.protobuf.RiakTsPB;
import com.ericsson.otp.erlang.OtpErlangBinary;
import com.ericsson.otp.erlang.OtpErlangBoolean;
import com.ericsson.otp.erlang.OtpErlangDouble;
import com.ericsson.otp.erlang.OtpErlangLong;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.google.protobuf.ByteString;

import java.util.Calendar;
import java.util.Date;

/**
 * Holds a piece of data for a Time Series @{link Row}.
 * A cell can hold 5 different types of raw data:
 * <ol>
 * <li><b>Varchar</b>s, which can hold byte arrays. Commonly used to store encoded strings.</li>
 * <li><b>SInt64</b>s, which can hold any signed 64-bit integers.</li>
 * <li><b>Double</b>s, which can hold any 64-bit floating point numbers.</li>
 * <li><b>Timestamp</b>s, which can hold any unix/epoch timestamp. Millisecond resolution is required.</li>
 * <li><b>Boolean</b>s, which can hold a true/false value. </li>
 * </ol>
 * Immutable once created.
 *
 * @author Alex Moore <amoore at basho dot com>
 * @author Sergey Galkin <srggal at gmail dot com>
 * @since 2.0.3
 */

public class Cell
{
    static final Cell NullCell = new Cell(RiakTsPB.TsCell.newBuilder().build());

    private final RiakTsPB.TsCell pbCell;

    /**
     * Creates a new "Varchar" Cell, based on the UTF8 binary encoding of the provided String.
     *
     * @param varcharValue The string to encode and store.
     */
    public Cell(String varcharValue)
    {
        if (varcharValue == null)
        {
            throw new IllegalArgumentException("String value cannot be NULL.");
        }

        final ByteString varcharByteString = ByteString.copyFromUtf8(varcharValue);
        this.pbCell = RiakTsPB.TsCell.newBuilder().setVarcharValue(varcharByteString).build();
    }

    /**
     * Creates a new "Varchar" cell from the provided BinaryValue.
     *
     * @param varcharValue The BinaryValue to store.
     */
    public Cell(BinaryValue varcharValue)
    {
        if (varcharValue == null)
        {
            throw new IllegalArgumentException("BinaryValue value cannot be NULL.");
        }

        final ByteString varcharByteString = ByteString.copyFrom(varcharValue.getValue());
        this.pbCell = RiakTsPB.TsCell.newBuilder().setVarcharValue(varcharByteString).build();
    }

    /**
     * Creates a new "Integer" Cell from the provided long.
     *
     * @param sint64Value The long to store.
     */
    public Cell(long sint64Value)
    {
        this.pbCell = RiakTsPB.TsCell.newBuilder().setSint64Value(sint64Value).build();
    }

    /**
     * Creates a new double cell.
     *
     * @param doubleValue The double to store.
     */
    public Cell(double doubleValue)
    {
        this.pbCell = RiakTsPB.TsCell.newBuilder().setDoubleValue(doubleValue).build();
    }

    /**
     * Creates a new "Boolean" Cell from the provided boolean.
     *
     * @param booleanValue The boolean to store.
     */
    public Cell(boolean booleanValue)
    {
        this.pbCell = RiakTsPB.TsCell.newBuilder().setBooleanValue(booleanValue).build();
    }

    /**
     * Creates a new "Timestamp" Cell from the provided Calendar, by fetching the current time in milliseconds.
     *
     * @param timestampValue The Calendar to fetch the timestamp from.
     */
    public Cell(Calendar timestampValue)
    {
        if (timestampValue == null)
        {
            throw new IllegalArgumentException("Calendar object for timestamp value cannot be NULL.");
        }

        this.pbCell = RiakTsPB.TsCell.newBuilder().setTimestampValue(timestampValue.getTimeInMillis()).build();
    }

    /**
     * Creates a new "Timestamp" Cell from the provided Date, by fetching the current time in milliseconds.
     *
     * @param timestampValue The Date to fetch the timestamp from.
     */
    public Cell(Date timestampValue)
    {
        if (timestampValue == null)
        {
            throw new IllegalArgumentException("Date object for timestamp value cannot be NULL.");
        }

        this.pbCell = RiakTsPB.TsCell.newBuilder().setTimestampValue(timestampValue.getTime()).build();
    }

    Cell(RiakTsPB.TsCell pbCell)
    {
        this.pbCell = pbCell;
    }

    /**
     * Creates a new "Timestamp" cell from the provided raw value.
     *
     * @param rawTimestampValue The epoch timestamp, including milliseconds.
     * @return The new timestamp Cell.
     */
    public static Cell newTimestamp(long rawTimestampValue)
    {
        final RiakTsPB.TsCell tsCell = RiakTsPB.TsCell.newBuilder().setTimestampValue(rawTimestampValue).build();
        return new Cell(tsCell);
    }

    public boolean hasVarcharValue()
    {
        return pbCell.hasVarcharValue();
    }

    public boolean hasLong()
    {
        return pbCell.hasSint64Value();
    }

    public boolean hasTimestamp()
    {
        return pbCell.hasTimestampValue();
    }

    public boolean hasBoolean()
    {
        return pbCell.hasBooleanValue();
    }

    public boolean hasDouble()
    {
        return pbCell.hasDoubleValue();
    }

    public String getVarcharAsUTF8String()
    {
        return pbCell.getVarcharValue().toStringUtf8();
    }

    public BinaryValue getVarcharValue()
    {
        return BinaryValue.unsafeCreate(pbCell.getVarcharValue().toByteArray());
    }

    public long getLong()
    {
        return pbCell.getSint64Value();
    }

    public double getDouble()
    {
        return pbCell.getDoubleValue();
    }

    public long getTimestamp()
    {
        return pbCell.getTimestampValue();
    }

    public boolean getBoolean()
    {
        return pbCell.getBooleanValue();
    }

    public RiakTsPB.TsCell getPbCell()
    {
        return pbCell;
    }

    public OtpErlangObject getErlangObject() {
        if (pbCell.hasVarcharValue()) {
            return new OtpErlangBinary(pbCell.getVarcharValue().toByteArray());
        }
        if (pbCell.hasSint64Value()) {
            return new OtpErlangLong(pbCell.getSint64Value());
        }
        if (pbCell.hasTimestampValue()) {
            return new OtpErlangLong(pbCell.getTimestampValue());
        }
        if (pbCell.hasBooleanValue()) {
            return new OtpErlangBoolean(pbCell.getBooleanValue());
        }
        if (pbCell.hasDoubleValue()) {
            return new OtpErlangDouble(pbCell.getDoubleValue());
        }
        // TODO GH-611 throw exception?
        return null;
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
                sb.append(value.substring(0, 32));
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

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        Cell cell = (Cell) o;

        return !(pbCell != null ? !pbCell.equals(cell.pbCell) : cell.pbCell != null);

    }

    @Override
    public int hashCode()
    {
        return pbCell != null ? pbCell.hashCode() : 0;
    }
}
