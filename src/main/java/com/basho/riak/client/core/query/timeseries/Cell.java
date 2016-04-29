package com.basho.riak.client.core.query.timeseries;

import com.basho.riak.client.core.util.BinaryValue;
import com.basho.riak.client.core.util.CharsetUtils;
import com.basho.riak.protobuf.RiakTsPB;
import com.ericsson.otp.erlang.*;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static Logger logger = LoggerFactory.getLogger(Cell.class);
    private final String varcharValue;
    private final Long sint64Value;
    private final Double doubleValue;
    private final Long timestampValue;
    private final Boolean booleanValue;

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

        this.varcharValue = varcharValue;
        this.sint64Value = null;
        this.doubleValue = null;
        this.timestampValue = null;
        this.booleanValue = null;
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

        this.varcharValue = varcharValue.toStringUtf8();
        this.sint64Value = null;
        this.doubleValue = null;
        this.timestampValue = null;
        this.booleanValue = null;
    }

    /**
     * Creates a new "Integer" Cell from the provided long.
     *
     * @param sint64Value The long to store.
     */
    public Cell(long sint64Value)
    {
        this.varcharValue = null;
        this.sint64Value = sint64Value;
        this.doubleValue = null;
        this.timestampValue = null;
        this.booleanValue = null;
    }

    /**
     * Creates a new double cell.
     *
     * @param doubleValue The double to store.
     */
    public Cell(double doubleValue)
    {
        this.varcharValue = null;
        this.sint64Value = null;
        this.doubleValue = doubleValue;
        this.timestampValue = null;
        this.booleanValue = null;
    }

    /**
     * Creates a new "Boolean" Cell from the provided boolean.
     *
     * @param booleanValue The boolean to store.
     */
    public Cell(boolean booleanValue)
    {
        this.varcharValue = null;
        this.sint64Value = null;
        this.doubleValue = null;
        this.timestampValue = null;
        this.booleanValue = booleanValue;
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

        this.varcharValue = null;
        this.sint64Value = null;
        this.doubleValue = null;
        this.timestampValue = timestampValue.getTimeInMillis();
        this.booleanValue = null;
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

        this.varcharValue = null;
        this.sint64Value = null;
        this.doubleValue = null;
        this.timestampValue = timestampValue.getTime();
        this.booleanValue = null;
    }

    Cell(RiakTsPB.TsCell pbCell)
    {
        if (pbCell.hasBooleanValue())
        {
            this.varcharValue = null;
            this.sint64Value = null;
            this.doubleValue = null;
            this.timestampValue = null;
            this.booleanValue = pbCell.getBooleanValue();
        }
        else if (pbCell.hasDoubleValue())
        {
            this.varcharValue = null;
            this.sint64Value = null;
            this.doubleValue = pbCell.getDoubleValue();
            this.timestampValue = null;
            this.booleanValue = null;
        }
        else if (pbCell.hasSint64Value())
        {
            this.varcharValue = null;
            this.sint64Value = pbCell.getSint64Value();
            this.doubleValue = null;
            this.timestampValue = null;
            this.booleanValue = null;
        }
        else if (pbCell.hasTimestampValue())
        {
            this.varcharValue = null;
            this.sint64Value = null;
            this.doubleValue = null;
            this.timestampValue = pbCell.getTimestampValue();
            this.booleanValue = null;
        }
        else if(pbCell.hasVarcharValue())
        {
            this.varcharValue = pbCell.getVarcharValue().toStringUtf8();
            this.sint64Value = null;
            this.doubleValue = null;
            this.timestampValue = null;
            this.booleanValue = null;
        }
        else
        {
            throw new IllegalArgumentException("Unknown PB Cell encountered.");
        }
    }

    private Cell(long rawTimestampValue, boolean isTimestamp)
    {
        this.varcharValue = null;
        this.sint64Value = null;
        this.doubleValue = null;
        this.timestampValue = rawTimestampValue;
        this.booleanValue = null;
    }

    /**
     * Creates a new "Timestamp" cell from the provided raw value.
     *
     * @param rawTimestampValue The epoch timestamp, including milliseconds.
     * @return The new timestamp Cell.
     */
    public static Cell newTimestamp(long rawTimestampValue)
    {
        return new Cell(rawTimestampValue, true);
    }

    public boolean hasVarcharValue()
    {
        return varcharValue != null;
    }

    public boolean hasLong()
    {
        return sint64Value != null;
    }

    public boolean hasTimestamp()
    {
        return timestampValue != null;
    }

    public boolean hasBoolean()
    {
        return booleanValue != null;
    }

    public boolean hasDouble()
    {
        return doubleValue != null;
    }

    public String getVarcharAsUTF8String()
    {
        return varcharValue;
    }

    public BinaryValue getVarcharValue()
    {
        return BinaryValue.unsafeCreate(varcharValue.getBytes(CharsetUtils.UTF_8));
    }

    public long getLong()
    {
        return sint64Value;
    }

    public double getDouble()
    {
        return doubleValue;
    }

    public long getTimestamp()
    {
        return timestampValue;
    }

    public boolean getBoolean()
    {
        return booleanValue;
    }

    public RiakTsPB.TsCell getPbCell()
    {
        final RiakTsPB.TsCell.Builder builder = RiakTsPB.TsCell.newBuilder();

        if (hasVarcharValue())
        {
            builder.setVarcharValue(ByteString.copyFromUtf8(varcharValue));
        }
        if (hasLong())
        {
            builder.setSint64Value(sint64Value);
        }
        if (hasTimestamp())
        {
            builder.setTimestampValue(timestampValue);
        }
        if (hasBoolean())
        {
            builder.setBooleanValue(booleanValue);
        }
        if (hasDouble())
        {
            builder.setDoubleValue(doubleValue);
        }

        return builder.build();
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

        if (varcharValue != null ? !varcharValue.equals(cell.varcharValue) : cell.varcharValue != null)
        {
            return false;
        }
        if (sint64Value != null ? !sint64Value.equals(cell.sint64Value) : cell.sint64Value != null)
        {
            return false;
        }
        if (doubleValue != null ? !doubleValue.equals(cell.doubleValue) : cell.doubleValue != null)
        {
            return false;
        }
        if (timestampValue != null ? !timestampValue.equals(cell.timestampValue) : cell.timestampValue != null)
        {
            return false;
        }
        return booleanValue != null ? booleanValue.equals(cell.booleanValue) : cell.booleanValue == null;

    }

    @Override
    public int hashCode()
    {
        int result = varcharValue != null ? varcharValue.hashCode() : 0;
        result = 31 * result + (sint64Value != null ? sint64Value.hashCode() : 0);
        result = 31 * result + (doubleValue != null ? doubleValue.hashCode() : 0);
        result = 31 * result + (timestampValue != null ? timestampValue.hashCode() : 0);
        result = 31 * result + (booleanValue != null ? booleanValue.hashCode() : 0);
        return result;
    }
}
