package com.basho.riak.client.core.query.timeseries;

import com.basho.riak.client.core.util.BinaryValue;
import com.basho.riak.client.core.util.CharsetUtils;
import com.basho.riak.protobuf.RiakTsPB;
import com.google.protobuf.ByteString;
import org.apache.commons.codec.binary.Hex;

import javax.xml.bind.DatatypeConverter;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;

/**
 * Holds a piece of data for a Time Series @{link Row}.
 * A cell can hold 6 different types of raw data:
 * <ol>
 * <li><b>Varchar</b>s, which can hold byte arrays. Commonly used to store encoded strings.</li>
 * <li><b>SInt64</b>s, which can hold any signed 64-bit integers.</li>
 * <li><b>Double</b>s, which can hold any 64-bit floating point numbers.</li>
 * <li><b>Timestamp</b>s, which can hold any unix/epoch timestamp. Millisecond resolution is required.</li>
 * <li><b>Boolean</b>s, which can hold a true/false value. </li>
 * <li><b>Blob</b>s, which can hold any binary data.</li>
 * </ol>
 * Immutable once created.
 *
 * @author Alex Moore <amoore at basho dot com>
 * @author Sergey Galkin <srggal at gmail dot com>
 * @since 2.0.3
 */

public class Cell
{
    private static final int VARCHAR_MASK = 0x00000001;
    private static final int SINT64_MASK = 0x00000002;
    private static final int DOUBLE_MASK = 0x00000004;
    private static final int TIMESTAMP_MASK = 0x00000008;
    private static final int BOOLEAN_MASK = 0x00000010;
    private static final int BLOB_MASK = 0x00000011;
    private int typeBitfield = 0x0;

    private String varcharValue = "";
    private long sint64Value = 0L;
    private double doubleValue = 0.0;
    private long timestampValue = 0L;
    private boolean booleanValue = false;
    private byte[] blobValue = {};

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

        initVarchar(varcharValue);
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

        initVarchar(varcharValue.toStringUtf8());
    }

    /**
     * Creates a new "Integer" Cell from the provided long.
     *
     * @param sint64Value The long to store.
     */
    public Cell(long sint64Value)
    {
        initSInt64(sint64Value);
    }

    /**
     * Creates a new double cell.
     *
     * @param doubleValue The double to store.
     */
    public Cell(double doubleValue)
    {
        initDouble(doubleValue);
    }

    /**
     * Creates a new "Boolean" Cell from the provided boolean.
     *
     * @param booleanValue The boolean to store.
     */
    public Cell(boolean booleanValue)
    {
        initBoolean(booleanValue);
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

        initTimestamp(timestampValue.getTimeInMillis());
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

        initTimestamp(timestampValue.getTime());
    }

    /**
     * Creates a new "Blob" Cell from the provided byte array.
     *
     * @param blobValue The blob to store.
     */
    public Cell(byte[] blobValue)
    {
        if (blobValue == null)
        {
            throw new IllegalArgumentException("Value for BLOB value cannot be NULL.");
        }

        initBlob(blobValue);
    }

    Cell(RiakTsPB.TsCell pbCell, RiakTsPB.TsColumnDescription columnDescription)
    {
        if (pbCell.hasBooleanValue())
        {
            initBoolean(pbCell.getBooleanValue());
        }
        else if (pbCell.hasDoubleValue())
        {
            initDouble(pbCell.getDoubleValue());
        }
        else if (pbCell.hasSint64Value())
        {
            initSInt64(pbCell.getSint64Value());
        }
        else if (pbCell.hasTimestampValue())
        {
            initTimestamp(pbCell.getTimestampValue());
        }
        else if (pbCell.hasVarcharValue() )
        {
            //  If there is no column description provided, VARCHAR will be used by default
            final RiakTsPB.TsColumnType type = columnDescription == null ? RiakTsPB.TsColumnType.VARCHAR : columnDescription.getType();

            switch (type)
            {
                case VARCHAR:
                    initVarchar(pbCell.getVarcharValue().toStringUtf8());
                    break;

                case BLOB:
                    initBlob(pbCell.getVarcharValue().toByteArray());
                    break;

                default:
                    throw new IllegalStateException(
                            String.format(
                                    "Type '%s' from the provided ColumnDefinition contradicts to the actual VARCHAR value",
                                    type.name()
                                )
                        );
            }
        }
        else
        {
            throw new IllegalArgumentException("Unknown PB Cell encountered.");
        }
    }

    private Cell()
    {
    }

    /**
     * Creates a new "Timestamp" cell from the provided raw value.
     *
     * @param rawTimestampValue The epoch timestamp, including milliseconds.
     * @return The new timestamp Cell.
     */
    public static Cell newTimestamp(long rawTimestampValue)
    {
        final Cell cell = new Cell();
        cell.initTimestamp(rawTimestampValue);
        return cell;
    }

    private void initBoolean(boolean booleanValue)
    {
        setBitfieldType(BOOLEAN_MASK);
        this.booleanValue = booleanValue;
    }

    private void initTimestamp(long timestampValue)
    {
        setBitfieldType(TIMESTAMP_MASK);
        this.timestampValue = timestampValue;
    }

    private void initDouble(double doubleValue)
    {
        setBitfieldType(DOUBLE_MASK);
        this.doubleValue = doubleValue;
    }

    private void initSInt64(long longValue)
    {
        setBitfieldType(SINT64_MASK);
        this.sint64Value = longValue;
    }

    private void initVarchar(String stringValue)
    {
        setBitfieldType(VARCHAR_MASK);
        this.varcharValue = stringValue;
    }

    private void initBlob(byte[] blobValue)
    {
        setBitfieldType(BLOB_MASK);
        this.blobValue = blobValue;
    }

    private void setBitfieldType(int mask)
    {
        typeBitfield |= mask;
    }

    private boolean bitfieldHasType(int mask)
    {
        return typeBitfield == mask;
    }

    public boolean hasVarcharValue()
    {
        return bitfieldHasType(VARCHAR_MASK);
    }

    public boolean hasLong()
    {
        return bitfieldHasType(SINT64_MASK);
    }

    public boolean hasDouble()
    {
        return bitfieldHasType(DOUBLE_MASK);
    }

    public boolean hasTimestamp()
    {
        return bitfieldHasType(TIMESTAMP_MASK);
    }

    public boolean hasBoolean()
    {
        return bitfieldHasType(BOOLEAN_MASK);
    }

    public boolean hasBlob()
    {
        return bitfieldHasType(BLOB_MASK);
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

    public byte[] getBlob()
    {
        return blobValue;
    }

    RiakTsPB.TsCell getPbCell()
    {
        final RiakTsPB.TsCell.Builder builder = RiakTsPB.TsCell.newBuilder();

        if (hasVarcharValue())
        {
            builder.setVarcharValue(ByteString.copyFromUtf8(varcharValue));
        }
        else if (hasLong())
        {
            builder.setSint64Value(sint64Value);
        }
        else if (hasTimestamp())
        {
            builder.setTimestampValue(timestampValue);
        }
        else if (hasBoolean())
        {
            builder.setBooleanValue(booleanValue);
        }
        else if (hasDouble())
        {
            builder.setDoubleValue(doubleValue);
        }
        else if(hasBlob())
        {
            builder.setVarcharValue(ByteString.copyFrom(blobValue));
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
        else if (this.hasBlob())
        {
            final int length = blobValue.length > 8 ? 8 : blobValue.length;
            final byte[] blobBlurb = Arrays.copyOfRange(blobValue, 0, length);
            sb.append("0x");
            sb.append( Hex.encodeHex(blobBlurb ,true));
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

        if (typeBitfield != cell.typeBitfield)
        {
            return false;
        }
        if (sint64Value != cell.sint64Value)
        {
            return false;
        }
        if (Double.compare(cell.doubleValue, doubleValue) != 0)
        {
            return false;
        }
        if (timestampValue != cell.timestampValue)
        {
            return false;
        }
        if (booleanValue != cell.booleanValue)
        {
            return false;
        }
        if (varcharValue != null ? !varcharValue.equals(cell.varcharValue) : cell.varcharValue != null)
        {
            return false;
        }
        return Arrays.equals(blobValue, cell.blobValue);

    }

    @Override
    public int hashCode()
    {
        int result;
        long temp;
        result = typeBitfield;
        result = 31 * result + (varcharValue != null ? varcharValue.hashCode() : 0);
        result = 31 * result + (int) (sint64Value ^ (sint64Value >>> 32));
        temp = Double.doubleToLongBits(doubleValue);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + (int) (timestampValue ^ (timestampValue >>> 32));
        result = 31 * result + (booleanValue ? 1 : 0);
        result = 31 * result + Arrays.hashCode(blobValue);
        return result;
    }
}
