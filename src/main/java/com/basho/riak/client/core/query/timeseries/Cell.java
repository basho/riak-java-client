package com.basho.riak.client.core.query.timeseries;

import com.basho.riak.client.core.util.BinaryValue;

/**
 *
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0.3
 */
public class Cell
{
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

    public boolean hasBinaryValue() {
        return binaryValue != null;
    }

    public boolean hasIntegerValue() {
        return this.isIntegerCell;
    }

    public boolean hasNumericValue() {
        return this.numericValue != null;
    }

    public boolean hasTimestampValue() {
        return this.isTimestampCell;
    }

    public boolean hasBooleanValue() {
        return this.isBooleanCell;
    }

    public boolean hasSetValue() {
        return this.setValue != null;
    }

    public boolean hasMapValue() {
        return this.mapValue != null;
    }
    
    public BinaryValue getBinaryValue() {
        return binaryValue;
    }

    public long getIntegerValue() {
        return integerValue;
    }

    public byte[] getNumericValue() {
        return numericValue;
    }

    public long getTimestampValue() {
        return timestampValue;
    }

    public boolean getBooleanValue() {
        return booleanValue;
    }

    public byte[][] getSetValue() {
        return setValue;
    }

    public byte[] getMapValue() {
        return mapValue;
    }

    public static Cell newBinaryCell(byte[] value)
    {
        Cell cell = new Cell();
        cell.binaryValue = BinaryValue.unsafeCreate(value);
        return cell;
    }

    public static Cell newBinaryCell(BinaryValue value)
    {
        Cell cell = new Cell();
        cell.binaryValue = value;
        return cell;
    }

    public static Cell newIntegerCell(long value)
    {
        Cell cell = new Cell();
        cell.integerValue = value;
        cell.isIntegerCell = true;
        return cell;
    }

    public static Cell newNumericCell(byte[] value)
    {
        Cell cell = new Cell();
        cell.numericValue = value;
        return cell;
    }

    public static Cell newTimestampCell(long value)
    {
        Cell cell = new Cell();
        cell.timestampValue = value;
        cell.isTimestampCell = true;
        return cell;
    }

    public static Cell newBooleanCell(boolean value)
    {
        Cell cell = new Cell();
        cell.booleanValue = value;
        cell.isBooleanCell = true;
        return cell;
    }

    public static Cell newSetCell(byte[][] value)
    {
        Cell cell = new Cell();
        cell.setValue = value;
        return cell;
    }

    public static Cell newMapCell(byte[] value)
    {
        Cell cell = new Cell();
        cell.mapValue = value;
        return cell;
    }
}


