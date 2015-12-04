package com.basho.riak.client.core.query.timeseries.immutable.pb;

import com.basho.riak.client.core.query.timeseries.ICell;
import com.basho.riak.client.core.util.BinaryValue;
import com.basho.riak.protobuf.RiakTsPB;

/**
 * @author Sergey Galkin <srggal at gmail dot com>
 * @since 2.0.3
 */
class Cell implements ICell
{
    private final RiakTsPB.TsCell pbCell;

    public Cell(RiakTsPB.TsCell pbCell)
    {
        this.pbCell = pbCell;
    }

    @Override
    public boolean hasVarcharValue()
    {
        return pbCell.hasVarcharValue();
    }

    @Override
    public boolean hasLong()
    {
        return pbCell.hasSint64Value();
    }

    @Override
    public boolean hasTimestamp()
    {
        return pbCell.hasTimestampValue();
    }

    @Override
    public boolean hasBoolean()
    {
        return pbCell.hasBooleanValue();
    }

    @Override
    public boolean hasDouble()
    {
        return pbCell.hasDoubleValue();
    }

    @Override
    public String getVarcharAsUTF8String()
    {
        return pbCell.getVarcharValue().toStringUtf8();
    }

    @Override
    public BinaryValue getVarcharValue()
    {
        return BinaryValue.unsafeCreate(pbCell.getVarcharValue().toByteArray());
    }

    @Override
    public long getLong()
    {
        return pbCell.getSint64Value();
    }

    @Override
    public double getDouble()
    {
        return pbCell.getDoubleValue();
    }

    @Override
    public long getTimestamp()
    {
        return pbCell.getTimestampValue();
    }

    @Override
    public boolean getBoolean()
    {
        return pbCell.getBooleanValue();
    }
}
