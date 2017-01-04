/*
 * Copyright 2013 Basho Technologies Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.basho.riak.client.core.converters;

import com.basho.riak.client.core.query.crdt.types.*;
import com.basho.riak.client.core.util.BinaryValue;
import com.basho.riak.protobuf.RiakDtPB;
import com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.List;

public class CrdtResponseConverter
{
    private RiakDatatype parseHll(long hllValue)
    {
        return new RiakHll(hllValue);
    }

    private RiakDatatype parseSet(List<ByteString> setValues)
    {
        List<BinaryValue> entries = new ArrayList<>(setValues.size());
        for (ByteString bstring : setValues)
        {
            entries.add(BinaryValue.unsafeCreate(bstring.toByteArray()));
        }
        return new RiakSet(entries);
    }

    private RiakDatatype parseMap(List<RiakDtPB.MapEntry> mapEntries)
    {
        List<RiakMap.MapEntry> entries = new ArrayList<>(mapEntries.size());
        for (RiakDtPB.MapEntry entry : mapEntries)
        {
            RiakDtPB.MapField field = entry.getField();

            RiakDatatype element;
            switch (field.getType())
            {
                case COUNTER:
                    element = new RiakCounter(entry.getCounterValue());
                    break;
                case FLAG:
                    element = new RiakFlag(entry.getFlagValue());
                    break;
                case MAP:
                    element = parseMap(entry.getMapValueList());
                    break;
                case REGISTER:
                    element = new RiakRegister(BinaryValue.unsafeCreate(entry.getRegisterValue().toByteArray()));
                    break;
                case SET:
                    element = parseSet(entry.getSetValueList());
                    break;
                default:
                    throw new IllegalStateException("Expecting a datatype in map entry but none found");
            }

            BinaryValue key = BinaryValue.unsafeCreate(entry.getField().getName().toByteArray());
            entries.add(new RiakMap.MapEntry(key, element));
        }

        return new RiakMap(entries);
    }

    public RiakDatatype convert(RiakDtPB.DtUpdateResp response)
    {
        RiakDatatype element = null;

        if (response.hasCounterValue())
        {
            element = new RiakCounter(response.getCounterValue());
        }
        else if (response.getSetValueCount() > 0)
        {
            element = parseSet(response.getSetValueList());
        }
        else if (response.getMapValueCount() > 0)
        {
            element = parseMap(response.getMapValueList());
        }
        else if (response.hasHllValue())
        {
            element = parseHll(response.getHllValue());
        }

        return element;
    }

    public RiakDatatype convert(RiakDtPB.DtFetchResp response)
    {
        RiakDatatype element;
        switch (response.getType())
        {
            case COUNTER:
                element = new RiakCounter(response.getValue().getCounterValue());
                break;
            case MAP:
                element = parseMap(response.getValue().getMapValueList());
                break;
            case SET:
                element = parseSet(response.getValue().getSetValueList());
                break;
            case HLL:
                element = parseHll(response.getValue().getHllValue());
                break;
            case GSET:
                element = parseSet(response.getValue().getGsetValueList());
                break;
            default:
                throw new IllegalStateException("No known datatype returned");
        }

        return element;
    }
}
