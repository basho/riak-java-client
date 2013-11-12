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

import com.basho.riak.client.query.crdt.types.*;
import com.basho.riak.client.util.ByteArrayWrapper;
import com.basho.riak.protobuf.RiakDtPB;
import com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.List;

public class CrdtResponseConverter
{
    private CrdtElement parseSet(List<ByteString> setValues)
    {
        List<ByteArrayWrapper> entries = new ArrayList<ByteArrayWrapper>(setValues.size());
        for (ByteString bstring : setValues)
        {
            entries.add(ByteArrayWrapper.unsafeCreate(bstring.toByteArray()));
        }
        return new CrdtSet(entries);
    }

    private CrdtElement parseMap(List<RiakDtPB.MapEntry> mapEntries)
    {
        List<CrdtMap.MapEntry> entries = new ArrayList<CrdtMap.MapEntry>(mapEntries.size());
        for (RiakDtPB.MapEntry entry : mapEntries)
        {

            RiakDtPB.MapField field = entry.getField();

            CrdtElement element;
            switch (field.getType())
            {
                case COUNTER:
                    element = new CrdtCounter(entry.getCounterValue());
                    break;
                case FLAG:
                    element = new CrdtFlag(entry.getFlagValue());
                    break;
                case MAP:
                    element = parseMap(entry.getMapValueList());
                    break;
                case REGISTER:
                    element = new CrdtRegister(ByteArrayWrapper.unsafeCreate(entry.getRegisterValue().toByteArray()));
                    break;
                case SET:
                    element = parseSet(entry.getSetValueList());
                    break;
                default:
                    throw new IllegalStateException("Expecting a datatype in map entry but none found");
            }

            ByteArrayWrapper key = ByteArrayWrapper.unsafeCreate(entry.getField().getName().toByteArray());
            entries.add(new CrdtMap.MapEntry(key, element));
        }

        return new CrdtMap(entries);
    }

    public CrdtElement convert(RiakDtPB.DtUpdateResp response)
    {

        CrdtElement element = null;

        if (response.hasCounterValue())
        {
            element = new CrdtCounter(response.getCounterValue());
        }
        else if (response.getSetValueCount() > 0)
        {
            element = parseSet(response.getSetValueList());
        }
        else if (response.getMapValueCount() > 0)
        {
            element = parseMap(response.getMapValueList());
        }

        return element;

    }

    public CrdtElement convert(RiakDtPB.DtFetchResp response)
    {
        CrdtElement element;
        switch (response.getType())
        {
            case COUNTER:
                element = new CrdtCounter(response.getValue().getCounterValue());
                break;
            case MAP:
                element = parseMap(response.getValue().getMapValueList());
                break;
            case SET:
                element = parseSet(response.getValue().getSetValueList());
                break;
            default:
                throw new IllegalStateException("No known datatype returned");
        }

        return element;
    }
}
