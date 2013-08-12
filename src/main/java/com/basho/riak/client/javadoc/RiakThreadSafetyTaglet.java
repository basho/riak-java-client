/*
 * Copyright 2013 Basho Technologies Inc.
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
package com.basho.riak.client.javadoc;
import com.sun.javadoc.*;
import com.sun.tools.doclets.Taglet;
import java.util.Map;
/**
 * Taglet for our docs to describe the thread safety of a class or method
 * 
 * 
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class RiakThreadSafetyTaglet implements Taglet
{
    private final static String NAME = "riak.threadsafety";
    private final static String HEADER = "Thread Safety:";
    
    @Override
    public boolean inField()
    {
        return true;
    }

    @Override
    public boolean inConstructor()
    {
        return true;
    }

    @Override
    public boolean inMethod()
    {
        return true;
    }

    @Override
    public boolean inOverview()
    {
        return true;
    }

    @Override
    public boolean inPackage()
    {
        return true;
    }

    @Override
    public boolean inType()
    {
        return true;
    }

    @Override
    public boolean isInlineTag()
    {
        return false;
    }

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public String toString(Tag tag)
    {
        return "<DT><B>" + HEADER + "</B></DT><DD>"
                + "<TABLE cellpadding=2 cellspacing=0 style=\"width:90%\">"
                + "<TR><TD bgcolor=#F0F0F5>"
                + tag.text()
                + "</TD></TR></TABLE></DD>\n";
    }

    @Override
    public String toString(Tag[] tags)
    {
        if (tags.length == 0) {
            return null;
        }
        String result = "\n<DT><B>" + HEADER + "</B></DT><DD>";
        result += "<table cellpadding=2 cellspacing=0 style=\"width:90%\"><tr><td style=\"width:90%\" bgcolor=#F0F0F5>";
        for (int i = 0; i < tags.length; i++) {
            if (i > 0) {
                result += ", ";
            }
            result += tags[i].text();
        }
        return result + "</td></tr></table></DD>\n";
    }
    
    public static void register(Map<String, Taglet> tagletMap) {
       RiakThreadSafetyTaglet tag = new RiakThreadSafetyTaglet();
       Taglet t = tagletMap.get(tag.getName());
       if (t != null) {
           tagletMap.remove(tag.getName());
       }
       tagletMap.put(tag.getName(), tag);
    }
}
