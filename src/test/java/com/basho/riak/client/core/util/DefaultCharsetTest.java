package com.basho.riak.client.core.util;

import java.nio.charset.Charset;

import org.junit.Test;
import static org.junit.Assert.*;

public class DefaultCharsetTest
{
    @Test
    public void testDefaults()
    {
        Charset systemDefault = Charset.defaultCharset();
        Charset clientDefault = DefaultCharset.get();

        String definedCharsetName = System.getProperty(Constants.CLIENT_OPTION_CHARSET);

        if(definedCharsetName == null || definedCharsetName.isEmpty())
        {
            assertEquals(systemDefault, clientDefault);
        }
        else
        {
            Charset definedCharset = Charset.forName(definedCharsetName);
            assertEquals(definedCharset, clientDefault);
        }
    }

    @Test
    public void testRuntimeSetCharset()
    {
        Charset systemDefault = Charset.defaultCharset();
        Charset ascii = Charset.forName("US-ASCII");
        Charset utf8 = Charset.forName("UTF-8");

        if(systemDefault != utf8)
        {
            DefaultCharset.set(utf8);
            Charset clientCharset = DefaultCharset.get();
            assertEquals(utf8, clientCharset);
        }
        else
        {
            DefaultCharset.set(ascii);
            Charset clientCharset = DefaultCharset.get();
            assertEquals(ascii, clientCharset);
        }
    }
}
