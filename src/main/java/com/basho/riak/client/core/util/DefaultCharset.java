package com.basho.riak.client.core.util;

import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
public final class DefaultCharset
{
    private final AtomicReference<Charset> currentCharset;
    private static DefaultCharset instance = initializeDefaultCharsetSingleton();

    private static DefaultCharset initializeDefaultCharsetSingleton()
    {
        Charset charset = Charset.defaultCharset();

        String declaredCharsetName = System.getProperty(Constants.CLIENT_OPTION_CHARSET);
        if(declaredCharsetName != null && !declaredCharsetName.isEmpty())
        {
            Charset declaredCharset = Charset.forName(declaredCharsetName);
            charset = declaredCharset;
        }

        return new DefaultCharset(charset);
    }

    private DefaultCharset(Charset c)
    {
        this.currentCharset = new AtomicReference<Charset>(c);
    }


    public static Charset get()
    {
        return instance.currentCharset.get();
    }

    public static void set(Charset charset)
    {
        instance.currentCharset.set(charset);
    }
}
