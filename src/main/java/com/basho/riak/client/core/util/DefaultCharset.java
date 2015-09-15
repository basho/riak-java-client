package com.basho.riak.client.core.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
public final class DefaultCharset
{
    private static final Logger logger = LoggerFactory.getLogger(DefaultCharset.class);
    private final static DefaultCharset instance = initializeDefaultCharsetSingleton();

    private final AtomicReference<Charset> currentCharset;

    private DefaultCharset(Charset c)
    {
        LogCharsetChange(c);
        this.currentCharset = new AtomicReference<Charset>(c);
    }

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

    private static void LogCharsetChange(Charset charset)
    {
        logger.info("Setting client charset to: {}", charset.name());
    }

    public static Charset get()
    {
        return instance.currentCharset.get();
    }

    public static void set(Charset charset)
    {
        LogCharsetChange(charset);
        instance.currentCharset.set(charset);
    }
}
