package com.basho.riak.client.core.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicReference;

/**
 * <p>
 * Holds an classloader-wide default charset, that
 * is then used to encode/decode between Strings and
 * Byte arrays for Riak's use (see {@link com.basho.riak.client.core.util.BinaryValue}).
 * </p>
 * <p/>
 * <p>
 * Before 2.0.3, the system used the JRE's default Charset from the
 * {@link Charset#defaultCharset()} property.
 * </p>
 * <p/>
 * <p>
 * With this class you may change the Riak client to use a different default.
 * You can set it at startup by providing the desired Charset name with the vm argument
 * {@code -Dcom.basho.riak.client.DefaultCharset="UTF-8"}, or at runtime
 * with the static method (see {@link #set(Charset)}).
 * </p>
 * <p/>
 * <p>
 * As of 2.0.3 it still defaults to the value provided by
 * {@link Charset#defaultCharset()},
 * but the default is planned to change to "UTF-8" with 2.1.0.
 * </p>
 * <p/>
 * <p>
 * If your JRE default charset is one of "<b>US-ASCII</b>",
 * "<b>UTF-8</b>", or "<b>ISO-8859-1</b>",
 * this change should <b>not</b> affect you.
 * </p>
 * <p/>
 * <p>
 * If your JRE default charset is one of "<b>UTF-16</b>",
 * "<b>UTF-16BE</b>", or "<b>UTF-16LE</b>",
 * <b>you will need to set that default on the command line or
 * after application startup once you upgrade</b>.
 * </p>
 *
 * @author Alex Moore <amoore AT basho dot com>
 * @since 2.0.3
 */

public final class DefaultCharset
{
    private static final Logger logger = LoggerFactory.getLogger(DefaultCharset.class);
    private final static DefaultCharset instance = initializeDefaultCharsetSingleton();

    private final AtomicReference<Charset> currentCharset;

    private DefaultCharset(Charset c)
    {
        logger.info("Initializing client charset to: {}", c.name());
        this.currentCharset = new AtomicReference<>(c);
    }

    private static DefaultCharset initializeDefaultCharsetSingleton()
    {
        Charset charset;
        final Charset systemDefault = Charset.defaultCharset();
        final String declaredCharsetName = System.getProperty(Constants.CLIENT_OPTION_CHARSET);

        if (declaredCharsetName != null && !declaredCharsetName.isEmpty())
        {
            try
            {
                charset = Charset.forName(declaredCharsetName);
            }
            catch (Exception ex)
            {
                charset = systemDefault;
                logger.warn("Requested charset '{}' is not available, the default charset '{}' will be used",
                            declaredCharsetName,
                            charset.name());
            }
        }
        else
        {
            logger.info("No desired charset found in system properties, the default charset '{}' will be used",
                        systemDefault.name());
            charset = systemDefault;
        }

        return new DefaultCharset(charset);
    }

    /**
     * Get the current classloader-wide default Charset for the Riak client.
     *
     * @return The current classloader-wide default charset.
     */
    public static Charset get()
    {
        return instance.currentCharset.get();
    }

    /**
     * Set the classloader-wide default Charset for the Riak client.
     *
     * @param charset The charset to set the classloader-wide default to.
     */
    public static void set(Charset charset)
    {
        final Charset current = instance.currentCharset.get();
        logger.info("Setting client charset from '{}' to '{}'", current.name(), charset.name());
        instance.currentCharset.set(charset);
    }
}
