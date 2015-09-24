package com.basho.riak.client.core.util;

import org.junit.Assume;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;

public class DefaultCharsetTest
{
    @Test
    public void testDefaultIfNoPropertyDefined()
    {
        Charset systemDefault = Charset.defaultCharset();
        Charset clientDefault = DefaultCharset.get();

        String definedCharsetName = System.getProperty(Constants.CLIENT_OPTION_CHARSET);

        Boolean noCharsetPropertyDefined = definedCharsetName == null;
        Assume.assumeTrue(noCharsetPropertyDefined);

        assertEquals(systemDefault, clientDefault);
    }

    @Test
    public void testDefinedCharset()
    {
        Charset clientDefault = DefaultCharset.get();

        String definedCharsetName = System.getProperty(Constants.CLIENT_OPTION_CHARSET);

        Boolean charsetPropertyDefined = definedCharsetName != null && !definedCharsetName.isEmpty();

        Assume.assumeTrue(charsetPropertyDefined);

        Charset definedCharset = Charset.forName(definedCharsetName);
        assertEquals(definedCharset, clientDefault);
    }

    @Test
    public void testStaticInitializerValidDefinedCharset()
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, NoSuchFieldException
    {
        String definedCharsetName = System.getProperty(Constants.CLIENT_OPTION_CHARSET);
        Charset systemDefault = Charset.defaultCharset();
        String goodCharsetName = "UTF-16";

        try
        {
            Charset goodCharset = Charset.forName(goodCharsetName);

            // Open up the private static initializer
            Method staticInitializer = DefaultCharset.class.getDeclaredMethod("initializeDefaultCharsetSingleton",
                                                                              null);
            staticInitializer.setAccessible(true);

            // Set system property for client charset to bad value
            System.setProperty(Constants.CLIENT_OPTION_CHARSET, goodCharsetName);

            // Run static initializer to test good charset handling
            DefaultCharset initializedDefaultCharset = (DefaultCharset) staticInitializer.invoke(null);

            // Get initialized charset
            Field currentCharsetField = DefaultCharset.class.getDeclaredField("currentCharset");
            currentCharsetField.setAccessible(true);
            AtomicReference<Charset> initializedCharset =
                    (AtomicReference<Charset>)currentCharsetField.get(initializedDefaultCharset);

            assertEquals(goodCharset.name(), initializedCharset.get().name());
        }
        finally
        {
            // Cleanup
            if (definedCharsetName != null)
            {
                System.setProperty(Constants.CLIENT_OPTION_CHARSET, definedCharsetName);
            }
            else
            {
                System.clearProperty(Constants.CLIENT_OPTION_CHARSET);
            }
        }
    }

    @Test
    public void testStaticInitializerInvalidDefinedCharset()
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, NoSuchFieldException
    {
        String definedCharsetName = System.getProperty(Constants.CLIENT_OPTION_CHARSET);
        Charset systemDefault = Charset.defaultCharset();
        String badCharsetName = "Foobar-16NE";

        try
        {
            // Open up the private static initializer
            Method staticInitializer = DefaultCharset.class.getDeclaredMethod("initializeDefaultCharsetSingleton",
                                                                              null);
            staticInitializer.setAccessible(true);

            // Set system property for client charset to bad value
            System.setProperty(Constants.CLIENT_OPTION_CHARSET, badCharsetName);

            // Run static initializer to test bad charset handling
            DefaultCharset initializedDefaultCharset = (DefaultCharset) staticInitializer.invoke(null);

            Field currentCharsetField = DefaultCharset.class.getDeclaredField("currentCharset");
            currentCharsetField.setAccessible(true);
            AtomicReference<Charset> initializedCharset =
                    (AtomicReference<Charset>)currentCharsetField.get(initializedDefaultCharset);

            assertEquals(systemDefault.name(), initializedCharset.get().name());
        }
        finally
        {
            // Cleanup
            if (definedCharsetName != null)
            {
                System.setProperty(Constants.CLIENT_OPTION_CHARSET, definedCharsetName);
            }
            else
            {
                System.clearProperty(Constants.CLIENT_OPTION_CHARSET);
            }
        }
    }

    @Test
    public void testRuntimeSetCharset()
    {
        Charset systemDefault = Charset.defaultCharset();
        Charset ascii = Charset.forName("US-ASCII");
        Charset utf8 = Charset.forName("UTF-8");

        if (systemDefault != utf8)
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
