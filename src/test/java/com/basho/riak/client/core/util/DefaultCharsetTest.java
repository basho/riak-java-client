package com.basho.riak.client.core.util;

import org.junit.After;
import org.junit.Assume;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicReference;

import static junit.framework.Assert.assertEquals;

public class DefaultCharsetTest
{
    private static final Charset SystemDefaultCharset = Charset.defaultCharset();
    private static final String definedCharsetName = System.getProperty(Constants.CLIENT_OPTION_CHARSET);

    @After
    public void tearDown()
    {
        if (definedCharsetName != null)
        {
            System.setProperty(Constants.CLIENT_OPTION_CHARSET, definedCharsetName);
        }
        else
        {
            System.clearProperty(Constants.CLIENT_OPTION_CHARSET);
        }
    }

    @Test
    public void testSystemDefaultCharsetIsReturnedIfNoPropertyIsDefined()
    {
        Charset clientDefault = DefaultCharset.get();

        Boolean noCharsetPropertyDefined = definedCharsetName == null;
        Assume.assumeTrue(noCharsetPropertyDefined);

        assertEquals(SystemDefaultCharset.name(), clientDefault.name());
    }

    @Test
    public void testDefinedCharsetIsReturnedIfPropertyIsDefined()
    {
        Charset clientDefault = DefaultCharset.get();

        Boolean charsetPropertyDefined = definedCharsetName != null && !definedCharsetName.isEmpty();

        Assume.assumeTrue(charsetPropertyDefined);

        Charset definedCharset = Charset.forName(definedCharsetName);
        assertEquals(definedCharset.name(), clientDefault.name());
    }

    @Test
    public void testIntializerProvidesTheRequestedCharsetIfItIsValid()
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, NoSuchFieldException
    {
        String goodCharsetName = "UTF-16";
        Charset goodCharset = Charset.forName(goodCharsetName);

        // Open up the private static initializer
        Method staticInitializer = getInitializeDefaultCharsetSingletonMethod();

        // Set system property for client charset to bad value
        System.setProperty(Constants.CLIENT_OPTION_CHARSET, goodCharsetName);

        // Run static initializer to test good charset handling
        DefaultCharset initializedDefaultCharset = (DefaultCharset) staticInitializer.invoke(null);

        // Get initialized charset
        AtomicReference<Charset> initializedCharset = getCurrentCharsetFromDefaultCharsetInstance(
                initializedDefaultCharset);

        assertEquals(goodCharset.name(), initializedCharset.get().name());
    }

    @Test
    public void testIntializerProvidesTheDefaultSytemCharsetIfInvalidOneWasRequested()
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, NoSuchFieldException
    {
        String badCharsetName = "Foobar-16NE";

        // Open up the private static initializer
        Method staticInitializer = getInitializeDefaultCharsetSingletonMethod();

        // Set system property for client charset to bad value
        System.setProperty(Constants.CLIENT_OPTION_CHARSET, badCharsetName);

        // Run static initializer to test bad charset handling
        DefaultCharset initializedDefaultCharset = (DefaultCharset) staticInitializer.invoke(null);

        AtomicReference<Charset> initializedCharset = getCurrentCharsetFromDefaultCharsetInstance(
                initializedDefaultCharset);

        assertEquals(SystemDefaultCharset.name(), initializedCharset.get().name());

    }

    private Method getInitializeDefaultCharsetSingletonMethod() throws NoSuchMethodException
    {
        Method staticInitializer = DefaultCharset.class.getDeclaredMethod("initializeDefaultCharsetSingleton");
        staticInitializer.setAccessible(true);
        return staticInitializer;
    }

    @SuppressWarnings("unchecked")
    private AtomicReference<Charset> getCurrentCharsetFromDefaultCharsetInstance(DefaultCharset
                                                                                         initializedDefaultCharset)
            throws NoSuchFieldException, IllegalAccessException
    {
        Field currentCharsetField = DefaultCharset.class.getDeclaredField("currentCharset");
        currentCharsetField.setAccessible(true);
        return (AtomicReference<Charset>) currentCharsetField.get(initializedDefaultCharset);
    }


    @Test
    public void testRuntimeSetCharset()
    {
        Charset ascii = Charset.forName("US-ASCII");
        Charset utf8 = Charset.forName("UTF-8");

        if (SystemDefaultCharset != utf8)
        {
            DefaultCharset.set(utf8);
            Charset clientCharset = DefaultCharset.get();
            assertEquals(utf8.name(), clientCharset.name());
        }
        else
        {
            DefaultCharset.set(ascii);
            Charset clientCharset = DefaultCharset.get();
            assertEquals(ascii.name(), clientCharset.name());
        }
    }
}
