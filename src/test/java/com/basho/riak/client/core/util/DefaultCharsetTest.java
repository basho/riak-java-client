package com.basho.riak.client.core.util;

import org.junit.*;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;

public class DefaultCharsetTest
{
    private static Charset SystemDefaultCharset;
    private static String definedCharsetName;

    @BeforeClass
    public static void captureInitialState()
    {
        SystemDefaultCharset = Charset.defaultCharset();
        definedCharsetName = System.getProperty(Constants.CLIENT_OPTION_CHARSET);
    }

    @After
    public void tearDown()
    {
        if (definedCharsetName != null)
        {
            System.setProperty(Constants.CLIENT_OPTION_CHARSET, definedCharsetName);
            DefaultCharset.set(Charset.forName(definedCharsetName));
        }
        else
        {
            System.clearProperty(Constants.CLIENT_OPTION_CHARSET);
            DefaultCharset.set(SystemDefaultCharset);
        }
    }

    /*
     * Public Interface State tests
     */

    @Test
    public void testSystemDefaultCharsetIsReturnedIfNoPropertyIsDefined()
    {
        Charset clientDefault = DefaultCharset.get();

        boolean noCharsetPropertyDefined = definedCharsetName == null;
        Assume.assumeTrue(noCharsetPropertyDefined);

        assertEquals(SystemDefaultCharset.name(), clientDefault.name());
    }

    @Test
    public void testDefinedCharsetIsReturnedIfPropertyIsDefined()
    {
        Charset clientDefault = DefaultCharset.get();

        boolean charsetPropertyDefined = definedCharsetName != null && !definedCharsetName.isEmpty();

        Assume.assumeTrue(charsetPropertyDefined);

        Charset definedCharset = Charset.forName(definedCharsetName);
        assertEquals(definedCharset.name(), clientDefault.name());
    }

    @Test
    public void testRuntimeSetCharsetChange()
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

    /*
     * Internal State tests
     */

    @Test
    public void testIntializerProvidesTheRequestedCharsetIfItIsValid()
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, NoSuchFieldException
    {
        String goodCharsetName = "UTF-16";
        Charset goodCharset = Charset.forName(goodCharsetName);

        final DefaultCharset defaultCharsetInstance = setInternalDefaultCharset(goodCharsetName);
        final Charset charset = getCurrentCharsetFromDefaultCharsetInstance(defaultCharsetInstance);

        assertEquals(goodCharset.name(), charset.name());
    }

    @Test
    public void testIntializerProvidesTheDefaultSytemCharsetIfInvalidOneWasRequested()
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, NoSuchFieldException
    {
        String badCharsetName = "Foobar-16NE";

        final DefaultCharset defaultCharsetInstance = setInternalDefaultCharset(badCharsetName);
        final Charset charset = getCurrentCharsetFromDefaultCharsetInstance(defaultCharsetInstance);

        assertEquals(SystemDefaultCharset.name(), charset.name());
    }

    /*
     * Reflection Test Helpers
     */

    private DefaultCharset setInternalDefaultCharset(String charsetName)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, NoSuchFieldException
    {
        // Open up the private static initializer
        Method staticInitializer = getInitializeDefaultCharsetSingletonMethod();

        // Set system property for client charset to bad value
        System.setProperty(Constants.CLIENT_OPTION_CHARSET, charsetName);

        // Run static initializer to test charset handling
        DefaultCharset initializedDefaultCharset = (DefaultCharset) staticInitializer.invoke(null);
        return initializedDefaultCharset;
    }

    private Method getInitializeDefaultCharsetSingletonMethod() throws NoSuchMethodException
    {
        Method staticInitializer = DefaultCharset.class.getDeclaredMethod("initializeDefaultCharsetSingleton");
        staticInitializer.setAccessible(true);
        return staticInitializer;
    }

    @SuppressWarnings("unchecked")
    private Charset getCurrentCharsetFromDefaultCharsetInstance(DefaultCharset initializedDefaultCharset)
            throws NoSuchFieldException, IllegalAccessException
    {
        Field currentCharsetField = DefaultCharset.class.getDeclaredField("currentCharset");
        currentCharsetField.setAccessible(true);
        return ((AtomicReference<Charset>) currentCharsetField.get(initializedDefaultCharset)).get();

    }
}
