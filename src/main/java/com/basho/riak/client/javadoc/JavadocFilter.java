package com.basho.riak.client.javadoc;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;

import com.sun.javadoc.Doc;
import com.sun.javadoc.DocErrorReporter;
import com.sun.javadoc.LanguageVersion;
import com.sun.javadoc.ProgramElementDoc;
import com.sun.javadoc.RootDoc;
import com.sun.tools.doclets.standard.Standard;
import com.sun.tools.javadoc.Main;

/**
 * Used to decide what gets included/excluded in the Javadoc
 * <p>
 * <li>It filters out all inner classes with "UnitTest" in their name.</li>
 * <li>It filters out classes with @ExcludeFromJavadoc in the javadoc comment</li>
 */
public class JavadocFilter {
    public static void main(String[] args) {
        String name = JavadocFilter.class.getName();
        Main.execute(name, name, args);
    }

    public static boolean validOptions(String[][] options, DocErrorReporter reporter) throws java.io.IOException {
        return Standard.validOptions(options, reporter);
    }

    public static LanguageVersion languageVersion() {
        return LanguageVersion.JAVA_1_5;
    }

    public static int optionLength(String option) {
        return Standard.optionLength(option);
    }

    public static boolean start(RootDoc root) throws java.io.IOException {
        return Standard.start((RootDoc) process(root, RootDoc.class));
    }

    private static boolean exclude(Doc doc) {
        if (doc.name().contains("UnitTest")) {
            return true;
        } else if (doc.tags("@ExcludeFromJavadoc").length > 0) {
            return true;
        } else if (doc instanceof ProgramElementDoc) {
            if (((ProgramElementDoc) doc).containingPackage().tags("@ExcludeFromJavadoc").length > 0)
                return true;
        }
        // nothing above found a reason to exclude
        return false;
    }

    private static Object process(Object obj, Class expect) {
        if (obj == null)
            return null;
        Class cls = obj.getClass();
        if (cls.getName().startsWith("com.sun.")) {
            return Proxy.newProxyInstance(cls.getClassLoader(), cls.getInterfaces(), new ExcludeHandler(obj));
        } else if (obj instanceof Object[]) {
            Class componentType = expect.getComponentType();
            if (componentType == null)
            {
                componentType = Object.class;
            }
            Object[] array = (Object[]) obj;
            List list = new ArrayList(array.length);
            for (Object entry : array)
            {
                if ((entry instanceof Doc) && exclude((Doc) entry))
                    continue;
                list.add(process(entry, componentType));
            }
            if (componentType == null)
            {
                System.out.println("Object: " + obj + "Expect: " + expect + " NULL; " + list);
                
            }
            return list.toArray((Object[]) Array.newInstance(componentType, list.size()));
        } else {
            return obj;
        }
    }

    private static class ExcludeHandler implements InvocationHandler {
        private final Object target;

        public ExcludeHandler(Object target) {
            this.target = target;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (args != null) {
                String methodName = method.getName();
                if (methodName.equals("compareTo") || methodName.equals("equals") || methodName.equals("overrides") || methodName.equals("subclassOf")) {
                    args[0] = unwrap(args[0]);
                }
            }
            try {
                return process(method.invoke(target, args), method.getReturnType());
            } catch (InvocationTargetException e) {
                throw e.getTargetException();
            }
        }

        private Object unwrap(Object proxy) {
            if (proxy instanceof Proxy)
                return ((ExcludeHandler) Proxy.getInvocationHandler(proxy)).target;
            return proxy;
        }
    }
}
