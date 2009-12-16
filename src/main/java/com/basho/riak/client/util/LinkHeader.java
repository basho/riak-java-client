package com.basho.riak.client.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LinkHeader {

    private static String TOKEN = "(?:[^\\(\\)<>@,;:\\\"/\\[\\]\\?={} \\t]+?)";
    private static String QUOTED_STRING = "(?:\"(?:\\\"|[^\"])*\")";
    private static String PARAMETER = String.format("(?:%s(?:=(?:%s|%s))?)", TOKEN, TOKEN, QUOTED_STRING);
    private static String LINK = "<[^>]*>\\s*(?:;\\s*" + PARAMETER + "?\\s*)*";
    private static String COMMA = "(?:\\s*(?:,\\s*)+)";
    private static String SEMICOLON = "(?:\\s*(?:;\\s*)+)";
    private static String LINK_SPLIT = LINK + "(?=" + COMMA + "|\\s*$)";
    private static String PARAM_SPLIT = PARAMETER + "(?=" + SEMICOLON + "|\\s*$)";
    private static Pattern LINK_SPLITTER = Pattern.compile(LINK_SPLIT);
    private static Pattern PARAM_SPLITTER = Pattern.compile(PARAM_SPLIT);

    public static Map<String, Map<String, String>> parse(String header) {
        Map<String, Map<String, String>> out = new LinkedHashMap<String, Map<String, String>>();
        
        if (header == null || header.length() == 0)
            return out;
        
        Matcher m = LINK_SPLITTER.matcher(header);
        while (m.find()) {
            String link = m.group().trim();
            String[] urlandparams = link.split(">", 1);
            String url = urlandparams[0].substring(1);
            Map<String, String> parsedLink = new HashMap<String, String>();
            
            if (urlandparams.length > 1) {
                String params = urlandparams[1];
                for (String param : splitParams(params)) {
                    String[] parts = param.split("=", 1);
                    if (parts.length > 1) {
                        parsedLink.put(parts[0].toLowerCase(), unquoteString(parts[1]));
                    } else {
                        parsedLink.put(parts[0].toLowerCase(), null);
                    }
                }
            }
            out.put(url, parsedLink);
        }
        
        return out;
    }

    private static String unquoteString(String s) {
        if (s.startsWith("\"") && s.endsWith("\""))
            s = s.substring(1, s.length() - 1);
        return s.replaceAll("\\(.)", "$1");
    }
    
    private static List<String> splitParams(String s) {
        
        List<String> items = new ArrayList<String>();
        if (s == null || s.length() == 0) 
            return items;
        
        Matcher m = PARAM_SPLITTER.matcher(s);
        while (m.find())
            items.add(m.group().trim());
        
        return items;
    }

}
