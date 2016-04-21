package com.basho.riak.protobuf.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class TemplateController
{
    public Map<String, Object> getProperties() throws IOException
    {
        HashMap<String, Object> result = new HashMap<String, Object>();

        File f = new File("./riak_pb/src/riak_pb_messages.csv");
        InputStream stream =
            new FileInputStream(f);

        BufferedReader in = new BufferedReader(new InputStreamReader(stream));

        String line;
        StringBuilder sb = new StringBuilder();
        String prefix = "public static final byte MSG_";
        String cast = "(byte)";

        while ((line = in.readLine()) != null)
        {
            String[] csv = line.split(",");

            String constName = csv[1].replaceFirst("^Rpb", "");

            sb.append(prefix).append(constName).append(" = ");

            int code = Integer.valueOf(csv[0]);
            if (code > 127)
            {
                sb.append(cast);
            }

            sb.append(csv[0]).append(";\n");
        }

        result.put("packageName", "com.basho.riak.protobuf");
        result.put("codes", sb.toString());
        return result;
    }
}
