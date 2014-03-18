package sys.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Enumeration;
import java.util.Map;
import java.util.Map.Entry;
import java.util.HashMap;
import java.util.Properties;

public class Props {

    public static Properties parseFile(String propName, PrintStream out) {
        try {
            File filename = new File(System.getProperty(propName));

            if (out != null)
                out.printf(";\tReading properties from: %s\n", filename);

            BufferedReader br = new BufferedReader(new FileReader(filename));
            final Properties props = new Properties();
            props.load(br);
            br.close();
            props.list(out);
            // BACKWARD-COMPABILITY HACK:
            Properties processedProps = new Properties();
            for (final String key : props.stringPropertyNames()) {
                processedProps.put(key, props.getProperty(key));
                processedProps.put(key.toLowerCase(), props.getProperty(key));
            }
            return processedProps;
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
            return null;
        }
    }

    public static String get(Properties props, String prop) {
        String p = props.getProperty(prop.toLowerCase());
        return p == null ? "" : p;
    }

    public static String get(Properties props, String prop, String defaultVal) {
        String p = props.getProperty(prop.toLowerCase());
        return p == null ? defaultVal : p;
    }

    public static boolean boolValue(Properties props, String prop, boolean defVal) {
        String p = props.getProperty(prop.toLowerCase());
        return p == null ? defVal : Boolean.valueOf(p);
    }

    public static int intValue(Properties props, String prop, int defVal) {
        String p = props.getProperty(prop.toLowerCase());
        return p == null ? defVal : Integer.valueOf(p);
    }

    public static long longValue(Properties props, String prop, long defVal) {
        String p = props.getProperty(prop.toLowerCase());
        return p == null ? defVal : Long.valueOf(p);
    }
}
