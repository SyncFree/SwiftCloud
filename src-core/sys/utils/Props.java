package sys.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Properties;

public class Props {

    public static Properties parseFile(String propName, PrintStream out) {
        try {
            final Properties props = new Properties();
            String filename = System.getProperty(propName);
            if (filename != null) {
                BufferedReader br = new BufferedReader(new FileReader(filename));
                props.load(br);
                br.close();
                if (out != null) {
                    out.printf("; Read properties from: %s\n", filename);

                    // Marek, naughty, naughty, you did break the statistics
                    // scripts ;-)
                    for (Object i : props.keySet())
                        out.printf(";\t%s=%s\n", i, props.getProperty((String) i));
                }
            }
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

    public static Properties parseFile(String propName, PrintStream out, String defaultFilename) {
        String filename = System.getProperty(propName);
        if (filename == null)
            System.setProperty(propName, defaultFilename);

        return parseFile(propName, out);
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
