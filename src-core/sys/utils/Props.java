package sys.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;

public class Props {

    public static void parseFile(String propName, PrintStream out) {
        try {
            File filename = new File(System.getProperty(propName));

            if (out != null)
                out.printf(";\tReading properties from: %s\n", filename);

            BufferedReader br = new BufferedReader(new FileReader(filename));
            String line;
            while ((line = br.readLine()) != null) {

                if (out != null)
                    out.printf(";\t%s\n", line);

                if (line.isEmpty())
                    continue;

                String[] tokens = line.split("=");
                if (tokens.length == 2)
                    System.setProperty(tokens[0].toLowerCase(), tokens[1]);
            }
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        }
    }

    public static void parseFile(String propName, String defaultPropFile) {
        if (System.getProperty(propName) == null)
            System.setProperty(propName, defaultPropFile);
        parseFile(propName, System.out);
    }

    public static String get(String prop) {
        String p = System.getProperty(prop.toLowerCase());
        return p == null ? "" : p;
    }

    public static String get(String prop, String defaultVal) {
        String p = System.getProperty(prop.toLowerCase());
        return p == null ? defaultVal : p;
    }

    public static boolean boolValue(String prop, boolean defVal) {
        String p = System.getProperty(prop.toLowerCase());
        return p == null ? defVal : Boolean.valueOf(p);
    }

    public static int intValue(String prop, int defVal) {
        String p = System.getProperty(prop.toLowerCase());
        return p == null ? defVal : Integer.valueOf(p);
    }

    public static long longValue(String prop, long defVal) {
        String p = System.getProperty(prop.toLowerCase());
        return p == null ? defVal : Long.valueOf(p);
    }
}
