package sys.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 
 * Convenience class for parsing main class arguments...
 * 
 * @author smduarte
 * 
 */
public class Args {

    static Map<String, String[]> _args = new HashMap<>();

    static String[] _current;

    static public void use(String[] args) {
        _current = args;
    }

    static public void useArgs(String key) {
        _current = _args.get(key);
        if (_current == null)
            throw new RuntimeException("Unknown key...");
    }

    static public void setArgs(String key, String[] args) {
        _current = args;
        _args.put(key, args);
    }

    static public boolean contains(String flag) {
        return contains(_current, flag);
    }

    static public String valueOf(String flag, String defaultValue) {
        return valueOf(_current, flag, defaultValue);
    }

    static public int valueOf(String flag, int defaultValue) {
        return valueOf(_current, flag, defaultValue);
    }

    static public double valueOf(String flag, double defaultValue) {
        return valueOf(_current, flag, defaultValue);
    }

    static public boolean valueOf(String flag, boolean defaultValue) {
        return valueOf(_current, flag, defaultValue);
    }

    static public List<String> subList(String flag) {
        return subList(_current, flag);
    }

    static public List<String> subList(String flag, String defaultValue) {
        return subList(_current, flag, defaultValue);
    }

    static public boolean contains(String[] args, String flag) {
        return Arrays.asList(args).contains(flag);
    }

    static public String valueOf(String[] args, String flag, String defaultValue) {
        for (int i = 0; i < args.length - 1; i++)
            if (flag.equals(args[i]))
                return args[i + 1];
        return defaultValue;
    }

    static public int valueOf(String[] args, String flag, int defaultValue) {
        for (int i = 0; i < args.length - 1; i++)
            if (flag.equals(args[i]))
                return Integer.parseInt(args[i + 1]);
        return defaultValue;
    }

    static public double valueOf(String[] args, String flag, double defaultValue) {
        for (int i = 0; i < args.length - 1; i++)
            if (flag.equals(args[i]))
                return Double.parseDouble(args[i + 1]);
        return defaultValue;
    }

    static public boolean valueOf(String[] args, String flag, boolean defaultValue) {
        for (int i = 0; i < args.length - 1; i++)
            if (flag.equals(args[i]))
                return Boolean.parseBoolean(args[i + 1]);
        return defaultValue;
    }

    static public List<String> subList(String[] args, String flag) {
        List<String> res = new ArrayList<String>();
        for (int i = 0; i < args.length - 1; i++)
            if (flag.equals(args[i])) {
                for (int j = i + 1; j < args.length; j++)
                    if (args[j].startsWith("-"))
                        return res;
                    else
                        res.add(args[j]);
            }
        return res;
    }

    static public List<String> subList(String[] args, String flag, String defaultValue) {
        return contains(args, flag) ? subList(args, flag) : subList(new String[] { flag, defaultValue }, flag);
    }
}
