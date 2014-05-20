/*****************************************************************************
 * Copyright 2011-2012 INRIA
 * Copyright 2011-2012 Universidade Nova de Lisboa
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *****************************************************************************/
package sys.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 
 * Convenience class for parsing main class arguments...
 * 
 * @author smduarte
 * 
 */
public class Args {

    // Can this be generalized????

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
