
/***
 * Altered version of the class available for public download at http://twit88.com/blog/2008/10/14/java-a-simple-json-utility/
 * With no visible license or restrictions the code is used here, please address-me at my GitHub mail
 * if it's incurring in any violation
 ***/

package org.uminho.gsd.benchmarks.helpers;


import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class JsonUtil {

    // ----------- Private static members --------

    private static final Logger log = Logger.getLogger(JsonUtil.class);     // Logging object
    private static JsonFactory jf = new JsonFactory();                 // JSON factory

    // ------------ Public static methods ----------------------


    /**
     * Convert JackSon string to Map<String, String>[].
     *
     * @param str - Jackson string
     * @return Map<String, String>[]
     */
    public static List<Map<String, String>> getListFromJsonArray(String str) {
        try {
            if (isNotBlank(str)) {
                ArrayList<Map<String, String>> arrList = (ArrayList<Map<String, String>>) new ObjectMapper()
                        .readValue(jf.createJsonParser(new StringReader(str)), ArrayList.class);
                return arrList;
            } else {
                log.warn("JacksonUtil.getListsFromJsonArray error| ErrMsg: input string is null ");
                return null;
            }
        } catch (Exception e) {
            log.error("JacksonUtil.getListsFromJsonArray error| ErrMsg: " + e.getMessage(), e);
            return null;
        }

    }

    private static boolean isNotBlank(String str) {
        if(str!=null && !str.trim().isEmpty()) {
            return true;
        }
        return false;
    }


    /**
     * Convert JackSon string to Map<String, String>
     *
     * @param str - jackson string
     * @return Map<String, String>
     */
    public static Map<String, String> getMapFromJsonString(String str) {
        try {
            if (isNotBlank(str)) {
                Map<String, String> map = (Map<String, String>) new ObjectMapper()
                        .readValue(jf.createJsonParser(new StringReader(str)), Map.class);
                return map;
            } else {
                log.warn("ErrMsg: input string is null ");
                return null;
            }
        } catch (Exception e) {
            log.error("ErrMsg: " + e.getMessage(), e);
            return null;
        }
    }

    /**
     * Convert JackSon string to Map<String, String>
     *
     * @param str - jackson string
     * @return Map<String, String>
     */
    public static Map<String, Map<String, Object>> getMapMapFromJsonString(String str) {
        try {
            if (isNotBlank(str)) {
                Map<String, Map<String, Object>> map = (Map<String, Map<String, Object>>) new ObjectMapper()
                        .readValue(jf.createJsonParser(new StringReader(str)), Map.class);
                return map;
            } else {
                log.warn("ErrMsg: input string is null ");
                return null;
            }
        } catch (Exception e) {
            log.error("ErrMsg: " + e.getMessage(), e);
            return null;
        }
    }


        /**
     * Convert JackSon string to Map<String, String>
     *
     * @param str - jackson string
     * @return Map<String, String>
     */
    public static Map<String, Map<String, String>> getStringMapMapFromJsonString(String str) {
        try {
            if (isNotBlank(str)) {
                Map<String, Map<String, String>> map = (Map<String, Map<String, String>>) new ObjectMapper()
                        .readValue(jf.createJsonParser(new StringReader(str)), Map.class);
                return map;
            } else {
                log.warn("ErrMsg: input string is null ");
                return null;
            }
        } catch (Exception e) {
            log.error("ErrMsg: " + e.getMessage(), e);
            return null;
        }
    }

    /**
     * Convert Map<String, String>[] to JackSon string
     *
     * @param list Array of Map<String,String>
     * @return jackson string
     */
    public static String getJsonStringFromList(List<Map<String, String>> list) {
        try {
            StringWriter sw = new StringWriter();
            JsonGenerator gen = jf.createJsonGenerator(sw);
            new ObjectMapper().writeValue(gen, list);
            gen.flush();
            return sw.toString();
        } catch (Exception e) {
            log.error("JacksonUtil.getJsonStringFromMap error| ErrMsg: " + e.getMessage(), e);
            return null;
        }
    }

    /**
     * Convert Map<String, String> to JackSon string
     *
     * @param aMap Map
     * @return Map<String, String>
     */
    public static String getJsonStringFromMap(Map<String, String> aMap) {
        try {
            StringWriter sw = new StringWriter();
            JsonGenerator gen = jf.createJsonGenerator(sw);
            new ObjectMapper().writeValue(gen, aMap);
            gen.flush();
            return sw.toString();
        } catch (Exception e) {
            log.error("ErrMsg: " + e.getMessage(), e);
            return null;
        }
    }

    /**
     * Convert Map<String, String> to JackSon string
     *
     * @param aMap Map
     * @return Map<String, String>
     */
    public static String getJsonStringFromMapMap(Map<String, Map<String, String>> aMap) {
        try {
            StringWriter sw = new StringWriter();
            JsonGenerator gen = jf.createJsonGenerator(sw);
            new ObjectMapper().writeValue(gen, aMap);
            gen.flush();
            return sw.toString();
        } catch (Exception e) {
            log.error("ErrMsg: " + e.getMessage(), e);
            return null;
        }
    }


    /**
     * Test Stub
     *
     * @param args
     */
    public static void main(String[] args) {
        Map<String, String> map = new HashMap<String, String>();
        map.put("name", "jason");
        map.put("email", "jason@hotmail.com");

        // Convert map to JSON string
        String jsonString = JsonUtil.getJsonStringFromMap(map);
        System.out.println(jsonString);

        // Convert Json string to map
        Map<String, String> newMap = JsonUtil.getMapFromJsonString(jsonString);
        System.out.println(newMap);


        Map<String, String> anotherMap = new HashMap<String, String>();
        anotherMap.put("name", "alice");
        anotherMap.put("email", "alice@hotmail.com");

        List<Map<String, String>> mapList = new ArrayList<Map<String, String>>();
        mapList.add(map);
        mapList.add(anotherMap);
        // Convert to json string
        jsonString = JsonUtil.getJsonStringFromList(mapList);
        System.out.println(jsonString);

        // Convert Json string to list of map
        List<Map<String, String>> newMapList = JsonUtil.getListFromJsonArray(jsonString);
        System.out.println(newMapList);


    }
}
