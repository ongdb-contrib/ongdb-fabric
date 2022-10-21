package data.lab.ongdb.util;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Yc-Ma
 * @PACKAGE_NAME: data.lab.ongdb.util.ApocConfiguration
 * @Description: TODO
 * @date 2022/10/17 20:03
 */
public class ApocConfiguration {

    private static Map<String, Object> apocConfig = new HashMap<>(10);

    public static boolean isEnabled(String key) {
        return Util.toBoolean(apocConfig.getOrDefault(key, false));
    }

    public static <T> T get(String key, T defaultValue) {
        return (T) apocConfig.getOrDefault(key, defaultValue);
    }

}



