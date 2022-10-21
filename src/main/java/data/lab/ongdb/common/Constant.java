package data.lab.ongdb.common;

/*
 *
 * Data Lab - graph database organization.
 *
 */

import java.io.File;
import java.io.FileInputStream;
import java.util.*;

/**
 * @author Yc-Ma
 * @PACKAGE_NAME: data.lab.ongdb.common.Constant
 * @Description: TODO
 * @date 2022/10/18 13:18
 */
public class Constant {

    public static final String FABRIC_PATH ="fabric" + File.separator + "fabric.properties";

    /**
     * @param
     * @return
     * @Description: TODO(加载配置文件)
     */
    private static Properties getIndexProperties() {
        try {
            FileInputStream inStream = new FileInputStream(new File(FABRIC_PATH));
            Properties properties = new Properties();
            properties.load(inStream);
            return properties;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String loadProperty(String key) {
        Properties properties = getIndexProperties();
        return Objects.requireNonNull(properties).getProperty(key);
    }

    public static List<String> loadPropertyKeys() {
        Properties properties = getIndexProperties();
        Enumeration e = Objects.requireNonNull(properties).propertyNames();
        List<String> list = new ArrayList<>();
        while (e.hasMoreElements()) {
            String value = (String) e.nextElement();
            list.add(value);
        }
        return list;
    }

}

