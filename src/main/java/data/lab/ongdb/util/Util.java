package data.lab.ongdb.util;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * @author Yc-Ma
 * @PACKAGE_NAME: data.lab.ongdb.util.Util
 * @Description: TODO
 * @date 2022/10/17 20:02
 */
public class Util {

    public static boolean toBoolean(Object value) {
        if ((value == null || value instanceof Number && (((Number) value).longValue()) == 0L || value instanceof String && (value.equals("") || ((String) value).equalsIgnoreCase("false") || ((String) value).equalsIgnoreCase("no") || ((String) value).equalsIgnoreCase("0")) || value instanceof Boolean && value.equals(false))) {
            return false;
        }
        return true;
    }

    public static List<String> labelStrings(Node n) {
        return StreamSupport.stream(n.getLabels().spliterator(), false).map(Label::name).sorted().collect(Collectors.toList());
    }

    public static <T> Map<String, T> map(T... values) {
        Map<String, T> map = new LinkedHashMap<>();
        for (int i = 0; i < values.length; i += 2) {
            if (values[i] == null) {
                continue;
            }
            map.put(values[i].toString(), values[i + 1]);
        }
        return map;
    }

}

