package data.lab.ongdb.result;

import java.util.Collections;
import java.util.Map;

/**
 * @author Yc-Ma
 * @PACKAGE_NAME: data.lab.ongdb.result.MapResult
 * @Description: TODO
 * @date 2022/10/17 20:03
 */
public class MapResult {

    private static final MapResult EMPTY = new MapResult(Collections.emptyMap());

    public final Map<String, Object> value;

    public static MapResult empty() {
        return EMPTY;
    }

    public MapResult(Map<String, Object> value) {
        this.value = value;
    }

}

