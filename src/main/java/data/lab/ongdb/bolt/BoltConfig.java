package data.lab.ongdb.bolt;

import java.util.Collections;
import java.util.Map;

import static data.lab.ongdb.util.Util.toBoolean;

/**
 * @author Yc-Ma
 * @PACKAGE_NAME: data.lab.ongdb.bolt.BoltConfig
 * @Description: TODO
 * @date 2022/10/17 20:02
 */
public class BoltConfig {

    private final boolean virtual;
    private final boolean addStatistics;
    private final boolean readOnly;
    private final boolean withRelationshipNodeProperties;

    public BoltConfig(Map<String, Object> configMap) {
        if (configMap == null) {
            configMap = Collections.emptyMap();
        }
        this.virtual = toBoolean(configMap.getOrDefault("virtual", true));
        this.addStatistics = toBoolean(configMap.getOrDefault("statistics", false));
        this.readOnly = toBoolean(configMap.getOrDefault("readOnly", true));
        this.withRelationshipNodeProperties = toBoolean(configMap.getOrDefault("withRelationshipNodeProperties", false));
    }

    public boolean isVirtual() {
        return virtual;
    }

    public boolean isAddStatistics() {
        return addStatistics;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public boolean isWithRelationshipNodeProperties() {
        return withRelationshipNodeProperties;
    }
}
