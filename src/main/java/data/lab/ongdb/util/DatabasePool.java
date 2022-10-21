package data.lab.ongdb.util;

/*
 *
 * Data Lab - graph database organization.
 *
 */

import data.lab.ongdb.common.Constant;
import org.neo4j.driver.internal.logging.JULogging;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;

import java.io.File;
import java.net.URISyntaxException;
import java.sql.*;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static data.lab.ongdb.util.Util.map;

/**
 * @author Yc-Ma
 * @PACKAGE_NAME: data.lab.ongdb.util.DatabasePool
 * @Description: TODO(Database connection pool util)
 * @date 2022/10/17 20:19
 */
public final class DatabasePool {

    /**
     * Database connection pool storage management
     **/
    private static final Map<String, Driver> MANAGER_MAP = new Hashtable<>();

    /**
     * @param dbName:数据库名称
     * @return
     * @Description: TODO(获取连接池 - 连接池的配置从mysql.properties文件读取)
     */
    public static Driver getInstance(String dbName) throws URISyntaxException,IllegalAccessError {
        if (MANAGER_MAP.containsKey(dbName)) {
            return MANAGER_MAP.get(dbName);
        } else {
            Driver instance = buildPool(dbName);
            MANAGER_MAP.put(dbName, instance);
            return instance;
        }
    }

    private static Driver buildPool(String cId) throws URISyntaxException,IllegalAccessError {
        Config driverConfig = toDriverConfig(Collections.emptyMap().getOrDefault("driverConfig", map()));
        String bolt = Constant.loadProperty(cId);
        if ("".equals(bolt) || bolt == null) {
            throw new IllegalAccessError(cId + " is not in "+Constant.FABRIC_PATH+"!");
        }
        UriResolver uri = new UriResolver(bolt, "bolt");
        uri.initialize();
        return GraphDatabase.driver(uri.getConfiguredUri(), uri.getToken(), driverConfig);
    }

    public static AssessUtil assessUtil() {
        return new AssessUtil();
    }

    public static class AssessUtil {

        public void close(Connection conn) {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        public void close(PreparedStatement pre) {
            if (pre != null) {
                try {
                    pre.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        public void close(Statement sta) {
            if (sta != null) {
                try {
                    sta.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        public void close(ResultSet result) {
            if (result != null) {
                try {
                    result.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        public void close(Connection conn, PreparedStatement pre) {
            close(conn);
            close(pre);
        }

        public void close(Connection conn, Statement sta) {
            close(conn);
            close(sta);
        }

        public void close(Connection conn, PreparedStatement pre, ResultSet result) {
            close(conn);
            close(pre);
            close(result);
        }

        public void close(Connection conn, Statement sta, ResultSet result) {
            close(conn);
            close(sta);
            close(result);
        }
    }

    private static Config toDriverConfig(Object driverConfig) {
        Map<String, Object> driverConfMap = (Map<String, Object>) driverConfig;
        String logging = (String) driverConfMap.getOrDefault("logging", "INFO");
        boolean encryption = (boolean) driverConfMap.getOrDefault("encryption", true);
        boolean logLeakedSessions = (boolean) driverConfMap.getOrDefault("logLeakedSessions", true);
        Long maxIdleConnectionPoolSize = (Long) driverConfMap.getOrDefault("maxIdleConnectionPoolSize", 10L);
        Long idleTimeBeforeConnectionTest = (Long) driverConfMap.getOrDefault("idleTimeBeforeConnectionTest", -1L);
        String trustStrategy = (String) driverConfMap.getOrDefault("trustStrategy", "TRUST_ALL_CERTIFICATES");
        Long routingFailureLimit = (Long) driverConfMap.getOrDefault("routingFailureLimit", 1L);
        Long routingRetryDelayMillis = (Long) driverConfMap.getOrDefault("routingRetryDelayMillis", 5000L);
        Long connectionTimeoutMillis = (Long) driverConfMap.getOrDefault("connectionTimeoutMillis", 5000L);
        Long maxRetryTimeMs = (Long) driverConfMap.getOrDefault("maxRetryTimeMs", 30000L);
        Config.ConfigBuilder config = Config.build();
        config.withLogging(new JULogging(Level.parse(logging)));
        if (!encryption) {
            config.withoutEncryption();
        }
        config.withTrustStrategy(Config.TrustStrategy.trustAllCertificates());
        if (!logLeakedSessions) {
            config.withoutEncryption();
        }
        config.withMaxIdleSessions(maxIdleConnectionPoolSize.intValue());
        config.withConnectionLivenessCheckTimeout(idleTimeBeforeConnectionTest, TimeUnit.MILLISECONDS);
        config.withRoutingFailureLimit(routingFailureLimit.intValue());
        config.withConnectionTimeout(connectionTimeoutMillis, TimeUnit.MILLISECONDS);
        config.withRoutingRetryDelay(routingRetryDelayMillis, TimeUnit.MILLISECONDS);
        config.withMaxTransactionRetryTime(maxRetryTimeMs, TimeUnit.MILLISECONDS);
        if (trustStrategy.equals("TRUST_ALL_CERTIFICATES")) {
            config.withTrustStrategy(Config.TrustStrategy.trustAllCertificates());
        } else if (trustStrategy.equals("TRUST_SYSTEM_CA_SIGNED_CERTIFICATES")) {
            config.withTrustStrategy(Config.TrustStrategy.trustSystemCertificates());
        } else {
            File file = new File(trustStrategy);
            config.withTrustStrategy(Config.TrustStrategy.trustCustomCertificateSignedBy(file));
        }
        return config.toConfig();
    }
}

