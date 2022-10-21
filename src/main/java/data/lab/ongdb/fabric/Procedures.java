package data.lab.ongdb.fabric;

import data.lab.ongdb.bolt.BoltConfig;
import data.lab.ongdb.result.MapResult;
import data.lab.ongdb.result.VirtualNode;
import data.lab.ongdb.result.VirtualRelationship;
import data.lab.ongdb.util.DatabasePool;
import data.lab.ongdb.util.Util;
import org.neo4j.driver.internal.InternalEntity;
import org.neo4j.driver.internal.logging.JULogging;
import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.summary.SummaryCounters;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Path;
import org.neo4j.driver.v1.types.Relationship;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.io.File;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.neo4j.helpers.collection.MapUtil.map;

/**
 * @author Yc-Ma
 * @PACKAGE_NAME: data.lab.ongdb.fabric.Procedures
 * @Description: TODO
 * @date 2022/10/17 17:46
 */
public class Procedures {

    /**
     * 运行环境/上下文
     */
    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    @Context
    public TerminationGuard terminationGuard;

    /**
     * @param cIds:服务ID
     * @param statement:查询
     * @param params:查询入参
     * @param config:Bolt驱动配置
     * @return
     * @Description: TODO
     */
    @Procedure(name = "c", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c(@Name("cIds") Object cIds, @Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        if (cIds instanceof ArrayList) {
            ArrayList<Object> coll = (ArrayList<Object>) cIds;
            return coll.parallelStream().flatMap((cId) -> {
                terminationGuard.check();
                try {
                    return cSingle(String.valueOf(cId), statement, params, config);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return Stream.of();
            });
        } else if (cIds instanceof String) {
            return cSingle(String.valueOf(cIds), statement, params, config);
        } else {
            throw new IllegalAccessError("C id must be STRING or LIST!");
        }
    }

    public Stream<MapResult> cSingle(@Name("cId") String cId, @Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        if (params == null) {
            params = Collections.emptyMap();
        }
        BoltConfig boltConfig = new BoltConfig(config);

        try {
            final Driver driver = DatabasePool.getInstance(cId);
            final Session session = driver.session();
            final Stream<MapResult> result;
            if (boltConfig.isAddStatistics()) {
                result = Stream.of(new MapResult(toMap(runStatement(statement, session, params, boltConfig).summary().counters())));
            } else {
                result = getRowResultStream(session, params, statement, boltConfig);
            }
            return result.onClose(session::close);
        } catch (URISyntaxException e) {
            throw new RuntimeException("It's not possible to create a connection due to: " + e.getMessage());
        } catch (IllegalAccessError e) {
            throw new IllegalAccessError(e.getMessage());
        } catch (org.neo4j.driver.v1.exceptions.ClientException e) {
            String message = e.getMessage();
            String notice = "Graph:" + cId + ",There is no procedure or function with the name `" + statement + "` registered for this database instance. Please ensure you've spelled the procedure name correctly and that the procedure or function is properly deployed.";
            if (isProcedureOrFunction(message)) {
                log.warn(notice);
                return Stream.of();
            } else {
                throw new IllegalAccessException(message);
            }
        }
    }

    private boolean isProcedureOrFunction(String message) {
        return (message.contains("function") || message.contains("procedure"));
    }

    @Procedure(name = "c1", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c1(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c1", statement, params, config);
    }

    @Procedure(name = "c2", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c2(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c2", statement, params, config);
    }

    @Procedure(name = "c3", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c3(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c3", statement, params, config);
    }

    @Procedure(name = "c4", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c4(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c4", statement, params, config);
    }

    @Procedure(name = "c5", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c5(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c5", statement, params, config);
    }

    @Procedure(name = "c6", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c6(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c6", statement, params, config);
    }

    @Procedure(name = "c7", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c7(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c7", statement, params, config);
    }

    @Procedure(name = "c8", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c8(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c8", statement, params, config);
    }

    @Procedure(name = "c9", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c9(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c9", statement, params, config);
    }

    @Procedure(name = "c10", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c10(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c10", statement, params, config);
    }

    @Procedure(name = "c11", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c11(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c11", statement, params, config);
    }

    @Procedure(name = "c12", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c12(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c12", statement, params, config);
    }

    @Procedure(name = "c13", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c13(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c13", statement, params, config);
    }

    @Procedure(name = "c14", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c14(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c14", statement, params, config);
    }

    @Procedure(name = "c15", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c15(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c15", statement, params, config);
    }

    @Procedure(name = "c16", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c16(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c16", statement, params, config);
    }

    @Procedure(name = "c17", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c17(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c17", statement, params, config);
    }

    @Procedure(name = "c18", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c18(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c18", statement, params, config);
    }

    @Procedure(name = "c19", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c19(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c19", statement, params, config);
    }

    @Procedure(name = "c20", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c20(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c20", statement, params, config);
    }

    @Procedure(name = "c21", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c21(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c21", statement, params, config);
    }

    @Procedure(name = "c22", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c22(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c22", statement, params, config);
    }

    @Procedure(name = "c23", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c23(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c23", statement, params, config);
    }

    @Procedure(name = "c24", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c24(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c24", statement, params, config);
    }

    @Procedure(name = "c25", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c25(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c25", statement, params, config);
    }

    @Procedure(name = "c26", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c26(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c26", statement, params, config);
    }

    @Procedure(name = "c27", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c27(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c27", statement, params, config);
    }

    @Procedure(name = "c28", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c28(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c28", statement, params, config);
    }

    @Procedure(name = "c29", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c29(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c29", statement, params, config);
    }

    @Procedure(name = "c30", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c30(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c30", statement, params, config);
    }

    @Procedure(name = "c31", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c31(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c31", statement, params, config);
    }

    @Procedure(name = "c32", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c32(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c32", statement, params, config);
    }

    @Procedure(name = "c33", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c33(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c33", statement, params, config);
    }

    @Procedure(name = "c34", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c34(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c34", statement, params, config);
    }

    @Procedure(name = "c35", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c35(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c35", statement, params, config);
    }

    @Procedure(name = "c36", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c36(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c36", statement, params, config);
    }

    @Procedure(name = "c37", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c37(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c37", statement, params, config);
    }

    @Procedure(name = "c38", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c38(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c38", statement, params, config);
    }

    @Procedure(name = "c39", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c39(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c39", statement, params, config);
    }

    @Procedure(name = "c40", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c40(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c40", statement, params, config);
    }

    @Procedure(name = "c41", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c41(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c41", statement, params, config);
    }

    @Procedure(name = "c42", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c42(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c42", statement, params, config);
    }

    @Procedure(name = "c43", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c43(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c43", statement, params, config);
    }

    @Procedure(name = "c44", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c44(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c44", statement, params, config);
    }

    @Procedure(name = "c45", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c45(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c45", statement, params, config);
    }

    @Procedure(name = "c46", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c46(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c46", statement, params, config);
    }

    @Procedure(name = "c47", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c47(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c47", statement, params, config);
    }

    @Procedure(name = "c48", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c48(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c48", statement, params, config);
    }

    @Procedure(name = "c49", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c49(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c49", statement, params, config);
    }

    @Procedure(name = "c50", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c50(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c50", statement, params, config);
    }

    @Procedure(name = "c51", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c51(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c51", statement, params, config);
    }

    @Procedure(name = "c52", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c52(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c52", statement, params, config);
    }

    @Procedure(name = "c53", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c53(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c53", statement, params, config);
    }

    @Procedure(name = "c54", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c54(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c54", statement, params, config);
    }

    @Procedure(name = "c55", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c55(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c55", statement, params, config);
    }

    @Procedure(name = "c56", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c56(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c56", statement, params, config);
    }

    @Procedure(name = "c57", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c57(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c57", statement, params, config);
    }

    @Procedure(name = "c58", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c58(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c58", statement, params, config);
    }

    @Procedure(name = "c59", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c59(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c59", statement, params, config);
    }

    @Procedure(name = "c60", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c60(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c60", statement, params, config);
    }

    @Procedure(name = "c61", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c61(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c61", statement, params, config);
    }

    @Procedure(name = "c62", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c62(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c62", statement, params, config);
    }

    @Procedure(name = "c63", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c63(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c63", statement, params, config);
    }

    @Procedure(name = "c64", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c64(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c64", statement, params, config);
    }

    @Procedure(name = "c65", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c65(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c65", statement, params, config);
    }

    @Procedure(name = "c66", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c66(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c66", statement, params, config);
    }

    @Procedure(name = "c67", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c67(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c67", statement, params, config);
    }

    @Procedure(name = "c68", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c68(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c68", statement, params, config);
    }

    @Procedure(name = "c69", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c69(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c69", statement, params, config);
    }

    @Procedure(name = "c70", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c70(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c70", statement, params, config);
    }

    @Procedure(name = "c71", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c71(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c71", statement, params, config);
    }

    @Procedure(name = "c72", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c72(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c72", statement, params, config);
    }

    @Procedure(name = "c73", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c73(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c73", statement, params, config);
    }

    @Procedure(name = "c74", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c74(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c74", statement, params, config);
    }

    @Procedure(name = "c75", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c75(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c75", statement, params, config);
    }

    @Procedure(name = "c76", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c76(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c76", statement, params, config);
    }

    @Procedure(name = "c77", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c77(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c77", statement, params, config);
    }

    @Procedure(name = "c78", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c78(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c78", statement, params, config);
    }

    @Procedure(name = "c79", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c79(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c79", statement, params, config);
    }

    @Procedure(name = "c80", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c80(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c80", statement, params, config);
    }

    @Procedure(name = "c81", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c81(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c81", statement, params, config);
    }

    @Procedure(name = "c82", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c82(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c82", statement, params, config);
    }

    @Procedure(name = "c83", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c83(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c83", statement, params, config);
    }

    @Procedure(name = "c84", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c84(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c84", statement, params, config);
    }

    @Procedure(name = "c85", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c85(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c85", statement, params, config);
    }

    @Procedure(name = "c86", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c86(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c86", statement, params, config);
    }

    @Procedure(name = "c87", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c87(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c87", statement, params, config);
    }

    @Procedure(name = "c88", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c88(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c88", statement, params, config);
    }

    @Procedure(name = "c89", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c89(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c89", statement, params, config);
    }

    @Procedure(name = "c90", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c90(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c90", statement, params, config);
    }

    @Procedure(name = "c91", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c91(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c91", statement, params, config);
    }

    @Procedure(name = "c92", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c92(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c92", statement, params, config);
    }

    @Procedure(name = "c93", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c93(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c93", statement, params, config);
    }

    @Procedure(name = "c94", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c94(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c94", statement, params, config);
    }

    @Procedure(name = "c95", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c95(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c95", statement, params, config);
    }

    @Procedure(name = "c96", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c96(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c96", statement, params, config);
    }

    @Procedure(name = "c97", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c97(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c97", statement, params, config);
    }

    @Procedure(name = "c98", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c98(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c98", statement, params, config);
    }

    @Procedure(name = "c99", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c99(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c99", statement, params, config);
    }

    @Procedure(name = "c100", mode = Mode.READ)
    @Description("Fabric c")
    public Stream<MapResult> c100(@Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return c("c100", statement, params, config);
    }

    private StatementResult runStatement(@Name("statement") String statement, Session session, Map<String, Object> finalParams, BoltConfig boltConfig) {
        return boltConfig.isReadOnly()
                ? session.readTransaction((Transaction tx) -> tx.run(statement, finalParams))
                : session.writeTransaction((Transaction tx) -> tx.run(statement, finalParams));
    }

    private Stream<MapResult> getRowResultStream(Session session, Map<String, Object> params, String statement, BoltConfig boltConfig) {
        Map<Long, org.neo4j.graphdb.Node> nodesCache = new ConcurrentHashMap<>();
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(runStatement(statement, session, params, boltConfig), 0), true)
                .map(record -> new MapResult(record.asMap(value -> convert(session, value, boltConfig, nodesCache))));
    }

    private Object convert(Session session, Object entity, BoltConfig boltConfig, Map<Long, org.neo4j.graphdb.Node> nodeCache) {
        if (entity instanceof Value) {
            return convert(session, ((Value) entity).asObject(), boltConfig, nodeCache);
        }
        if (entity instanceof Node) {
            return toNode(entity, boltConfig, nodeCache);
        }
        if (entity instanceof Relationship) {
            return toRelationship(session, entity, boltConfig, nodeCache);
        }
        if (entity instanceof Path) {
            return toPath(session, entity, boltConfig, nodeCache);
        }
        if (entity instanceof Collection) {
            return toCollection(session, (Collection) entity, boltConfig, nodeCache);
        }
        if (entity instanceof Map) {
            return toMap(session, (Map<String, Object>) entity, boltConfig, nodeCache);
        }
        return entity;
    }

    private Object toMap(Session session, Map<String, Object> entity, BoltConfig boltConfig, Map<Long, org.neo4j.graphdb.Node> nodeCache) {
        return entity.entrySet().stream()
                .map(entry -> new AbstractMap.SimpleEntry(entry.getKey(), convert(session, entry.getValue(), boltConfig, nodeCache)))
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
    }

    private Object toCollection(Session session, Collection entity, BoltConfig boltConfig, Map<Long, org.neo4j.graphdb.Node> nodeCache) {
        return entity.stream()
                .map(elem -> convert(session, elem, boltConfig, nodeCache))
                .collect(Collectors.toList());
    }

    private Object toNode(Object value, BoltConfig boltConfig, Map<Long, org.neo4j.graphdb.Node> nodesCache) {
        Node node;
        if (value instanceof Value) {
            node = ((InternalEntity) value).asValue().asNode();
        } else if (value instanceof Node) {
            node = (Node) value;
        } else {
            throw getUnsupportedConversionException(value);
        }
        if (boltConfig.isVirtual()) {
            final Label[] labels = getLabelsAsArray(node);
            return nodesCache.computeIfAbsent(node.id(), (id) -> new VirtualNode(id, labels, node.asMap(), db));
        } else {
            return Util.map("entityType", "NODE", "labels", node.labels(), "id", node.id(), "properties", node.asMap());
        }
    }

    private Object toRelationship(Session session, Object value, BoltConfig boltConfig, Map<Long, org.neo4j.graphdb.Node> nodesCache) {
        Relationship relationship;
        if (value instanceof Value) {
            relationship = ((InternalEntity) value).asValue().asRelationship();
        } else if (value instanceof Relationship) {
            relationship = (Relationship) value;
        } else {
            throw getUnsupportedConversionException(value);
        }
        if (boltConfig.isVirtual()) {
            final org.neo4j.graphdb.Node start;
            final org.neo4j.graphdb.Node end;
            if (boltConfig.isWithRelationshipNodeProperties()) {
                final Function<Long, org.neo4j.graphdb.Node> retrieveNode = (id) -> {
                    final Node node = session.readTransaction(tx -> tx.run("MATCH (n) WHERE id(n) = $id RETURN n",
                            Collections.singletonMap("id", id)))
                            .single()
                            .get("n")
                            .asNode();
                    final Label[] labels = getLabelsAsArray(node);
                    return new VirtualNode(id, labels, node.asMap(), db);
                };
                start = nodesCache.computeIfAbsent(relationship.startNodeId(), retrieveNode);
                end = nodesCache.computeIfAbsent(relationship.endNodeId(), retrieveNode);
            } else {
                start = nodesCache.getOrDefault(relationship.startNodeId(), new VirtualNode(relationship.startNodeId(), db));
                end = nodesCache.getOrDefault(relationship.endNodeId(), new VirtualNode(relationship.endNodeId(), db));
            }
            return new VirtualRelationship(relationship.id(), start, end, RelationshipType.withName(relationship.type()), relationship.asMap());
        } else {
            return Util.map("entityType", "RELATIONSHIP", "type", relationship.type(), "id", relationship.id(), "start", relationship.startNodeId(), "end", relationship.endNodeId(), "properties", relationship.asMap());
        }
    }

    private ClassCastException getUnsupportedConversionException(Object value) {
        return new ClassCastException("Conversion from class " + value.getClass().getName() + " not supported");
    }

    private Label[] getLabelsAsArray(Node node) {
        return StreamSupport.stream(node.labels().spliterator(), false)
                .map(Label::label)
                .collect(Collectors.toList())
                .toArray(new Label[0]);
    }

    private Object toPath(Session session, Object value, BoltConfig boltConfig, Map<Long, org.neo4j.graphdb.Node> nodesCache) {
        List<Object> entityList = new LinkedList<>();
        Path path;
        if (value instanceof Value) {
            path = ((InternalEntity) value).asValue().asPath();
        } else if (value instanceof Path) {
            path = (Path) value;
        } else {
            throw getUnsupportedConversionException(value);
        }
        path.forEach(p -> {
            entityList.add(toNode(p.start(), boltConfig, nodesCache));
            entityList.add(toRelationship(session, p.relationship(), boltConfig, nodesCache));
            entityList.add(toNode(p.end(), boltConfig, nodesCache));
        });
        return entityList;
    }

    private Map<String, Object> toMap(SummaryCounters resultSummary) {
        return map(
                "nodesCreated", resultSummary.nodesCreated(),
                "nodesDeleted", resultSummary.nodesDeleted(),
                "labelsAdded", resultSummary.labelsAdded(),
                "labelsRemoved", resultSummary.labelsRemoved(),
                "relationshipsCreated", resultSummary.relationshipsCreated(),
                "relationshipsDeleted", resultSummary.relationshipsDeleted(),
                "propertiesSet", resultSummary.propertiesSet(),
                "constraintsAdded", resultSummary.constraintsAdded(),
                "constraintsRemoved", resultSummary.constraintsRemoved(),
                "indexesAdded", resultSummary.indexesAdded(),
                "indexesRemoved", resultSummary.indexesRemoved()
        );
    }

    private Config toDriverConfig(Object driverConfig) {
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

