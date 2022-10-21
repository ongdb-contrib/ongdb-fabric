package data.lab.ongdb.fabric;
/*
 *
 * Data Lab - graph database organization.
 *
 */

import data.lab.ongdb.common.Constant;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.UserFunction;

import java.util.*;

/**
 * @author Yc-Ma
 * @PACKAGE_NAME: data.lab.ongdb.fabric.Functions
 * @Description: TODO
 * @date 2022/10/17 17:46
 */
public class Functions {

    @UserFunction(name = "c.ids")
    @Description("查询已经注册的服务ID")
    public List<String> cIds() {
        return Constant.loadPropertyKeys();
    }

}



