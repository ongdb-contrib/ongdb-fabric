package data.lab.ongdb.common;

import org.junit.Test;

/*
 *
 * Data Lab - graph database organization.
 *
 */

/**
 * @author Yc-Ma
 * @PACKAGE_NAME: data.lab.ongdb.common
 * @Description: TODO
 * @date 2022/10/17 19:59
 */
public class ConstantTest {

    @Test
    public void loadPropertyKeys() {
        Constant.loadPropertyKeys().forEach(System.out::println);
    }
}

