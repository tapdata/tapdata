package io.tapdata.connector.mrs;

import cn.hutool.core.codec.Base64;
import io.tapdata.connector.hive.HiveJdbcContext;
import io.tapdata.connector.mrs.config.MrsHive3Config;
import io.tapdata.connector.mrs.util.Krb5Util;

import java.sql.Connection;
import java.sql.SQLException;

public class MrsHive3JdbcContext extends HiveJdbcContext {

    public MrsHive3JdbcContext(MrsHive3Config mrsHive3Config) {
        super(mrsHive3Config);
    }

    public Connection getConnection() throws SQLException {
        MrsHive3Config mrsHive3Config = (MrsHive3Config) getConfig();
        if (mrsHive3Config.getKrb5()) {
            System.setProperty("java.security.krb5.conf", Krb5Util.confPath(mrsHive3Config.getKrb5Path()));
            String zkPrincipal = "zookeeper/" + getUserRealm(mrsHive3Config.getKrb5Conf());
            System.setProperty("zookeeper.server.principal", zkPrincipal);
        }
        if (mrsHive3Config.getSsl()) {
            System.setProperty("zookeeper.clientCnxnSocket", "org.apache.zookeeper.ClientCnxnSocketNetty");
            System.setProperty("zookeeper.client.secure", "true");
        }
        Connection connection = super.getConnection();
        connection.setAutoCommit(true);
        return connection;
    }

    public static String getUserRealm(String krb5Conf) {
        String serverRealm = System.getProperty("SERVER_REALM");
        String authHostName;
        if (serverRealm != null && !serverRealm.equals("")) {
            authHostName = "hadoop." + serverRealm.toLowerCase();
        } else {
            serverRealm = Krb5Util.getRealms(Base64.decodeStr(krb5Conf)).keySet().stream().findFirst().orElse("");
            if (serverRealm != null && !serverRealm.equals("")) {
                authHostName = "hadoop." + serverRealm.toLowerCase();
            } else {
                authHostName = "hadoop";
            }
        }
        return authHostName;
    }
}
