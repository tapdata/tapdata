package io.tapdata.connector.hazelcast.util;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.core.HazelcastInstance;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import org.apache.commons.lang3.StringUtils;

import java.util.Properties;

/**
 * Author:Skeet
 * Date: 2023/2/7
 **/
public class HazelcastClientUtil {
    public static final String TAG = HazelcastClientUtil.class.getSimpleName();

    public static HazelcastInstance getClient(TapConnectionContext tapConnectionContext) throws Exception {
        DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
        String clusterID = connectionConfig.getString("clusterID");
        String token = connectionConfig.getString("token");
        String password = connectionConfig.getString("password");
        if (StringUtils.isBlank(clusterID)) {
            throw new Exception("Cluster ID cannot be empty.");
        }
        if (StringUtils.isBlank(token)) {
            throw new Exception("Token cannot be empty.");
        }
        if (StringUtils.isBlank(password)) {
            throw new Exception("Password cannot be empty.");
        }

        ClassLoader classLoader = HazelcastClientUtil.class.getClassLoader();
        Properties props = new Properties();
        props.setProperty("javax.net.ssl.keyStore", classLoader.getResource("client.keystore").toURI().getPath());
        props.setProperty("javax.net.ssl.keyStorePassword", password);
        props.setProperty("javax.net.ssl.trustStore",
                classLoader.getResource("client.truststore").toURI().getPath());
        props.setProperty("javax.net.ssl.trustStorePassword", password);
        ClientConfig config = new ClientConfig();
        config.getNetworkConfig().setSSLConfig(new SSLConfig().setEnabled(true).setProperties(props));
        config.getNetworkConfig().setConnectionTimeout(10 * 1000)
                .getCloudConfig()
                .setDiscoveryToken(token)
                .setEnabled(true);
        config.setProperty("hazelcast.client.cloud.url", "https://api.viridian.hazelcast.com");
        config.setClusterName(clusterID);

        HazelcastInstance client = HazelcastClient.newHazelcastClient(config);
        TapLogger.info(TAG, "The Hazelcast cluster is connected successfully.");
        return client;
    }

    public static void closeClient(HazelcastInstance client) throws Exception {
        if (client != null) {
            try {
                client.shutdown();
            } catch (Exception e) {
                throw new Exception("Connection closing failure." + e.getMessage());
            }
        }
    }

}
