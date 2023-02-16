package io.tapdata.connector.hazelcast.util;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientConnectionStrategyConfig;
import com.hazelcast.client.config.ConnectionRetryConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Properties;
import java.util.UUID;

/**
 * Author:Skeet
 * Date: 2023/2/7
 **/
public class HazelcastClientUtil {
    public static final String TAG = HazelcastClientUtil.class.getSimpleName();

    private final static String KEYSTORE_DIR = ".keystore";
    private final static String KEYSTORE_SUFFIX = ".keystore";
    private final static String TRUSTSTORE_SUFFIX = ".truststore";
    private final static String TAPDATA_WORKER_DIR = "TAPDATA_WORK_DIR";

    public static HazelcastInstance getClient(TapConnectionContext tapConnectionContext) throws Exception {
        if (tapConnectionContext.getConnectionConfig() == null) {
            throw new NullPointerException("ConnectionConfig cannot be null");
        }
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

        File keystoreFile = null;
        File truststoreFile = null;
        boolean ssl = true;
        boolean needCleanFile = false;
        ClientConfig config = new ClientConfig();
        Properties props = new Properties();

        if (ssl) {
            String sslKey = connectionConfig.getString("sslKey");
            String sslCA = connectionConfig.getString("sslCA");
            String sslPass = connectionConfig.getString("password");
            checkSSLConfig(sslKey, sslCA, sslPass);

            String tapdataWorkerDir = TAPDATA_WORKER_DIR;
            String keystoreDirPath = StringUtils.isBlank(tapdataWorkerDir) ? KEYSTORE_DIR : tapdataWorkerDir + File.separator + KEYSTORE_DIR;
            File keystoreDir = new File(keystoreDirPath);

            if (!keystoreDir.exists()) {
                boolean mkdir = keystoreDir.mkdir();
                if (!mkdir) {
                    throw new Exception("Connect to Hazelcast Cloud failed; Cannot create keystore dir: " + keystoreDirPath);
                }
            }

            String keyFilename = UUID.randomUUID().toString().replace("-", "");
            needCleanFile = true;

            // write keystore, truststore file to local
            String keystorePath = keystoreDir.getAbsolutePath() + File.separator + keyFilename + KEYSTORE_SUFFIX;
            keystoreFile = new File(keystorePath);
            createAndOverwriteFile(keystoreFile, Base64.getMimeDecoder().decode(sslKey), StandardCharsets.US_ASCII);
            TapLogger.info(TAG, "Create and write keystore to local file: " + keystoreFile.getAbsolutePath());


            String truststorePath = keystoreDir.getAbsolutePath() + File.separator + keyFilename + TRUSTSTORE_SUFFIX;
            truststoreFile = new File(truststorePath);
            createAndOverwriteFile(truststoreFile, Base64.getMimeDecoder().decode(sslCA), StandardCharsets.US_ASCII);
            TapLogger.info(TAG, "Create and write truststore to local file: " + truststoreFile.getAbsolutePath());

            props.setProperty("javax.net.ssl.keyStore", keystoreFile.getAbsolutePath());
            props.setProperty("javax.net.ssl.keyStorePassword", password);
            props.setProperty("javax.net.ssl.trustStore", truststoreFile.getAbsolutePath());
            props.setProperty("javax.net.ssl.trustStorePassword", password);
            config.getNetworkConfig().setSSLConfig(new SSLConfig().setEnabled(true).setProperties(props));
        }
        config.getNetworkConfig().setConnectionTimeout(10 * 1000)
                .getCloudConfig()
                .setDiscoveryToken(token)
                .setEnabled(true);
        config.setProperty("hazelcast.client.cloud.url", "https://api.viridian.hazelcast.com");
        config.setClusterName(clusterID);
        config.setConnectionStrategyConfig(new ClientConnectionStrategyConfig().setConnectionRetryConfig(new ConnectionRetryConfig().setClusterConnectTimeoutMillis(5 * 1000)));

        HazelcastInstance client = HazelcastClient.newHazelcastClient(config);

        try {
            if (needCleanFile) {
                if (keystoreFile != null && keystoreFile.exists()) {
                    keystoreFile.delete();
                }
                if (truststoreFile != null && truststoreFile.exists()) {
                    truststoreFile.delete();
                }
            }
        } catch (Exception ignore) {
        }
        return client;
    }

    public static void closeClient(HazelcastInstance client) throws Exception {
        if (client != null) {
            try {
                client.shutdown();
            } catch (HazelcastException e) {
                throw new Exception("Connection closing failure." + e.getMessage(), e);
            }
        }
    }

    private static void checkSSLConfig(String sslKey, String sslCA, String sslPass) throws Exception {
        if (StringUtils.isBlank(sslKey)) {
            throw new Exception("Config Hazelcast SSL failed; Keystore file is empty");
        }
        if (StringUtils.isBlank(sslCA)) {
            throw new Exception("Config Hazelcast SSL failed; Truststore file is empty");
        }
        if (StringUtils.isBlank(sslPass)) {
            throw new Exception("Config Hazelcast SSL failed; Store file password is empty");
        }
    }

    public static void createAndOverwriteFile(File file, byte[] content, Charset charset) throws Exception {
        if (file == null || content == null) {
            return;
        }
        if (file.exists()) {
            boolean delete = file.delete();
            if (!delete) {
                throw new RuntimeException("Delete local file failed; Path: " + file.getAbsolutePath());
            }
        }
        try (
                OutputStream outputStream = new FileOutputStream(file)
        ) {
            outputStream.write(content);
            outputStream.flush();
        } catch (Exception e) {
            throw new Exception("Write local file failed; Path: " + file.getAbsolutePath() + "; Content: " + new String(content, charset), e);
        }
    }
}
