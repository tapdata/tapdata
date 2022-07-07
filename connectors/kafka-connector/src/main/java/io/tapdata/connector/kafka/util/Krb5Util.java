package io.tapdata.connector.kafka.util;

import io.tapdata.entity.logger.TapLogger;
import org.apache.commons.lang3.StringUtils;
import sun.security.krb5.Config;
import sun.security.krb5.KrbException;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Kerberos 工具类
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/11/17 下午5:04 Create
 */
public class Krb5Util {

    public static final String TAG = Krb5Util.class.getSimpleName();

    private static String decodeConf(String base64Conf) {
        byte[] bytes = Base64.getUrlDecoder().decode(base64Conf);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static String storeDir() {
        String dir = System.getenv("TAPDATA_WORK_DIR");
        if (null == dir) {
            dir = ".";
        }
        return paths(dir, "krb5");
    }

    private static String paths(String... paths) {
        return String.join(File.separator, paths);
    }

    /**
     * 保存文件
     *
     * @param data         数据
     * @param savePath     保存路径
     * @param deleteExists 是否存在删除
     * @throws IOException 异常
     */
    private static void save(byte[] data, String savePath, boolean deleteExists) throws IOException {
        File file = new File(savePath);
        if (file.exists()) {
            if (!deleteExists) {
                throw new RuntimeException("Save config is exists: " + savePath);
            }
            file.delete();
        } else {
            File dir = file.getParentFile();
            if (!dir.exists()) {
                dir.mkdirs();
            }
        }
        try (FileOutputStream fos = new FileOutputStream(file)) {
            fos.write(data);
        }
    }

    private static Map<String, Map<String, String>> getRealms(String krb5Conf) {
        try {
            String[] kv;
            boolean inRealms = false, begin = true;
            Map<String, String> kvMap = null;
            Map<String, Map<String, String>> map = new HashMap<>();
            for (String line : krb5Conf.split("\n")) {
                if (line.startsWith("#")) continue;
                line = line.replaceAll("#.*", "").trim();
                if (line.isEmpty()) continue;

                if (!inRealms) {
                    if (Pattern.matches("^[\\[]*\\[realms].*", line)) {
                        inRealms = true;
                    }
                    continue;
                } else if (line.startsWith("[")) {
                    break;
                }

                if (begin) {
                    kvMap = new HashMap<>();
                    map.put(line.replaceAll("([^\\s]+).*", "$1"), kvMap);
                    begin = false;
                    continue;
                } else if (line.contains("}")) {
                    begin = true;
                    continue;
                }
                kv = line.split("=");
                if (kv.length != 2) continue;
                kvMap.put(kv[0].trim(), kv[1].trim());
            }
            return map;
        } catch (Exception e) {
            throw new RuntimeException("Parse [realms] failed: " + e.getMessage(), e);
        }
    }

    /**
     * 获取配置
     *
     * @param krb5Path      授权路径
     * @param krb5Principal 主体
     * @return 配置
     */
    private static String saslJaasConfig(String krb5Path, String krb5Principal) {
        return "com.sun.security.auth.module.Krb5LoginModule required\n" +
                "    useKeyTab=true\n" +
                "    storeKey=true\n" +
                "    useTicketCache=true\n" +
                "    keyTab=\"" + keytabPath(krb5Path) + "\"\n" +
                "    principal=\"" + krb5Principal + "\";";
    }

    /**
     * 获取密钥路径
     *
     * @param dir 配置目录
     * @return 密钥路径
     */
    public static String keytabPath(String dir) {
        return paths(dir, "krb5.keytab");
    }

    /**
     * 获取配置路径
     *
     * @param dir 配置目录
     * @return 配置路径
     */
    public static String confPath(String dir) {
        return paths(dir, "krb5.conf");
    }

    /**
     * 检查配置域
     *
     * @param conf 配置
     * @throws UnknownHostException 异常
     */
    public static void checkKDCDomains(String conf) throws UnknownHostException {
        Map<String, Map<String, String>> realms = getRealms(conf);
        for (Map<String, String> realm : realms.values()) {
            for (Map.Entry<String, String> en : realm.entrySet()) {
                if (StringUtils.containsAny(en.getKey(), "kdc", "master_kdc", "admin_server", "default_domain")
                        && null != en.getValue()) {
                    String s = en.getValue().split(":")[0].trim();
                    if (!s.isEmpty()) {
                        InetAddress.getAllByName(s);
                    }
                }
            }
        }
    }

    /**
     * 检查配置域
     *
     * @param base64Conf base64配置
     * @throws UnknownHostException 异常
     */
    public static void checkKDCDomainsBase64(String base64Conf) throws UnknownHostException {
        String conf = decodeConf(base64Conf);
        checkKDCDomains(conf);
    }

    /**
     * 根据类别保存
     *
     * @param catalog      类别
     * @param keytab       密钥
     * @param conf         配置
     * @param deleteExists 存在删除
     * @return 配置路径
     */
    public static String saveByCatalog(String catalog, String keytab, String conf, boolean deleteExists) {
        byte[] bytes;
        String savePath = null;
        String dir = paths(storeDir(), catalog);
        try {
            savePath = keytabPath(dir);
            bytes = Base64.getDecoder().decode(keytab);
            save(bytes, savePath, deleteExists);
        } catch (Exception e) {
            throw new RuntimeException("Save kerberos keytab failed: " + savePath, e);
        }
        try {
            savePath = confPath(dir);
            bytes = Base64.getDecoder().decode(conf);
            save(bytes, savePath, deleteExists);
        } catch (Exception e) {
            throw new RuntimeException("Save kerberos conf failed: " + savePath, e);
        }
        return dir;
    }

    /**
     * 更新 Kafka 配置
     *
     * @param serviceName 服务名
     * @param principal   主体
     * @param krb5Path    授权配置目录
     * @param krb5Conf    授权配置
     * @param conf        kafka配置
     */
    public static void updateKafkaConf(String serviceName, String principal, String krb5Path, String krb5Conf, Map<String, Object> conf) {
        try {
            String krb5ConfPath = confPath(krb5Path);
            String saslJaasConfig = saslJaasConfig(krb5Path, principal);

            conf.put("security.protocol", "SASL_PLAINTEXT");
            conf.put("sasl.mechanism", "GSSAPI");
            conf.put("sasl.kerberos.service.name", serviceName);
            conf.put("sasl.jaas.config", saslJaasConfig);
            System.setProperty("java.security.krb5.conf", krb5ConfPath);

            String realm = null;
            if (null != principal) {
                String[] arr = principal.split("@");
                if (arr.length == 2) realm = arr[1].trim();
            }
            if (null == realm || realm.isEmpty()) {
                TapLogger.warn(TAG, "Parse krb5 realm failed: " + principal);
                return;
            }

            krb5Conf = decodeConf(krb5Conf);
            Map<String, Map<String, String>> realms = getRealms(krb5Conf);
            Map<String, String> currentRealms = realms.get(realm);
            if (null != currentRealms && currentRealms.containsKey("kdc")) {
                System.setProperty("java.security.krb5.realm", realm);
                System.setProperty("java.security.krb5.kdc", currentRealms.get("kdc"));

                Config.refresh();
            } else {
                TapLogger.warn(TAG, "Not found kdc in realm '{}' >> {}", realm, krb5Conf);
            }
        } catch (KrbException e) {
            throw new RuntimeException("Refresh krb5 config failed", e);
        }
    }


    public static void main(String[] args) throws Exception {
        String conf;
        try (FileInputStream fis = new FileInputStream("/Users/lhs/Downloads/krb5-test.conf")) {
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                int len;
                byte[] buf = new byte[1024];
                while (-1 != (len = fis.read(buf, 0, 1024))) {
                    baos.write(buf, 0, len);
                }
                conf = baos.toString("UTF-8");
            }
        }
        System.out.println(conf);
        checkKDCDomains(conf);
    }
}
