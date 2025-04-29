package com.tapdata.tm.mcp.mongodb;

import jakarta.xml.bind.DatatypeConverter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import javax.net.ssl.*;
import java.io.*;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * SSL工具类，用于处理SSL证书和私钥相关操作
 */
public class SSLUtil {

  private static final String SSL_PROTOCOL = "SSL";
  private static final String KEY_STORE_TYPE = "JKS";
  private static final String KEY_MANAGER_ALGORITHM = "SunX509";
  private static final String CERTIFICATE_TYPE = "X.509";
  private static final String RSA_ALGORITHM = "RSA";
  private static final String EMPTY_PASSWORD = "";
  private static final String DEFAULT_ALIAS = "";

  /**
   * 创建SSL上下文
   *
   * @param privateKey 私钥
   * @param certificates 证书列表
   * @param trustCertificates 信任证书列表
   * @param password 密码
   * @return SSLContext实例
   * @throws Exception 如果创建过程中发生错误
   */
  public static SSLContext createSSLContext(String privateKey, List<String> certificates,
                                            List<String> trustCertificates, String password) throws Exception {
    String finalPassword = password != null ? password : EMPTY_PASSWORD;

    SSLContext sslContext = SSLContext.getInstance(SSL_PROTOCOL);
    TrustManager[] trustManagers = createTrustManagers(trustCertificates, finalPassword);
    KeyManager[] keyManagers = createKeyManagers(privateKey, certificates, finalPassword);

    sslContext.init(keyManagers, trustManagers, null);
    return sslContext;
  }

  /**
   * 创建信任管理器
   */
  public static TrustManager[] createTrustManagers(List<String> certificates, String password) throws Exception {
    X509Certificate[] x509Certificates = createCertificates(certificates);

    if (x509Certificates == null || x509Certificates.length == 0) {
      return new TrustManager[]{new X509TrustManager() {
        @Override
        public X509Certificate[] getAcceptedIssuers() {
          return new X509Certificate[0];
        }

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType) {
          // 信任所有客户端证书
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) {
          // 信任所有服务器证书
        }
      }};
    }

    KeyStore trustStore = KeyStore.getInstance(KEY_STORE_TYPE);
    trustStore.load(null, password.toCharArray());
    trustStore.setCertificateEntry(DEFAULT_ALIAS, x509Certificates[0]);

    TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(
            TrustManagerFactory.getDefaultAlgorithm());
    trustManagerFactory.init(trustStore);

    return trustManagerFactory.getTrustManagers();
  }

  /**
   * 创建密钥管理器
   */
  public static KeyManager[] createKeyManagers(String privateKey, List<String> certificates,
                                               String password) throws Exception {
    KeyStore keystore = createKeyStore(privateKey, certificates, password);
    KeyManagerFactory kmf = KeyManagerFactory.getInstance(KEY_MANAGER_ALGORITHM);
    kmf.init(keystore, password.toCharArray());
    return kmf.getKeyManagers();
  }

  /**
   * 从PEM文件创建KeyStore
   */
  public static KeyStore createKeyStore(String privateKey, List<String> certificates,
                                        String password) throws Exception {
    X509Certificate[] x509Certificates = createCertificates(certificates);

    byte[] keyBytes = DatatypeConverter.parseBase64Binary(privateKey);
    java.security.Security.addProvider(new BouncyCastleProvider());
    PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(keyBytes);
    KeyFactory factory = KeyFactory.getInstance(RSA_ALGORITHM);
    PrivateKey key = factory.generatePrivate(spec);

    KeyStore keystore = KeyStore.getInstance(KEY_STORE_TYPE);
    keystore.load(null, null);
    keystore.setKeyEntry(DEFAULT_ALIAS, key, password.toCharArray(), x509Certificates);

    return keystore;
  }

  /**
   * 从字符串中提取私钥
   */
  public static String retrievePrivateKey(String privatePEMString) {
    if (StringUtils.isBlank(privatePEMString)) {
      return "";
    }

    StringBuilder result = new StringBuilder();
    String[] lines = privatePEMString.split("\\r?\\n");
    boolean isPrivateKeyContent = false;

    for (String line : lines) {
      if (line.contains("BEGIN") && line.contains("PRIVATE KEY")) {
        isPrivateKeyContent = true;
        continue;
      }
      if (line.contains("END") && line.contains("PRIVATE KEY")) {
        break;
      }
      if (isPrivateKeyContent) {
        result.append(line);
      }
    }

    return result.toString();
  }

  /**
   * 从字符串中提取证书
   */
  public static List<String> retrieveCertificates(String certificatePEMString) {
    List<String> certificates = new ArrayList<>();
    if (StringUtils.isBlank(certificatePEMString)) {
      return certificates;
    }

    String[] lines = certificatePEMString.split("\\r?\\n");
    StringBuilder currentCertificate = new StringBuilder();
    boolean isCollecting = false;

    for (String line : lines) {
      if (line.contains("BEGIN CERTIFICATE")) {
        isCollecting = true;
        currentCertificate = new StringBuilder();
        continue;
      }
      if (line.contains("END CERTIFICATE")) {
        if (currentCertificate.length() > 0) {
          certificates.add(currentCertificate.toString());
        }
        isCollecting = false;
        continue;
      }
      if (isCollecting && !line.startsWith("----")) {
        currentCertificate.append(line);
      }
    }

    return certificates;
  }

  private static X509Certificate[] createCertificates(List<String> certificates) throws Exception {
    if (CollectionUtils.isEmpty(certificates)) {
      return null;
    }

    List<X509Certificate> result = new ArrayList<>();
    for (String certificate : certificates) {
      byte[] certBytes = DatatypeConverter.parseBase64Binary(certificate);
      CertificateFactory factory = CertificateFactory.getInstance(CERTIFICATE_TYPE);
      result.add( (X509Certificate) factory.generateCertificate(new ByteArrayInputStream(certBytes)));
    }

    return result.toArray(new X509Certificate[0]);
  }
}
