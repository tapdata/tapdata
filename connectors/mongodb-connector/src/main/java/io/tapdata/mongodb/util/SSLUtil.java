package io.tapdata.mongodb.util;

import io.tapdata.kit.EmptyKit;
import org.apache.commons.collections4.CollectionUtils;

import javax.net.ssl.*;
import javax.xml.bind.DatatypeConverter;
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

public class SSLUtil {

  public static SSLContext createSSLContext(String privateKey, List<String> certificates, List<String> trustCertificates, String password) throws Exception {
    SSLContext sslContext = SSLContext.getInstance("SSL");
    if (password == null) {
      password = "";
    }

    TrustManager[] trustManagers = createTrustManagers(trustCertificates, password);
    KeyManager[] keyManagers = createKeyManagers(privateKey, certificates, password);
    sslContext.init(keyManagers,
      trustManagers,
      null);

    return sslContext;
  }

  public static TrustManager[] createTrustManagers(List<String> certificates, String password) throws Exception {
    X509Certificate[] x509Certificates = createCertificates(certificates);
    TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());

    if (x509Certificates != null && x509Certificates.length > 0) {
      KeyStore trustStoreContainingTheCertificate = KeyStore.getInstance("JKS");
      trustStoreContainingTheCertificate.load(null, password.toCharArray());

      trustStoreContainingTheCertificate.setCertificateEntry("", x509Certificates[0]);

      trustManagerFactory.init(trustStoreContainingTheCertificate);

      return trustManagerFactory.getTrustManagers();
    }

    return createTrustAllHost();
  }

  public static KeyManager[] createKeyManagers(String privateKey, List<String> certificates, String password) throws Exception {
    final KeyStore keystore = createKeyStore(privateKey, certificates, password);
    final KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
    kmf.init(keystore, password.toCharArray());
    return kmf.getKeyManagers();
  }

  private static TrustManager[] createTrustAllHost() {
    TrustManager[] trustAllCerts = new TrustManager[]{new X509TrustManager() {

      public java.security.cert.X509Certificate[] getAcceptedIssuers() {
        return new java.security.cert.X509Certificate[]{};
      }

      public void checkClientTrusted(X509Certificate[] chain, String authType) {

      }

      public void checkServerTrusted(X509Certificate[] chain, String authType) {

      }
    }};

    return trustAllCerts;
  }

  /**
   * Create a KeyStore from standard PEM file
   */
  public static KeyStore createKeyStore(String privateKey, List<String> certificates, final String password)
    throws Exception {

    final X509Certificate[] x509Certificates = createCertificates(certificates);
    final KeyStore keystore = KeyStore.getInstance("JKS");
    keystore.load(null);
    // Import private key
    final PrivateKey key = createPrivateKey(privateKey);
    keystore.setKeyEntry("", key, password.toCharArray(), x509Certificates);
    return keystore;
  }

  public static String retrivePrivateKey(File privateKeyPem) throws IOException {
    try (final BufferedReader r = new BufferedReader(new FileReader(privateKeyPem))) {
      String s = r.readLine();
      while (s != null) {
        if (s.contains("BEGIN") && s.contains("PRIVATE KEY")) {
          break;
        }
        s = r.readLine();
      }

      final StringBuffer b = new StringBuffer();
      s = "";
      while (s != null) {
        if (s.contains("END") && s.contains("PRIVATE KEY")) {
          break;
        }
        b.append(s);
        s = r.readLine();
      }

      return b.toString();
    }
  }

  public static List<String> retriveCertificates(File certificatePem) throws IOException {
    List<String> result = new ArrayList<>();

    try (final BufferedReader r = new BufferedReader(new FileReader(certificatePem))) {
      String s = r.readLine();
      while (s != null) {
        if (s.contains("BEGIN CERTIFICATE")) {
          break;
        }
        s = r.readLine();
      }
      StringBuffer b = new StringBuffer();
      while (s != null) {
        if (s.contains("END CERTIFICATE")) {
          String hexString = b.toString();
          result.add(hexString);
          b = new StringBuffer();
        } else {
          if (!s.startsWith("----")) {
            b.append(s);
          }
        }
        s = r.readLine();
      }

      return result;
    }
  }

  public static List<String> retriveCertificates(String certificatePEMString) {
    List<String> result = new ArrayList<>();
    if (EmptyKit.isNotBlank(certificatePEMString)) {
      String[] certificate = certificatePEMString.split("\\r?\\n");
      StringBuffer b = new StringBuffer();
      boolean isBegin = false;
      for (String s : certificate) {
        if (s.contains("BEGIN CERTIFICATE")) {
          b = new StringBuffer();
          isBegin = true;
          continue;
        }
        if (s.contains("END CERTIFICATE")) {
          String hexString = b.toString();
          result.add(hexString);
          isBegin = false;
        } else {
          if (isBegin) {
            b.append(s);
          }
        }
      }
    }
    return result;
  }

  public static String retrivePrivateKey(String privatePEMString) {
    final StringBuffer b = new StringBuffer();
    if (EmptyKit.isNotBlank(privatePEMString)) {
      String[] certificate = privatePEMString.split("\\r?\\n");
      boolean isBegin = false;
      for (String s : certificate) {
        if (s.contains("BEGIN") && s.contains("PRIVATE KEY")) {
          isBegin = true;
          continue;
        }

        if (s.contains("END") && s.contains("PRIVATE KEY")) {
          break;
        }

        if (isBegin) {
          b.append(s);
        }

      }
    }
    return b.toString();
  }

  private static PrivateKey createPrivateKey(String privateKey) throws Exception {

    final byte[] bytes = DatatypeConverter.parseBase64Binary(privateKey);
    return generatePrivateKeyFromDER(bytes);
  }

  private static X509Certificate[] createCertificates(List<String> certificates) throws Exception {
    if (CollectionUtils.isNotEmpty(certificates)) {
      final List<X509Certificate> result = new ArrayList<>();

      for (String certificate : certificates) {
        final byte[] bytes = DatatypeConverter.parseBase64Binary(certificate);
        X509Certificate cert = generateCertificateFromDER(bytes);
        result.add(cert);
      }

      return result.toArray(new X509Certificate[result.size()]);
    }
    return null;
  }

  private static RSAPrivateKey generatePrivateKeyFromDER(byte[] keyBytes) throws InvalidKeySpecException, NoSuchAlgorithmException {
    java.security.Security.addProvider(
      new org.bouncycastle.jce.provider.BouncyCastleProvider()
    );
    final PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(keyBytes);
    final KeyFactory factory = KeyFactory.getInstance("RSA");
    return (RSAPrivateKey) factory.generatePrivate(spec);
  }

  private static X509Certificate generateCertificateFromDER(byte[] certBytes) throws CertificateException {
    final CertificateFactory factory = CertificateFactory.getInstance("X.509");
    return (X509Certificate) factory.generateCertificate(new ByteArrayInputStream(certBytes));
  }
}
