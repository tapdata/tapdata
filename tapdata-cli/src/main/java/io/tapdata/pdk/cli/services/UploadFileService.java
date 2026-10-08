package io.tapdata.pdk.cli.services;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.tapdata.tm.sdk.auth.BasicCredentials;
import com.tapdata.tm.sdk.auth.Signer;
import com.tapdata.tm.sdk.util.Base64Util;
import com.tapdata.tm.sdk.util.IOUtil;
import com.tapdata.tm.sdk.util.SignUtil;
import io.tapdata.encryptor.JarEncryptor;
import io.tapdata.pdk.cli.utils.HttpRequest;
import io.tapdata.pdk.cli.utils.OkHttpUtils;
import io.tapdata.pdk.cli.utils.PrintUtil;
import lombok.extern.slf4j.Slf4j;
import okhttp3.*;
import okhttp3.internal.Util;
import okio.BufferedSink;
import okio.Okio;
import okio.Source;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.AlgorithmParameterSpec;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @Author: Zed
 * @Date: 2022/2/22
 * @Description:
 */
@Slf4j
public class UploadFileService {

  private static final String ADMIN_EMAIL = "admin@admin.com";
  private static final String RC4_KEY = "Gotapd8";
  private static final String RC4_ALGORITHM = "RC4";
  private static final byte[] SALTED_MAGIC = "Salted__".getBytes(java.nio.charset.StandardCharsets.US_ASCII);

  public static void upload(Map<String, InputStream> inputStreamMap, File file, List<String> jsons, boolean latest, String hostAndPort, String accessCode, String username, String password, String ak, String sk, PrintUtil printUtil) throws Exception {

    boolean cloud = StringUtils.isNotBlank(ak);


    String token = null;
    if (!cloud) {
      token = StringUtils.isNotBlank(accessCode)
              ? generateAccessCodeToken(hostAndPort, accessCode, printUtil)
              : login(hostAndPort, username, password);
      if (StringUtils.isBlank(token)) {
        throw new IllegalStateException("TM server not found or authentication failed");
      }
    }

    Map<String, String> params = new HashMap<>();
    params.put("ts", String.valueOf(System.currentTimeMillis()));
    params.put("nonce", UUID.randomUUID().toString());
    params.put("signVersion", "1.0");
    params.put("accessKey", ak);


    MessageDigest digest = null;
    try {
      digest = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }


    MultipartBody.Builder builder = new MultipartBody.Builder();
    builder.setType(MultipartBody.FORM);
    if (file != null) {
      if (cloud) {
        digest.update("file".getBytes(UTF_8));
        digest.update(file.getName().getBytes(UTF_8));
        try {
          digest.update(IOUtil.readFile(file));
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }

    if (inputStreamMap != null) {
      for (Map.Entry<String, InputStream> entry : inputStreamMap.entrySet()) {
        String k = entry.getKey();
        InputStream v = entry.getValue();
        if (cloud) {
          byte[] in_b = new byte[0];
          try {
            in_b = IOUtil.readInputStream(v);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          v = new ByteArrayInputStream(in_b);
          digest.update("file".getBytes(UTF_8));
          digest.update(k.getBytes(UTF_8));
          digest.update(in_b);
          inputStreamMap.put(k, v);
        }
      }
    }

    //要上传的文字参数
    if (jsons != null && !jsons.isEmpty()) {
      for (String json : jsons) {
        if (cloud) {
          digest.update("source".getBytes(UTF_8));
          digest.update(json.getBytes(UTF_8));
        }
      }
      // if the jsons size == 1, the data received by TM will be weird, adding an empty string helps TM receive the
      // proper data; the empty string should be dealt in TM.
      if (jsons.size() == 1) {
        if (cloud) {
          digest.update("source".getBytes(UTF_8));
          digest.update("".getBytes());
        }
      }
    }    // whether replace the latest version
    String latestString = String.valueOf(latest);
    if (cloud) {
      digest.update("latest".getBytes(UTF_8));
      digest.update(latestString.getBytes(UTF_8));
    }


    String url;
    final String method = "POST";
    HttpRequest request;
    if (cloud) {

      String bodyHash = Base64Util.encode(digest.digest());

      printUtil.print(PrintUtil.TYPE.DEBUG, String.format("bodyHash: %s", bodyHash));
      BasicCredentials basicCredentials = new BasicCredentials(ak, sk);
      Signer signer = Signer.getSigner(basicCredentials);


      String canonicalQueryString = SignUtil.canonicalQueryString(params);
      String stringToSign = String.format("%s:%s:%s", method, canonicalQueryString, bodyHash);
      printUtil.print(PrintUtil.TYPE.DEBUG, String.format("stringToSign: %s", stringToSign));
      String sign = signer.signString(stringToSign, basicCredentials);

      params.put("sign", sign);
      printUtil.print(PrintUtil.TYPE.DEBUG, "sign: " + sign);

      String queryString = params.keySet().stream().map(key -> {
        try {
          return String.format("%s=%s", SignUtil.percentEncode(key), SignUtil.percentEncode(params.get(key)));
        } catch (UnsupportedEncodingException e) {
          e.printStackTrace();
        }
        return key + "=" + params.get(key);
      }).collect(Collectors.joining("&"));
      url = hostAndPort + "/api/pdk/upload/source?";
      request = new HttpRequest(url + queryString, method);
    } else {
      url = hostAndPort + "/api/pdk/upload/source?access_token=" + token;
      request = new HttpRequest(url, method);
    }
    request.connectTimeout(180000).readTimeout(180000);//连接超时设置
    //request.progress((uploaded, total) -> System.out.println("uploaded: " + uploaded + " total: " + total + " time: " + System.currentTimeMillis()));
    if (file != null) {
      request.part("file", file.getName(), "application/java-archive", file);
    }

    if (inputStreamMap != null) {
      for (Map.Entry<String, InputStream> entry : inputStreamMap.entrySet()) {
        String k = entry.getKey();
        request.part("file", k, "image/*", entry.getValue());
      }
    }

    //要上传的文字参数
    if (jsons != null && !jsons.isEmpty()) {
      for (String json : jsons) {
        request.part("source", json);
      }
      // if the jsons size == 1, the data received by TM will be weird, adding an empty string helps TM receive the
      // proper data; the empty string should be dealt in TM.
      if (jsons.size() == 1) {
        request.part("source", "");
      }
    }    // whether replace the latest version
    request.part("latest", latestString);

    String response = request.body();

    Map map = JSON.parseObject(response, Map.class);

    String msg = "success";
    String result = "success";
    boolean uploadSucceeded = "ok".equals(map.get("code"));
    if (!uploadSucceeded) {
        msg = map.get("reqId") != null ? (String) map.get("message") : (String) map.get("msg");
        result = "fail";
      printUtil.print(PrintUtil.TYPE.ERROR, String.format("* Register Connector: %s Failed, message: %s", file.getName(), msg));
    } else {
      printUtil.print(PrintUtil.TYPE.INFO, String.format("* Register Connector: %s Completed", file.getName()));
    }
    printUtil.print(PrintUtil.TYPE.WARN, "result:" + result + ", name:" + file.getName() + ", msg:" + msg + ", response:" + response);
    if (!uploadSucceeded) {
      throw new IllegalStateException("Connector registration failed: " + msg);
    }
    JarEncryptor.encryptJar(file.getPath());
  }

  /**
   * Compatibility overload for callers using the administrator-password flow.
   */
  public static void upload(Map<String, InputStream> inputStreamMap, File file, List<String> jsons, boolean latest, String hostAndPort, String username, String password, String ak, String sk, PrintUtil printUtil) throws Exception {
    upload(inputStreamMap, file, jsons, latest, hostAndPort, null, username, password, ak, sk, printUtil);
  }

  /**
   * Compatibility overload for the original accessCode-based registration API.
   */
  public static void upload(Map<String, InputStream> inputStreamMap, File file, List<String> jsons, boolean latest, String hostAndPort, String accessCode, String ak, String sk, PrintUtil printUtil) throws Exception {
    upload(inputStreamMap, file, jsons, latest, hostAndPort, accessCode, null, null, ak, sk, printUtil);
  }

  static String generateAccessCodeToken(String hostAndPort, String accessCode, PrintUtil printUtil) {
    String tokenUrl = hostAndPort + "/api/users/generatetoken";
    Map<String, String> param = new HashMap<>();
    param.put("accesscode", accessCode);
    String response = OkHttpUtils.postJsonParams(tokenUrl, JSON.toJSONString(param));

    printUtil.print(PrintUtil.TYPE.DEBUG, "generate token " + response);

    if (StringUtils.isBlank(response)) {
      printUtil.print(PrintUtil.TYPE.ERROR, "TM sever not found or generate token failed");
      throw new IllegalStateException("TM server not found or generate token failed");
    }

    JSONObject result = JSON.parseObject(response);
    JSONObject data = result.getJSONObject("data");
    String token = data == null ? null : data.getString("id");
    if (StringUtils.isBlank(token)) {
      printUtil.print(PrintUtil.TYPE.ERROR, "TM sever not found or generate token failed");
      throw new IllegalStateException("TM server not found or generate token failed");
    }
    return token;
  }

  static String login(String hostAndPort, String username, String password) {
    if (!ADMIN_EMAIL.equals(StringUtils.trim(username))) {
      throw new IllegalArgumentException("Connector registration only supports admin@admin.com");
    }
    if (StringUtils.isBlank(password)) {
      throw new IllegalArgumentException("Administrator password is required");
    }
    String loginUrl = hostAndPort + "/api/users/login";
    String response = OkHttpUtils.postJsonParams(loginUrl, createLoginRequest(username, password));
    return parseAccessToken(response);
  }

  static String createLoginRequest(String username, String password) {
    Map<String, String> loginRequest = new HashMap<>();
    loginRequest.put("email", StringUtils.trim(username));
    loginRequest.put("password", encryptPassword(password));
    return JSON.toJSONString(loginRequest);
  }

  static String parseAccessToken(String response) {
    if (StringUtils.isBlank(response)) {
      throw new IllegalStateException("TM server not found or login failed");
    }
    JSONObject result = JSON.parseObject(response);
    JSONObject data = result.getJSONObject("data");
    String token = data == null ? null : data.getString("id");
    if (!"ok".equals(result.getString("code")) || StringUtils.isBlank(token)) {
      String message = StringUtils.defaultIfBlank(result.getString("message"), result.getString("msg"));
      throw new IllegalStateException(StringUtils.defaultIfBlank(message, "TM server not found or login failed"));
    }
    return token;
  }

  static String encryptPassword(String password) {
    try {
      byte[] pass = RC4_KEY.getBytes(java.nio.charset.StandardCharsets.US_ASCII);
      byte[] salt = new SecureRandom().generateSeed(8);
      byte[] passAndSalt = concat(pass, salt);
      byte[] hash = new byte[0];
      byte[] keyAndIv = new byte[0];
      for (int index = 0; index < 3 && keyAndIv.length < 48; index++) {
        MessageDigest md = MessageDigest.getInstance("MD5");
        hash = md.digest(concat(hash, passAndSalt));
        keyAndIv = concat(keyAndIv, hash);
      }
      SecretKeySpec key = new SecretKeySpec(Arrays.copyOfRange(keyAndIv, 0, 32), RC4_ALGORITHM);
      Cipher cipher = Cipher.getInstance(RC4_ALGORITHM);
      cipher.init(Cipher.ENCRYPT_MODE, key, (AlgorithmParameterSpec) null);
      byte[] encrypted = cipher.doFinal(password.getBytes(UTF_8));
      return Base64.getEncoder().encodeToString(concat(concat(SALTED_MAGIC, salt), encrypted));
    } catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException
             | InvalidAlgorithmParameterException | IllegalBlockSizeException | BadPaddingException e) {
      throw new IllegalStateException("Failed to encrypt administrator password", e);
    }
  }

  private static byte[] concat(byte[] first, byte[] second) {
    byte[] result = new byte[first.length + second.length];
    System.arraycopy(first, 0, result, 0, first.length);
    System.arraycopy(second, 0, result, first.length, second.length);
    return result;
  }

  public static RequestBody create(final MediaType mediaType, final InputStream inputStream) {
    return new RequestBody() {
      @Override
      public MediaType contentType() {
        return mediaType;
      }

      @Override
      public long contentLength() {
        try {
          return inputStream.available();
        } catch (IOException e) {
          return 0;
        }
      }

      @Override
      public void writeTo(BufferedSink sink) throws IOException {
        Source source = null;
        try {
          source = Okio.source(inputStream);
          sink.writeAll(source);
        } finally {
          Util.closeQuietly(source);
        }
      }
    };
  }

}
