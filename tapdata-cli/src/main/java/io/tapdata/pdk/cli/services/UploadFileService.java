package io.tapdata.pdk.cli.services;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.tapdata.tm.sdk.auth.BasicCredentials;
import com.tapdata.tm.sdk.auth.Signer;
import com.tapdata.tm.sdk.util.Base64Util;
import com.tapdata.tm.sdk.util.IOUtil;
import com.tapdata.tm.sdk.util.SignUtil;
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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @Author: Zed
 * @Date: 2022/2/22
 * @Description:
 */
@Slf4j
public class UploadFileService {
  public static final String POST = "POST";
  public static final String LATEST = "latest";
  public static final String FILE = "file";
  public static final String SOURCE = "source";

  public static final String OP_URL = "%s/api/pdk/upload/source?access_token=%s";

  public static class Param {
    Map<String, InputStream> inputStreamMap;
    File file;
    List<String> jsons;
    boolean latest;
    String hostAndPort;
    String token;
    String ak;
    String sk;
    PrintUtil printUtil;

    public Map<String, InputStream> getInputStreamMap() {
      return inputStreamMap;
    }

    public void setInputStreamMap(Map<String, InputStream> inputStreamMap) {
      this.inputStreamMap = inputStreamMap;
    }

    public File getFile() {
      return file;
    }

    public void setFile(File file) {
      this.file = file;
    }

    public List<String> getJsons() {
      return jsons;
    }

    public void setJsons(List<String> jsons) {
      this.jsons = jsons;
    }

    public boolean isLatest() {
      return latest;
    }

    public void setLatest(boolean latest) {
      this.latest = latest;
    }

    public String getHostAndPort() {
      return hostAndPort;
    }

    public void setHostAndPort(String hostAndPort) {
      this.hostAndPort = hostAndPort;
    }

    public String getToken() {
      return token;
    }

    public void setToken(String token) {
      this.token = token;
    }

    public String getAk() {
      return ak;
    }

    public void setAk(String ak) {
      this.ak = ak;
    }

    public String getSk() {
      return sk;
    }

    public void setSk(String sk) {
      this.sk = sk;
    }

    public PrintUtil getPrintUtil() {
      return printUtil;
    }

    public void setPrintUtil(PrintUtil printUtil) {
      this.printUtil = printUtil;
    }
  }

  public static boolean isCloud(String ak) {
    return StringUtils.isNotBlank(ak);
  }

  public static String findOpToken(String hostAndPort, String accessCode, PrintUtil printUtil) {
    String tokenUrl = String.format("%s/api/users/generatetoken", hostAndPort);
    Map<String, String> param = new HashMap<>();
    param.put("accesscode", accessCode);
    String jsonString = JSON.toJSONString(param);
    String s = OkHttpUtils.postJsonParams(tokenUrl, jsonString);

    printUtil.print(PrintUtil.TYPE.DEBUG, String.format("generate token %s", s));
    String error = "* TM sever not found or generate token failed";
    if (StringUtils.isBlank(s)) {
      printUtil.print(PrintUtil.TYPE.ERROR, error);
      return null;
    }

    Map<?, ?> map = JSON.parseObject(s, Map.class);
    Object data = map.get("data");
    if (null == data) {
      printUtil.print(PrintUtil.TYPE.ERROR, error);
      return null;
    }
    JSONObject data1 = (JSONObject) data;
    String token = (String) data1.get("id");
    if (StringUtils.isBlank(token)) {
      printUtil.print(PrintUtil.TYPE.ERROR, error);
      return token;
    }
    return token;
  }

  /**
   * @deprecated please use method to upload connector,
   * */
  public static void uploadSourceToTM(Param paramEntity) {
    Map<String, InputStream> inputStreamMap = paramEntity.getInputStreamMap();
    File file = paramEntity.getFile();
    List<String> jsons = paramEntity.jsons;
    boolean latest = paramEntity.latest;
    String hostAndPort = paramEntity.hostAndPort;
    String token = paramEntity.token;
    String ak = paramEntity.ak;
    String sk = paramEntity.sk;
    PrintUtil printUtil = paramEntity.printUtil;
    boolean cloud = isCloud(ak);

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
          digest.update(json.getBytes());
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

    Map<?, ?> map = JSON.parseObject(response, Map.class);

    String msg = "success";
    String result = "success";
    if (!"ok".equals(map.get("code"))) {
      msg = map.get("reqId") != null ? (String) map.get("message") : (String) map.get("msg");
      result = "fail";
    }
    printUtil.print(PrintUtil.TYPE.DEBUG, "* Register result: " + result + ", name:" + (null == file ? "-" : file.getName()) + ", msg:" + msg + ", response:" + response);
  }

  public static void uploadConnector(Param paramEntity) {
    if (null == paramEntity.file) {
      paramEntity.printUtil.print(PrintUtil.TYPE.DEBUG, "Ignore an empty connector file to register");
      return;
    }
    if (isCloud(paramEntity.ak)) {
      uploadSourceToTM(paramEntity);
    } else {
      uploadSourceToOP(paramEntity);
    }
  }

  protected static Map<String, String> initHttpParam(Param paramEntity) {
    Map<String, String> params = new HashMap<>();
    params.put("ts", String.valueOf(System.currentTimeMillis()));
    params.put("nonce", UUID.randomUUID().toString());
    params.put("signVersion", "1.0");
    params.put("accessKey", paramEntity.ak);
    return params;
  }

  protected static void uploadSourceToOP(Param paramEntity) {
    final String url = String.format(OP_URL, paramEntity.getHostAndPort(), paramEntity.getToken());
    HttpRequest request = new HttpRequest(url, POST);
    sendSource(request, paramEntity, String.valueOf(paramEntity.isLatest()));
  }

  protected static MessageDigest initMessageDigest(Param paramEntity) {
    MessageDigest digest = null;
    try {
      digest = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
    MultipartBody.Builder builder = new MultipartBody.Builder();
    builder.setType(MultipartBody.FORM);
    digest.update(FILE.getBytes(UTF_8));
    File file = paramEntity.getFile();
    digest.update(file.getName().getBytes(UTF_8));
    try {
      digest.update(IOUtil.readFile(file));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return digest;
  }

  protected static void uploadSourceToCloud(Param paramEntity) {
    PrintUtil printUtil = paramEntity.printUtil;
    Map<String, String> params = initHttpParam(paramEntity);
    MessageDigest digest = initMessageDigest(paramEntity);

    Optional.ofNullable(paramEntity.getInputStreamMap()).ifPresent(inputStreamMap -> {
      for (Map.Entry<String, InputStream> entry : inputStreamMap.entrySet()) {
        String k = entry.getKey();
        InputStream v = entry.getValue();
        byte[] inBytes;
        try {
          inBytes = IOUtil.readInputStream(v);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        v = new ByteArrayInputStream(inBytes);
        digest.update(FILE.getBytes(UTF_8));
        digest.update(k.getBytes(UTF_8));
        digest.update(inBytes);
        inputStreamMap.put(k, v);
      }
    });

    //要上传的文字参数
    Optional.ofNullable(paramEntity.jsons).ifPresent(jsons -> {
      for (String json : jsons) {
        digest.update(SOURCE.getBytes(UTF_8));
        digest.update(json.getBytes());
      }
      // if the jsons size == 1, the data received by TM will be weird, adding an empty string helps TM receive the
      // proper data; the empty string should be dealt in TM.
      if (jsons.size() == 1) {
        digest.update(SOURCE.getBytes(UTF_8));
        digest.update("".getBytes());
      }
    });

    // whether replace the latest version
    String latestString = String.valueOf(paramEntity.isLatest());
    digest.update(LATEST.getBytes(UTF_8));
    digest.update(latestString.getBytes(UTF_8));

    String bodyHash = Base64Util.encode(digest.digest());

    printUtil.print(PrintUtil.TYPE.DEBUG, String.format("Hash body: %s", bodyHash));
    BasicCredentials basicCredentials = new BasicCredentials(paramEntity.getAk(), paramEntity.getSk());
    Signer signer = Signer.getSigner(basicCredentials);
    String canonicalQueryString = SignUtil.canonicalQueryString(params);
    String stringToSign = String.format("%s:%s:%s", POST, canonicalQueryString, bodyHash);

    printUtil.print(PrintUtil.TYPE.DEBUG, String.format("Sign string is: %s", stringToSign));
    String sign = signer.signString(stringToSign, basicCredentials);

    params.put("sign", sign);
    printUtil.print(PrintUtil.TYPE.DEBUG, String.format("Sign result is: %s", sign));

    final String formatSingRegex = "%s=%s";
    String queryString = params.keySet().stream().map(key -> {
      try {
        return String.format(formatSingRegex, SignUtil.percentEncode(key), SignUtil.percentEncode(params.get(key)));
      } catch (UnsupportedEncodingException e) {
        printUtil.print(PrintUtil.TYPE.WARN, String.format("Percent encode sing info failed, message: %s", e.getMessage()));
      }
      return String.format(formatSingRegex, key, params.get(key));
    }).collect(Collectors.joining("&"));

    final String url = String.format("%s/api/pdk/upload/source?%s", paramEntity.getHostAndPort(), queryString);
    HttpRequest request = new HttpRequest(url, POST);
    sendSource(request, paramEntity, latestString);
  }

  protected static void sendSource(HttpRequest request, Param paramEntity, String latestString) {
    String fileName = paramEntity.getFile().getName();
    request.part(FILE, fileName, "application/java-archive", paramEntity.getFile());
    Optional.ofNullable(paramEntity.getInputStreamMap()).ifPresent(inputStreamMap -> {
      for (Map.Entry<String, InputStream> entry : inputStreamMap.entrySet()) {
        String k = entry.getKey();
        request.part(FILE, k, "image/*", entry.getValue());
      }
    });
    //要上传的文字参数
    Optional.ofNullable(paramEntity.getJsons()).ifPresent(jsons -> {
      for (String json : jsons) {
        request.part(SOURCE, json);
      }
      // if the jsons size == 1, the data received by TM will be weird, adding an empty string helps TM receive the
      // proper data; the empty string should be dealt in TM.
      if (jsons.size() == 1) {
        request.part(SOURCE, "");
      }
    });
    // whether replace the latest version
    request.part(LATEST, latestString);
    String response = request.body();
    Map<?, ?> map = JSON.parseObject(response, Map.class);
    String msg = String.format("Connector %s register succeed", fileName);
    PrintUtil printUtil = paramEntity.getPrintUtil();
    if (!"ok".equals(map.get("code"))) {
      msg = map.get("reqId") != null ? (String) map.get("message") : (String) map.get("msg");
      printUtil.print(PrintUtil.TYPE.DEBUG, String.format("* Register %s, connector file name: %s, msg: %s, response: %s", PrintUtil.string(PrintUtil.TYPE.ERROR, "failed"), fileName, msg, response));
      return;
    }
    printUtil.print(PrintUtil.TYPE.DEBUG, String.format("* Register %s, connector file name: %s, msg: %s, response: %s", PrintUtil.string(PrintUtil.TYPE.INFO, "succeed"), fileName, msg, response));
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
