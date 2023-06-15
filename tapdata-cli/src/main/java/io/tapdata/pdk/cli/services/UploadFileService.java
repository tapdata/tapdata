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

  public static void upload(Map<String, InputStream> inputStreamMap, File file, List<String> jsons, boolean latest, String hostAndPort, String accessCode, String ak, String sk) {

    boolean cloud = StringUtils.isNotBlank(ak);


    String token = null;
    if (!cloud) {

      String tokenUrl = hostAndPort + "/api/users/generatetoken";
      Map<String, String> param = new HashMap<>();
      param.put("accesscode", accessCode);
      String jsonString = JSON.toJSONString(param);
      String s = OkHttpUtils.postJsonParams(tokenUrl, jsonString);

      System.out.println("generate token " + s);

      if (StringUtils.isBlank(s)) {
        log.error("tm sever not found or generate token failed");
        return;
      }

      Map map = JSON.parseObject(s, Map.class);
      Object data = map.get("data");
      JSONObject data1 = (JSONObject) data;
      token = (String) data1.get("id");
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

      System.out.println("bodyHash: " + bodyHash);
      BasicCredentials basicCredentials = new BasicCredentials(ak, sk);
      Signer signer = Signer.getSigner(basicCredentials);


      String canonicalQueryString = SignUtil.canonicalQueryString(params);
      String stringToSign = String.format("%s:%s:%s", method, canonicalQueryString, bodyHash);
      System.out.println("stringToSign: " + stringToSign);
      String sign = signer.signString(stringToSign, basicCredentials);

      params.put("sign", sign);
      System.out.println("sign: " + sign);

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

    Map map = JSON.parseObject(response, Map.class);

    String msg = "success";
    String result = "success";
    if (!"ok".equals(map.get("code"))) {
        msg = map.get("reqId") != null ? (String) map.get("message") : (String) map.get("msg");
        result = "fail";
    }
    System.out.println("result:" + result + ", name:" + file.getName() + ", msg:" + msg + ", response:" + response);
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
