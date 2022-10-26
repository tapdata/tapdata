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
        int i = k.lastIndexOf("/");
        String substring = k.substring(i + 1);
        if (cloud) {
          byte[] in_b = new byte[0];
          try {
            in_b = IOUtil.readInputStream(v);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          v = new ByteArrayInputStream(in_b);
          digest.update("file".getBytes(UTF_8));
          digest.update(substring.getBytes(UTF_8));
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
        int i = k.lastIndexOf("/");
        k = k.substring(i + 1);
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
    System.out.println(response);


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

//  public void testSignForUploadFileRequest() throws NoSuchAlgorithmException, IOException {
//
//    final String accessKey = "eEfOPokhZxhJ6VW787mbTLUfwHz31ZdN";
//    final String secretKey = "6KbMpGqUyD3EDoeOjDRFZJ4TBg6QEsqA";
//
//    Map<String, String> params = new HashMap<>();
//    params.put("ts", String.valueOf(System.currentTimeMillis()));
//    params.put("nonce", UUID.randomUUID().toString());
//    params.put("signVersion", "1.0");
//    params.put("accessKey", accessKey);
//
//    String source = "{\"tapTypeDataTypeMap\":\"{\\\"io.tapdata.entity.schema.type.TapTime\\\":\\\"datetime\\\"}\",\"expression\":\"{\\\"date\\\":{\\\"byte\\\":3,\\\"range\\\":[\\\"1000-01-01\\\",\\\"9999-12-31\\\"],\\\"to\\\":\\\"TapDate\\\"},\\\"string\\\":{\\\"queryOnly\\\":true,\\\"to\\\":\\\"TapString\\\"},\\\"byte\\\":{\\\"bit\\\":8,\\\"priority\\\":3,\\\"value\\\":[-128,127],\\\"to\\\":\\\"TapNumber\\\"},\\\"double\\\":{\\\"precision\\\":[1,17],\\\"preferPrecision\\\":11,\\\"scale\\\":[0,17],\\\"preferScale\\\":4,\\\"fixed\\\":false,\\\"to\\\":\\\"TapNumber\\\"},\\\"integer\\\":{\\\"bit\\\":32,\\\"priority\\\":1,\\\"value\\\":[-2147483648,2147483647],\\\"to\\\":\\\"TapNumber\\\"},\\\"float\\\":{\\\"precision\\\":[1,6],\\\"scale\\\":[0,6],\\\"fixed\\\":false,\\\"to\\\":\\\"TapNumber\\\"},\\\"long\\\":{\\\"bit\\\":64,\\\"priority\\\":3,\\\"value\\\":[-9223372036854775808,9223372036854775807],\\\"to\\\":\\\"TapNumber\\\"},\\\"datetime\\\":{\\\"byte\\\":8,\\\"range\\\":[\\\"1000-01-01 00:00:00 000000000\\\",\\\"9999-12-31 23:59:59 999999999\\\"],\\\"to\\\":\\\"TapDateTime\\\"},\\\"boolean\\\":{\\\"to\\\":\\\"TapBoolean\\\"},\\\"scaled_float\\\":{\\\"queryOnly\\\":true,\\\"fixed\\\":false,\\\"to\\\":\\\"TapNumber\\\"},\\\"binary\\\":{\\\"to\\\":\\\"TapBinary\\\"},\\\"short\\\":{\\\"bit\\\":16,\\\"priority\\\":3,\\\"value\\\":[-32768,32767],\\\"to\\\":\\\"TapNumber\\\"},\\\"text\\\":{\\\"byte\\\":\\\"4g\\\",\\\"to\\\":\\\"TapString\\\"},\\\"half_float\\\":{\\\"queryOnly\\\":true,\\\"fixed\\\":false,\\\"to\\\":\\\"TapNumber\\\"},\\\"keyword\\\":{\\\"byte\\\":32766,\\\"to\\\":\\\"TapString\\\"},\\\"object\\\":{\\\"to\\\":\\\"TapString\\\"}}\",\"capabilities\":[{\"id\":\"write_record_function\",\"type\":11},{\"id\":\"create_table_function\",\"type\":11},{\"id\":\"clear_table_function\",\"type\":11},{\"id\":\"drop_table_function\",\"type\":11}],\"dataTypesMap\":{\"empty\":false},\"icon\":\"icons/redis.svg\",\"name\":\"Redis\",\"messages\":{\"default\":\"en_US\",\"zh_TW\":{\"sentinel\":\"哨兵部署\",\"deploymentMode\":\"部署模式\",\"passwordDisplay\":\"是否使用密碼\",\"Address\":\"服務器地址\",\"standalone\":\"單機部署\",\"cachePrefix\":\"緩存key\",\"database\":\"數據庫名稱\",\"password\":\"密碼\",\"cacheKey\":\"緩存鍵\",\"port\":\"端口\",\"sentinelPort\":\"服務器端口\",\"valueType\":\"存儲格式\",\"sentinelAddress\":\"服務器地址\",\"host\":\"數據庫地址\",\"doc\":\"docs/redis_zh_TW.md\",\"prompt\":\"添加\",\"sentinelName\":\"哨兵名稱\"},\"en_US\":{\"sentinel\":\"Sentinel deployment\",\"deploymentMode\":\"Deployment mode\",\"passwordDisplay\":\"Whether to use a password\",\"Address\":\"Please enter the server address\",\"standalone\":\"Single machine deployment\",\"cachePrefix\":\"cache key\",\"database\":\"Database Name\",\"password\":\"Password\",\"cacheKey\":\"cache key\",\"port\":\"Port\",\"sentinelPort\":\"server port\",\"valueType\":\"storage format\",\"sentinelAddress\":\"Server address\",\"host\":\"DB Host\",\"doc\":\"docs/redis_en_US.md\",\"prompt\":\"add\",\"sentinelName\":\"Sentinel name\"},\"zh_CN\":{\"sentinel\":\"哨兵部署\",\"deploymentMode\":\"部署模式\",\"passwordDisplay\":\"是否使用密码\",\"Address\":\"服务器地址\",\"standalone\":\"单机部署\",\"cachePrefix\":\"缓存key\",\"database\":\"数据库名称\",\"password\":\"密码\",\"cacheKey\":\"缓存键\",\"port\":\"端口\",\"sentinelPort\":\"端口\",\"valueType\":\"存储格式\",\"sentinelAddress\":\"服务器地址\",\"host\":\"数据库地址\",\"doc\":\"docs/redis_zh_CN.md\",\"prompt\":\"添加\",\"sentinelName\":\"哨兵名称\"}},\"id\":\"redis\",\"type\":\"target\",\"version\":\"1.0-SNAPSHOT\",\"configOptions\":{\"connection\":{\"type\":\"object\",\"properties\":{\"deploymentMode\":{\"type\":\"string\",\"title\":\"${deploymentMode}\",\"default\":\"standalone\",\"x-decorator\":\"FormItem\",\"x-component\":\"Select\",\"x-index\":1,\"enum\":[{\"label\":\"${standalone}\",\"value\":\"standalone\"},{\"label\":\"${sentinel}\",\"value\":\"sentinel\"}],\"x-reactions\":[{\"target\":\"*(host,port)\",\"fulfill\":{\"state\":{\"visible\":\"{{$self.value === 'standalone'}}\"}}},{\"target\":\"*(sentinelName,sentinelAddress)\",\"fulfill\":{\"state\":{\"visible\":\"{{$self.value === 'sentinel'}}\"}}}]},\"host\":{\"required\":true,\"type\":\"string\",\"title\":\"${host}\",\"x-decorator\":\"FormItem\",\"x-component\":\"Input\",\"apiServerKey\":\"database_host\",\"x-index\":2},\"port\":{\"type\":\"string\",\"title\":\"${port}\",\"x-decorator\":\"FormItem\",\"x-component\":\"InputNumber\",\"apiServerKey\":\"database_port\",\"x-index\":3,\"required\":true},\"database\":{\"type\":\"string\",\"title\":\"${database}\",\"x-decorator\":\"FormItem\",\"x-component\":\"Input\",\"apiServerKey\":\"database_name\",\"x-index\":4,\"required\":true},\"passwordDisplay\":{\"type\":\"boolean\",\"title\":\"${passwordDisplay}\",\"x-decorator\":\"FormItem\",\"x-component\":\"Switch\",\"x-index\":6,\"x-reactions\":{\"target\":\"password\",\"fulfill\":{\"state\":{\"visible\":\"{{!!$self.value}}\"}}}},\"password\":{\"type\":\"string\",\"title\":\"${password}\",\"x-decorator\":\"FormItem\",\"x-component\":\"Password\",\"apiServerKey\":\"database_password\",\"x-index\":7},\"sentinelName\":{\"type\":\"string\",\"title\":\"${sentinelName}\",\"x-decorator\":\"FormItem\",\"x-component\":\"Input\",\"x-index\":8,\"required\":true},\"sentinelAddress\":{\"type\":\"array\",\"title\":\"${sentinelAddress}\",\"x-decorator\":\"FormItem\",\"x-component\":\"ArrayItems\",\"x-index\":9,\"items\":{\"type\":\"object\",\"properties\":{\"space\":{\"type\":\"void\",\"x-component\":\"Space\",\"properties\":{\"host\":{\"type\":\"string\",\"x-decorator\":\"FormItem\",\"x-component\":\"Input\",\"x-component-props\":{\"placeholder\":\"${Address}\"},\"x-index\":1},\"port\":{\"type\":\"number\",\"x-decorator\":\"FormItem\",\"x-component\":\"InputNumber\",\"x-component-props\":{\"placeholder\":\"${sentinelPort}\"},\"x-index\":2},\"remove\":{\"type\":\"void\",\"x-decorator\":\"FormItem\",\"x-component\":\"ArrayItems.Remove\"}}}}},\"properties\":{\"add\":{\"type\":\"void\",\"title\":\"${prompt}\",\"x-component\":\"ArrayItems.Addition\"}}}}},\"node\":{\"properties\":{\"valueType\":{\"type\":\"string\",\"title\":\"${valueType}\",\"default\":\"list\",\"x-decorator\":\"FormItem\",\"x-component\":\"Select\",\"x-index\":3,\"enum\":[{\"label\":\"list with header\",\"value\":\"list\"},{\"label\":\"json\",\"value\":\"json\"}],\"x-reactions\":[{\"target\":\".cacheKeys\",\"fulfill\":{\"state\":{\"visible\":\"{{$self.value === 'json'}}\"}}}],\"index\":1},\"cachePrefix\":{\"type\":\"string\",\"title\":\"${cachePrefix}\",\"x-decorator\":\"FormItem\",\"x-component\":\"Input\",\"index\":2,\"required\":true},\"cacheKeys\":{\"type\":\"string\",\"title\":\"${cacheKey}\",\"x-decorator\":\"FormItem\",\"x-component\":\"FieldSelect\",\"x-component-props\":{\"allowCreate\":true,\"multiple\":true,\"filterable\":true},\"x-reactions\":[\"{{useAsyncDataSourceByConfig({service: loadNodeFieldOptions, withoutField: true}, $values.$inputs[0])}}\",{\"dependencies\":[\".valueType\"],\"fulfill\":{\"state\":{\"visible\":\"{{$deps[0] === 'json'}}\"}}}],\"index\":3,\"required\":true}}}},\"group\":\"io.tapdata\"}";
//    String source1 = "";
//    File enUS = new File("/Users/lg/workspace/tapdata/connectors/redis-connector/target/classes/docs/redis_en_US.md");
//    File zhCN = new File("/Users/lg/workspace/tapdata/connectors/redis-connector/target/classes/docs/redis_zh_CN.md");
//    File zhTW = new File("/Users/lg/workspace/tapdata/connectors/redis-connector/target/classes/docs/redis_zh_TW.md");
//    File icon = new File("/Users/lg/workspace/tapdata/connectors/redis-connector/target/classes/icons/redis.svg");
//    File jar = new File("/Users/lg/workspace/tapdata/connectors/redis-connector/target/redis-connector-v1.0-SNAPSHOT.jar");
//
//    MessageDigest digest = MessageDigest.getInstance("SHA-256");
//    digest.update("source".getBytes(UTF_8));
//    digest.update(source.getBytes(UTF_8));
//
//    digest.update("source".getBytes(UTF_8));
//    digest.update(source1.getBytes());
//
//    digest.update("file".getBytes(UTF_8));
//    digest.update("redis_en_US.md".getBytes(UTF_8));
//    digest.update(IOUtil.readFile(enUS));
//
//    digest.update("file".getBytes(UTF_8));
//    digest.update("redis_zh_CN.md".getBytes(UTF_8));
//    digest.update(IOUtil.readFile(zhCN));
//
//    digest.update("file".getBytes(UTF_8));
//    digest.update("redis_zh_TW.md".getBytes(UTF_8));
//    digest.update(IOUtil.readFile(zhTW));
//
//    digest.update("file".getBytes(UTF_8));
//    digest.update("redis.svg".getBytes(UTF_8));
//    digest.update(IOUtil.readFile(icon));
//
//    digest.update("file".getBytes(UTF_8));
//    digest.update("redis-connector-v1.0-SNAPSHOT.jar".getBytes(UTF_8));
//    digest.update(IOUtil.readFile(jar));
//
//    digest.update("latest".getBytes(UTF_8));
//    digest.update("true".getBytes(UTF_8));
//
//    String bodyHash = Base64Util.encode(digest.digest());
//
//    BasicCredentials basicCredentials = new BasicCredentials(accessKey, secretKey);
//    Signer signer = Signer.getSigner(basicCredentials);
//
//    final String method = "POST";
//    String canonicalQueryString = SignUtil.canonicalQueryString(params);
//    String stringToSign = String.format("%s:%s:%s", method, canonicalQueryString, bodyHash);
//    String sign = signer.signString(stringToSign, basicCredentials);
//
//    System.out.println("String to sign: " + stringToSign);
//    System.out.println("Body hash: " + bodyHash);
//    System.out.println("Sign: " + sign);
//
//    params.put("sign", sign);
//
//    String queryString = params.keySet().stream().map(key -> {
//      try {
//        return String.format("%s=%s", SignUtil.percentEncode(key), SignUtil.percentEncode(params.get(key)));
//      } catch (UnsupportedEncodingException e) {
//        e.printStackTrace();
//      }
//      return key + "=" + params.get(key);
//    }).collect(Collectors.joining("&"));
//
//    HttpRequest request = new HttpRequest("http://127.0.0.1:30100/tm/api/pdk/upload/source?" + queryString, method);
//    request.part("source", source);
//    request.part("source", source1);
//    request.part("file", "redis_en_US.md", "text/markdown", enUS);
//    request.part("file", "redis_zh_CN.md", "text/markdown", zhCN);
//    request.part("file", "redis_zh_TW.md", "text/markdown", zhTW);
//    request.part("file", "redis.svg", "image/svg", icon);
//
//    request.part("file", "redis-connector-v1.0-SNAPSHOT.jar", "application/java-archive", jar);
//    request.part("latest", "true");
//
//
//    String response = request.body();
//    System.out.println(response);
//
//    Map<String, Object> result = parseJson(response, new TypeToken<Map<String, Object>>() {
//    });
//    Assert.assertEquals(result.get("code"), "ok");
//  }

}
