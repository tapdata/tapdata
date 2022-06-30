package io.tapdata.pdk.cli.services;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.tapdata.pdk.cli.utils.OkHttpUtils;
import okhttp3.*;
import okhttp3.internal.Util;
import okio.BufferedSink;
import okio.Okio;
import okio.Source;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author: Zed
 * @Date: 2022/2/22
 * @Description:
 */
public class UploadFileService {

  public static void upload(Map<String, InputStream> inputStreamMap, File file, List<String> jsons, boolean latest, String hostAndPort, String accessCode) {
    String tokenUrl = hostAndPort + "/api/users/generatetoken";
    Map<String, String> param = new HashMap<>();
    param.put("accesscode", accessCode);
    String jsonString = JSON.toJSONString(param);
    String s = OkHttpUtils.postJsonParams(tokenUrl, jsonString);

    System.out.println("generate token " + s);
    Map map = JSON.parseObject(s, Map.class);
    Object data = map.get("data");
    JSONObject data1 = (JSONObject) data;
    String token = (String) data1.get("id");


    MultipartBody.Builder builder = new MultipartBody.Builder();
    builder.setType(MultipartBody.FORM);
    if (file != null) {
      RequestBody body = RequestBody.create(MediaType.parse("image/*"), file);
      builder.addFormDataPart("file", file.getName(), body);
    }

    if (inputStreamMap != null) {
      inputStreamMap.forEach((k, v) ->{
        RequestBody body = create(MediaType.parse("image/*"), v);
        builder.addFormDataPart("file", k, body);
      });

    }

    //要上传的文字参数
    if (jsons != null && !jsons.isEmpty()) {
      for (String json : jsons) {
        builder.addFormDataPart("source", json);
      }
      // if the jsons size == 1, the data received by TM will be weird, adding an empty string helps TM receive the
      // proper data; the empty string should be dealt in TM.
      if (jsons.size() == 1) {
        builder.addFormDataPart("source", "");
      }
    }    // whether replace the latest version
    builder.addFormDataPart("latest", String.valueOf(latest));

    String url = hostAndPort + "/api/pdk/upload/source?access_token=" + token;

    Request request = new Request.Builder()
      .url(url)
      .post(builder.build())
      .build();

    try {
      new OkHttpClient().newCall(request).execute();
    } catch (Exception e) {
      System.out.println("call register interface failed, e =" + e);
    }
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
