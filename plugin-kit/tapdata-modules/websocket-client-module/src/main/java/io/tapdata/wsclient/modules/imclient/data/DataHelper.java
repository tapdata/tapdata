package io.tapdata.wsclient.modules.imclient.data;

import com.alibaba.fastjson.JSON;
import io.tapdata.wsclient.modules.imclient.impls.data.Result;
import io.tapdata.wsclient.utils.LoggerEx;

import java.io.UnsupportedEncodingException;

public class DataHelper {

    public static byte[] toJsonBytes(IMData data) {
        String json = JSON.toJSONString(data);
        try {
            return json.getBytes("utf8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            LoggerEx.error("toContentBytes utf8 encode failed, " + e.getMessage(), e);
            return null;
        }
    }

    public static IMData fromJsonBytes(byte[] contentBytes, Class<? extends IMData> clazz) {
        try {
            String json = new String(contentBytes, "utf8");
            return JSON.parseObject(json, clazz);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            LoggerEx.error("fromContentBytes utf8 encode failed, " + e.getMessage(), e);
        }
        return null;
    }

    public static IMResult fromResult(Result result) {
        if(result == null) return null;
        IMResult imResult = new IMResult();
        imResult.setCode(result.getCode());
        imResult.setDescription(result.getDescription());
        imResult.setForId(result.getForId());
        imResult.setServerId(result.getServerId());
        imResult.setTime(result.getTime());
        imResult.setData(result.getContent());
        return imResult;
    }
}
