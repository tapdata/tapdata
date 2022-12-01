package io.tapdata.coding.postman;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class PostMain {
    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        Map<String,Object> jsonObject = JSONUtil.readJSONObject(new File("D:\\GavinData\\InstallPackage\\飞书\\data\\data\\Feishu_1658300789.zh_CN.postman_collection.json"), StandardCharsets.UTF_8);

        System.out.println(System.currentTimeMillis()-start);
    }

//    public static Map<String,Object> json(){
//        return JSONUtil.parseObj(PostMain.post_man);
//    }
}
