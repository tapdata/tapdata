package com.tapdata.tm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.tapdata.manager.common.utils.JsonUtil;
import com.tapdata.tm.base.dto.Where;
import org.apache.commons.collections.BidiMap;
import org.apache.commons.collections.bidimap.DualHashBidiMap;
import org.apache.commons.collections4.MapUtils;
import org.junit.jupiter.api.Test;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class URlEncodeTest {

    @Test
    public void encode() throws UnsupportedEncodingException {
//        String s = "{ \"id\" : \"619c62be9b71bd1318543d5a\" }";

        String s = "access_token=9072b986865c4acca8ab44dc513e7dcdfabcc8c4ea9e4a289eecf63e66754809&where={\"process_id\":\"f3ebe1b88623ca4f933af4e27f4075a0\",\"worker_type\":\"api-server\"}";
        String encodeS = java.net.URLEncoder.encode(s, "UTF-8");
        System.out.println(encodeS);

        System.out.println(java.net.URLDecoder.decode(encodeS, "UTF-8"));
    }


    public Where parseWhere(String whereJson) {
        replaceLoopBack(whereJson);
        return JsonUtil.parseJson(whereJson, Where.class);
    }

    /**
     * asdas
     *
     * @param json
     * @return
     */
    public String replaceLoopBack(String json) {
        if (com.tapdata.manager.common.utils.StringUtils.isNotBlank(json)) {
            json = json.replace("\"like\"", "\"$regex\"");
            json = json.replace("\"options\"", "\"$options\"");
            json = json.replace("\"$inq\"", "\"$in\"");
            json = json.replace("\"in\"", "\"$in\"");
        }
        return json;
    }


    @Test
    public void StrToMap() {
        String s = " {\"_id\":{\"in\":[\"61519f8ed51f7400c71cfdc1\",\"6155699428f0ea0052de4482\",\"61610d7c8ada020054d1226d\",\"61a49db2728c0100ad04c049\",\"61a49f4e728c0100ad04c14c\",\"6131c368a4a5140052ae130e\",\"61a49d67728c0100ad04c018\",\"61a49d6b728c0100ad04c01d\",\"619f3dd47e7bfb737e8c369f\",\"61a590dc728c0100ad055c3a\",\"6151918c562c8a0052ff1f07\",\"6151b142d51f7400c71d2bea\",\"615424a20e5b5800dbcb4dde\",\"6131d317a4a5140052ae1eed\"]}}";
        Where where = parseWhere(s);
        Map jsonObject = (Map) where.get("_id");
        List jsonArray = (List) jsonObject.get("in");
        //printResult(jsonArray);
    }

    @Test
    public void apacheCommons() {
        BidiMap bidiMap = new DualHashBidiMap();
        bidiMap.put("A", 1);
        bidiMap.put("B", 2);
        bidiMap.put("C", null);
        bidiMap.put("D", 4);
        System.out.println(bidiMap.getKey(4));
    }

    @Test
    public void testAes(){
    }

}
