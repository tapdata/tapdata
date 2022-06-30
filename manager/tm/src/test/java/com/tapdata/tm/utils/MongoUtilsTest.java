package com.tapdata.tm.utils;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.map.MapUtil;
import cn.hutool.json.JSON;
import cn.hutool.json.JSONUtil;
import com.tapdata.tm.BaseJunit;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Where;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.security.Key;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
class MongoUtilsTest extends BaseJunit {


    @Test
    void buildCriteria() {
        String s = "{\"where\":{\"type\":\"userOperation\",\"modular\":\"message\",\"operation\":\"read\",\"createTime\":{\"$gt\": {\"$date\": 1635696000000},\"$lt\": {\"$date\": 1637078400000}}},\"limit\":20,\"skip\":0,\"order\":\"createTime desc\"}";
        Filter filter = parseFilter(s);

        Where where = filter.getWhere();
        System.out.println(JSONUtil.toJsonStr(where));

    }

    public void showItem(HashMap<String, Object> map) {
        for (java.util.Map.Entry<String, Object> entry : map.entrySet()) {
            Object o = entry.getValue();
            if (o instanceof HashMap) {
                showItem((HashMap<String, Object>) o);
            } else {
                System.out.println(entry.getKey() + ": " + entry.getValue().toString());
            }
        }
    }

    @Test
    void applySort() {
    }

    @Test
    void applyField() {
    }

    @Test
    void toObjectId() {


    }
}