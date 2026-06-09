package com.tapdata.tm.utils;

import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.tapdata.tm.base.dto.Filter;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/6/3 10:54 Create
 * @description
 */
public final class TapQueryUtil {
    private TapQueryUtil() {}

    public static Query buildQuery(Filter filter, Class<?> clazz) {
        JSONObject jsonObject = JSONUtil.parseObj(filter);
        JSONObject whereJsonObject = jsonObject.getJSONObject("where");
        Sort sort = buildSort(jsonObject, clazz);
        Query query =  buildQuery(whereJsonObject);
        query.with(sort);
        return query;
    }

    public static Sort buildSort(JSONObject jsonObject, Class<?> clazz) {
        Sort sort = Sort.by("createAt").descending();
        if (jsonObject == null) {
            return sort;
        }
        JSONArray orderJsonObject = jsonObject.getJSONArray("order");
        if (orderJsonObject == null) {
            return sort;
        }
        List<Field> fieldLis = Arrays.asList(clazz.getDeclaredFields());
        Sort sorted = null;
        for (Object sortItem : orderJsonObject) {
            for (java.lang.reflect.Field field : fieldLis) {
                String orderByString = sortItem.toString();
                String filedName = field.getName();
                if (!orderByString.contains(filedName)) {
                    continue;
                }
                Sort item = null;
                if (orderByString.contains("ASC")) {
                    item = Sort.by(filedName).ascending();
                } else if (orderByString.contains("DESC")) {
                    item = Sort.by(filedName).descending();
                }
                if (item != null) {
                    if (sorted == null) {
                        sorted = item;
                    } else {
                        sorted = sorted.and(item);
                    }
                }
            }
        }
        return null != sorted ? sorted : sort;
    }

    public static Query buildQuery(JSONObject whereJsonObject) {
        Query query = new Query();
        if (null != whereJsonObject) {
            for (Map.Entry<String, Object> entry : whereJsonObject.entrySet()) {
                String proName = entry.getKey();
                String proValue = entry.getValue().toString();
                if (proValue.contains("$in")) {
                    JSONObject jsonObjectValue = JSONUtil.parseObj(proValue);
                    JSONArray jsonArray = jsonObjectValue.getJSONArray("$in");
                    List<?> list = jsonArray.toList(Object.class);
                    query.addCriteria(Criteria.where(proName).in(list));
                } else {
                    query.addCriteria(Criteria.where(proName).is(proValue));
                }
            }
        }
        query.addCriteria(Criteria.where("is_deleted").ne(true));
        return query;
    }
}
