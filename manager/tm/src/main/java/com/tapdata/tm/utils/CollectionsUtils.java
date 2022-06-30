package com.tapdata.tm.utils;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.aggregation.ConvertOperators;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class CollectionsUtils {


    /**
     * 可用于使用json的子弹注解
     * @param source
     * @param clazz
     * @param <T>
     * @param <M>
     * @return
     */
    public static <T, M> List<M> deepCloneList(T source, Class<M> clazz) {
        try {
            List<M> target = JSONObject.parseArray(JSONObject.toJSONString(source), clazz);
            return target;
        }catch (Exception ex){
            log.error("集合类克隆失败，失败原因-->{}" + ex);
        }

        return null;
    }

    public static <T> List<T> deepCopyList(List<T> src) {
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        ObjectOutputStream out = null;
        try {
            out = new ObjectOutputStream(byteOut);
            out.writeObject(src);
            ByteArrayInputStream byteIn = new ByteArrayInputStream(byteOut.toByteArray());
            ObjectInputStream in = new ObjectInputStream(byteIn);
            List<T> dest = (List<T>) in.readObject();
            return dest;
        } catch (Exception e) {
            log.error("集合类复制失败，失败原因-->{}" + e);
        }
        return null;
    }

    public static List<ObjectId> stringToObjectId(List<String> ids){
        List<ObjectId> objectIdList=new ArrayList<>();
        if (CollectionUtils.isNotEmpty(ids)){
            ids.forEach(id->{
                objectIdList.add(MongoUtils.toObjectId(id));
            });

        }
        return objectIdList;
    }

    public static List<String> ObjectIdToString(List<ObjectId> ids){
        List<String> objectIdList=new ArrayList<>();
        if (CollectionUtils.isNotEmpty(ids)){
            ids.forEach(id->{
                objectIdList.add(id.toString());
            });

        }
        return objectIdList;
    }
}
