package com.tapdata.tm.utils;

import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;


public class BeanUtil {
    private static Logger logger = LoggerFactory.getLogger(BeanUtil.class);

    public static <T, M> M deepClone(T source, Class<M> clazz) {
        try {
            M target = JSONObject.parseObject(JSONObject.toJSONString(source), clazz);

            return target;
        } catch (Exception ex) {
            logger.info("类复制失败，失败原因-->{}" + ex);
        }

        return null;
    }

    /**
     * 可用于使用json的子弹注解
     *
     * @param source
     * @param clazz
     * @param <T>
     * @param <M>
     * @return
     */
    public static <T, M> List<M> deepCloneList(T source, Class<M> clazz) {
        List<M> target = new ArrayList<>();
        try {
//            String sourceJson=JsonUtil.toJsonUseJackson(source);
            if (source instanceof List) {
                ((List<?>) source).forEach(item -> {
                    M singleTarget = cn.hutool.core.bean.BeanUtil.copyProperties(item, clazz);
                    target.add(singleTarget);
                });
            }

            return target;
        } catch (Exception ex) {
            logger.error("集合类克隆失败，失败原因-->{}" + ex);
        }
        return target;
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
            logger.info("集合类复制失败，失败原因-->{}" + e);
        }
        return null;
    }


}
