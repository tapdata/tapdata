package com.tapdata.generator;

import com.tapdata.bean.Model;
import com.tapdata.config.Config;
import com.tapdata.utils.MapUtils;

import java.util.LinkedHashMap;

/**
 * @Author: Zed
 * @Date: 2021/9/3
 * @Description:
 */
public interface Generator {

    String getType();

    String getJavaType();


    default String write(Model model, Config config, LinkedHashMap<String, Object> hashMap, String name) {

        if (model != null) {
            StringBuilder fieldBuilder = model.getFieldBuilder();
            String description = MapUtils.getAsString(hashMap, "description");
            if (description != null) {
                fieldBuilder.append("    /** ").append(description).append(" */").append("\n");
            }
            fieldBuilder.append("    private ").append(getJavaType()).append(" ").append(name).append(";\n");
        }

        return null;
    }
}
