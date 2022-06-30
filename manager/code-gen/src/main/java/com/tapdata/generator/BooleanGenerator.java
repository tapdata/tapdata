package com.tapdata.generator;

/**
 * @Author: Zed
 * @Date: 2021/9/3
 * @Description:
 */
public class BooleanGenerator implements Generator {
    @Override
    public String getType() {
        return "boolean";
    }

    @Override
    public String getJavaType() {
        return "Boolean";
    }

}
