package com.tapdata.generator;

/**
 * @Author: Zed
 * @Date: 2021/9/3
 * @Description:
 */
public class NullGenerator implements Generator {
    @Override
    public String getType() {
        return "null";
    }
    @Override
    public String getJavaType() {
        return "Object";
    }
}
