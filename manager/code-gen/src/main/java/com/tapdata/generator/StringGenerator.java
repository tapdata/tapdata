package com.tapdata.generator;

/**
 * @Author: Zed
 * @Date: 2021/9/3
 * @Description:
 */
public class StringGenerator implements Generator {
    @Override
    public String getType() {
        return "string";
    }
    @Override
    public String getJavaType() {
        return "String";
    }
}
