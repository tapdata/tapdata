package com.tapdata.generator;

/**
 * @Author: Zed
 * @Date: 2021/9/3
 * @Description:
 */
public class IntegerGenerator implements Generator {
    @Override
    public String getType() {
        return "integer";
    }
    @Override
    public String getJavaType() {
        return "Integer";
    }
}
