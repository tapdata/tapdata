package com.tapdata.generator;

/**
 * @Author: Zed
 * @Date: 2021/9/3
 * @Description:
 */
public class NumberGenerator implements Generator {
    @Override
    public String getType() {
        return "number";
    }
    @Override
    public String getJavaType() {
        return "Double";
    }
}
