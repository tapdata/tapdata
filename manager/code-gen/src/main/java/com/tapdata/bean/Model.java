package com.tapdata.bean;

import lombok.Data;

/**
 * @Author: Zed
 * @Date: 2021/9/3
 * @Description:
 */
@Data
public class Model {

    /**  */
    private String path;
    private StringBuilder importClassBuilder = new StringBuilder();
    private String name;
    private StringBuilder fieldBuilder = new StringBuilder();

}
