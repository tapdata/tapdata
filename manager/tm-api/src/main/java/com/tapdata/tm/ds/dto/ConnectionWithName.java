package com.tapdata.tm.ds.dto;

import lombok.Data;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/4/29 16:53 Create
 * @description
 */
@Data
public class ConnectionWithName {
    String id;

    String name;

    int sort;

    public ConnectionWithName() {
    }

    public ConnectionWithName(String id, String name) {
        this.id = id;
        this.name = name;
    }
}
