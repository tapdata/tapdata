package com.tapdata.tm.externalStorage.param;

import lombok.Data;

import java.util.List;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/6/4 10:19 Create
 * @description
 */
@Data
public class ConnectionSharedCDCParam {
    List<String> connectionIds;
}
