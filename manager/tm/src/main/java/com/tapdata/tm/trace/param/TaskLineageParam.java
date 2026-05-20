package com.tapdata.tm.trace.param;

import lombok.Data;

import java.util.List;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/5/20 16:37 Create
 * @description
 */
@Data
public class TaskLineageParam {
    String connectionId;
    String table;
    String type;
    List<String> traceFilterFieldNames;
}
