package com.tapdata.tm.commons.trace;

import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/5/27 17:24 Create
 * @description
 */
@Data
public class ChangeLogData {
    List<Map<String, Object>> logs;
    Long lastKey;
}
