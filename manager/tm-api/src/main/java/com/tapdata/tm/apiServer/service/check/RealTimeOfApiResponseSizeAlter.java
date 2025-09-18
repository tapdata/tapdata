package com.tapdata.tm.apiServer.service.check;

import java.util.List;
import java.util.Map;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/9/15 18:13 Create
 * @description
 */
public interface RealTimeOfApiResponseSizeAlter extends ApiServerCheckBase {
    void check(String userId, List<Map<String, Long>> apiReqBytesList);
}
