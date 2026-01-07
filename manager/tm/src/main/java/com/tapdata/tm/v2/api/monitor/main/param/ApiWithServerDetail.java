package com.tapdata.tm.v2.api.monitor.main.param;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/12/31 09:35 Create
 * @description
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class ApiWithServerDetail extends QueryBase {
    String apiId;
    String orderBy;
}
