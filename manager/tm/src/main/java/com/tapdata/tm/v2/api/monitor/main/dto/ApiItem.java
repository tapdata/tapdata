package com.tapdata.tm.v2.api.monitor.main.dto;

import com.tapdata.tm.commons.base.DecimalFormat;
import com.tapdata.tm.commons.base.SortField;
import com.tapdata.tm.module.dto.ModulesDto;
import com.tapdata.tm.v2.api.monitor.utils.ApiPathUtil;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/12/31 09:03 Create
 * @description
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class ApiItem extends TopApiInServer {

    /**
     * 吞吐量
     */
    @SortField(name = {"totalRps"}, originField = {"bytes", "reqCount"})
    @DecimalFormat
    double totalRps;

    public static ApiItem create() {
        ApiItem item = new ApiItem();
        item.setRequestCount(0L);
        item.setErrorRate(0.0D);
        item.setErrorCount(0L);
        item.setResponseTimeAvg(0.0D);
        return item;
    }
}
