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
public class ApiItem extends DataValueBase {
    String apiId;
    String apiPath;
    String apiName;
    boolean notExistsApi;
    /**
     * 总调用数
     */
    @SortField(name = {"requestCount", "rc"}, normal = true)
    long requestCount;

    /**
     * 错误率
     */
    @DecimalFormat
    @SortField(name = {"errorRate"})
    double errorRate;

    @SortField(name = {"errorCount"})
    long errorCount;

    /**
     * 吞吐量
     */
    @SortField(name = {"totalRps"})
    @DecimalFormat
    double totalRps;


    public static List<ApiItem> supplement(List<ApiItem> topApiInServers, Map<String, ModulesDto> apiInfos) {
        List<String> existsApiIds = topApiInServers.stream()
                .filter(Objects::nonNull)
                .map(ApiItem::getApiId)
                .filter(StringUtils::isNotBlank)
                .distinct()
                .toList();
        for (String apiId : apiInfos.keySet()) {
            if (!existsApiIds.contains(apiId)) {
                ApiItem item = new ApiItem();
                item.setApiId(apiId);
                ModulesDto apiInfo = apiInfos.get(apiId);
                item.setApiName(apiInfos.get(apiId).getName());
                String path = ApiPathUtil.apiPath(apiInfo.getApiVersion(), apiInfo.getBasePath(), apiInfo.getPrefix());
                item.setApiPath(path);
                topApiInServers.add(item);
            }
        }
        return topApiInServers;
    }
}
