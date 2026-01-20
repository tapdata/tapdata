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
import java.util.function.Function;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/12/30 18:30 Create
 * @description
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class TopApiInServer extends DataValueBase {
    String apiId;
    String apiName;
    String apiPath;
    boolean notExistsApi;
    @SortField(name = {"requestCount", "rc"}, normal = true, originField = {"reqCount"})
    Long requestCount;
    @DecimalFormat
    @SortField(name = {"errorRate", "er"}, originField = {"reqCount", "errorCount"})
    Double errorRate;
    @SortField(name = {"errorCount"}, originField = {"reqCount", "errorCount"})
    long errorCount;

    public static TopApiInServer create() {
        TopApiInServer item = new TopApiInServer();
        item.setRequestCount(0L);
        item.setErrorRate(0.0D);
        item.setErrorCount(0L);
        item.setResponseTimeAvg(0.0D);
        return item;
    }

    public static <T extends TopApiInServer>List<T> supplement(List<T> topApiInServers, Map<String, ModulesDto> apiInfos, Function<ModulesDto, T> instance) {
        List<String> existsApiIds = topApiInServers.stream()
                .filter(Objects::nonNull)
                .map(TopApiInServer::getApiId)
                .filter(StringUtils::isNotBlank)
                .distinct()
                .toList();
        for (String apiId : apiInfos.keySet()) {
            if (!existsApiIds.contains(apiId)) {
                ModulesDto apiInfo = apiInfos.get(apiId);
                T item = instance.apply(apiInfo);
                item.setApiId(apiId);
                item.setApiName(apiInfos.get(apiId).getName());
                String path = ApiPathUtil.apiPath(apiInfo.getApiVersion(), apiInfo.getBasePath(), apiInfo.getPrefix());
                item.setApiPath(path);
                topApiInServers.add(item);
            }
        }
        return topApiInServers;
    }
}
