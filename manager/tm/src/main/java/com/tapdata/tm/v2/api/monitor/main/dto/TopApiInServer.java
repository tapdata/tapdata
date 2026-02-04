package com.tapdata.tm.v2.api.monitor.main.dto;

import com.tapdata.tm.commons.base.DecimalFormat;
import com.tapdata.tm.commons.base.SortField;
import com.tapdata.tm.module.dto.ModulesDto;
import com.tapdata.tm.module.dto.PathSetting;
import com.tapdata.tm.v2.api.monitor.utils.ApiPathUtil;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

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

    List<String> historyApiBeUsed = new ArrayList<>();

    public static TopApiInServer create() {
        TopApiInServer item = new TopApiInServer();
        item.setRequestCount(0L);
        item.setErrorRate(0.0D);
        item.setErrorCount(0L);
        item.setResponseTimeAvg(0.0D);
        return item;
    }

    public static <T extends TopApiInServer>List<T> supplement(List<T> topApiInServers, Map<String, ModulesDto> apiInfos, Function<ModulesDto, T> instance) {
        Map<String, ModulesDto> apiOfPath = new HashMap<>();
        apiInfos.values().forEach(apiInfo -> {
            List<PathSetting> pathSetting = apiInfo.getPathSetting();
            Map<String, String> collect = pathSetting.stream()
                    .filter(Objects::nonNull)
                    .filter(e -> Objects.nonNull(e.getMethod()))
                    .collect(Collectors.toMap(PathSetting::getMethod, PathSetting::getPath, (e1, e2) -> e2));
            String pathOfGetEnd = collect.computeIfAbsent(PathSetting.PathSettingType.DEFAULT_GET.getMethod(), k -> PathSetting.PathSettingType.DEFAULT_GET.getMethod());
            String pathGet = ApiPathUtil.apiPath(apiInfo.getApiVersion(), apiInfo.getBasePath(), apiInfo.getPrefix(), pathOfGetEnd);

            String pathOfPostEnd = collect.computeIfAbsent(PathSetting.PathSettingType.DEFAULT_POST.getMethod(), k -> PathSetting.PathSettingType.DEFAULT_POST.getMethod());
            String pathPost = ApiPathUtil.apiPath(apiInfo.getApiVersion(), apiInfo.getBasePath(), apiInfo.getPrefix(), pathOfPostEnd);
            apiOfPath.put(pathGet, apiInfo);
            apiOfPath.put(pathPost, apiInfo);
        });
        List<String> existsPaths = topApiInServers.stream()
                .filter(Objects::nonNull)
                .map(TopApiInServer::getApiPath)
                .filter(StringUtils::isNotBlank)
                .distinct()
                .toList();
        for (String path : apiOfPath.keySet()) {
            if (!existsPaths.contains(path)) {
                ModulesDto apiInfo = apiOfPath.get(path);
                T item = instance.apply(apiInfo);
                item.setApiPath(path);
                item.setApiId(path);
                item.setApiName(apiInfo.getName());
                topApiInServers.add(item);
            }
        }
        return topApiInServers;
    }
}
