package com.tapdata.tm.modules.vo;

import com.tapdata.tm.system.api.dto.TextEncryptionRuleDto;
import com.tapdata.tm.worker.dto.ApiServerWorkerInfo;
import com.tapdata.tm.worker.dto.ApiWorkerInfo;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class ApiDefinitionVo {
    String clusterId;
    List apis;
    List<ConnectionVo> connections;

    List<ApiServerWorkerInfo> workerInfo;

    /**
     * api所使用到的文本加密规则配置
     * */
    Map<String, TextEncryptionRuleDto> textEncryptionRules;

    List<ApiInfo> apiInfo;

    @Data
    public static class ApiInfo {
        String name;
        String url;
        boolean publish;
        String apiId;
        Map<String, String> pathSetting;
    }
}
