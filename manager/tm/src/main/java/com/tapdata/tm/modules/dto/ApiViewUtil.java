package com.tapdata.tm.modules.dto;

import com.fasterxml.jackson.databind.util.BeanUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ApiViewUtil {
    public static ApiView convert(Map<String, List<ModulesDto>> modules,String ip){
        ApiView apiView = new ApiView();
        List<ApiType> apiTypes = new ArrayList<>();
        modules.keySet().forEach(s -> {
            ApiType apiType = new ApiType();
            apiType.setApiTypeName(s);
            List<ApiModule> allModules = new ArrayList<>();
            modules.get(s).forEach(modulesDto -> {
                ApiModule module = new ApiModule();
                module.setName(modulesDto.getName());
                module.setIp(ip);
                module.setPath(modulesDto.getPaths().get(0).getPath());
                module.setDescription(modulesDto.getDescription());
                module.setFields(modulesDto.getPaths().get(0).getFields());
                module.setParams(modulesDto.getPaths().get(0).getParams());
                allModules.add(module);
            });
            apiType.setApiList(allModules);
            apiTypes.add(apiType);
        });
        apiView.setApiTypeList(apiTypes);
        return apiView;
    }
}
