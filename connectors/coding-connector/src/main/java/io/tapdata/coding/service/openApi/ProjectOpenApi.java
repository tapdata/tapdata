package io.tapdata.coding.service.openApi;

import io.tapdata.coding.service.openApi.lamada.HttpBodyChecker;
import io.tapdata.coding.service.openApi.lamada.HttpResultGet;

import java.util.Collections;
import java.util.Map;

public class ProjectOpenApi extends OpenApi {
    public ProjectOpenApi(Map<String,String> httpHeard,String openUrl){
       super(httpHeard, openUrl);
    }

    public static ProjectOpenApi create(Map<String,String> httpHeard,String openUrl){
        return new ProjectOpenApi(httpHeard,openUrl);
    }

    public Map<String,Object> createCodingProject(Map<String,Object> httpBody){
        return (Map<String, Object>) this.http(()-> HttpBodyChecker.verify(httpBody,Action.CreateIteration,"ProjectName","Name"),httpBody,null);
    }
    public Integer createCodingProjectAsSimpleResultBack(Map<String,Object> httpBody){
        return (Integer) this.http(() ->  HttpBodyChecker.verify(httpBody,Action.CreateIteration,"ProjectName","Name")
                , httpBody
                , (response)-> {
                    Object byKey = HttpResultGet.getByKey("Response.ProjectId", response);
                    if (byKey instanceof Integer) {
                        return (Integer)byKey;
                    }
                    return -1;
                });
    }

    public Map<String,Object> createProjectMember(Map<String,Object> httpBody){
        return (Map<String, Object>) this.http(()-> HttpBodyChecker.verify(httpBody,Action.CreateProjectMember,"ProjectId","Type","UserGlobalKeyList"),httpBody,null);
    }
    public Map<String,Object> createProjectMemberAsSimpleResultBack(Map<String,Object> httpBody){
        return (Map<String,Object>) this.http(() ->  HttpBodyChecker.verify(httpBody,Action.CreateProjectMember,"ProjectId","Type","UserGlobalKeyList")
                , httpBody
                , (response)-> {
                    Object byKey = HttpResultGet.getByKey("Response", response);
                    if (byKey instanceof Map) {
                        return (Map<String,Object>)byKey;
                    }
                    return Collections.emptyMap();
                });
    }
}
