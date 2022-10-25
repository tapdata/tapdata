package io.tapdata.coding.service.openApi;

import io.tapdata.coding.service.openApi.lamada.HttpBodyChecker;
import io.tapdata.coding.service.openApi.lamada.HttpResultGet;

import java.util.Collections;
import java.util.Map;

public class IterationsOpenApi extends OpenApi {

    public IterationsOpenApi(Map<String,String> httpHeard,String openUrl){
        super(httpHeard, openUrl);
    }
    public static IterationsOpenApi create(Map<String,String> httpHeard,String openUrl){
        return new IterationsOpenApi(httpHeard,openUrl);
    }

    /**创建迭代
     *
     参数名称	必选	类型	描述
     Action	是	String	公共参数，本接口取值：CreateIteration
     ProjectName	是	String	项目名称
     Name	是	String	标题
     Goal	否	String	目标
     Assignee	否	Integer	处理人 ID
     StartAt	否	String	开始时间，格式：2020-01-01
     EndAt	否	String	结束时间，格式：2020-01-01
     * */
    public Map<String,Object> createCodingProject(Map<String,Object> httpBody){
        return (Map<String, Object>) this.http(()-> HttpBodyChecker.verify(httpBody,Action.CreateIteration,"ProjectName","Name"),httpBody,null);
    }
    public Map<String,Object> createCodingProjectAsSimpleResultBack(Map<String,Object> httpBody){
        return (Map<String, Object>) this.http(() ->  HttpBodyChecker.verify(httpBody,Action.CreateIteration,"ProjectName","Name")
                , httpBody
                , (response)-> {
                    Object byKey = HttpResultGet.getByKey("Response.Iteration", response);
                    if (byKey instanceof Map) return (Map<String,Object>)byKey;
                    return Collections.emptyMap();
                });
    }

}
