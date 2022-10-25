package io.tapdata.coding.service.openApi;

import io.tapdata.coding.service.openApi.lamada.HttpBodyChecker;
import io.tapdata.coding.service.openApi.lamada.HttpResultGet;
import io.tapdata.coding.utils.http.CodingHttp;

import java.util.HashMap;
import java.util.Map;

public abstract class OpenApi {
    Map<String,String> httpHeard;
    String openUrl;
    public OpenApi(Map<String,String> httpHeard,String openUrl){
        this.httpHeard = null == httpHeard ?new HashMap<>():httpHeard;
        this.openUrl = null == openUrl || "".equals(openUrl)? "":openUrl;
    }

    protected boolean verifyArgs(){
        return (null != this.httpHeard && !httpHeard.isEmpty()) && ( CodingHttp.isUrl(this.openUrl) );
    }

    protected Object http(HttpBodyChecker checker, Map<String,Object> httpBody, HttpResultGet get){
        if (!verifyArgs()) return null;
        Map<String,Object> result = null;
        return checker.checker() ?
                null != (result = CodingHttp.create(this.httpHeard, httpBody, this.openUrl).post()) ?
                        null == get ?
                                result
                                : get.result(result)
                        : null
                :null;
    }
}
