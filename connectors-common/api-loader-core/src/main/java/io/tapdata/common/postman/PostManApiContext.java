package io.tapdata.common.postman;

import io.tapdata.common.support.comom.TapApiBase;
import io.tapdata.common.postman.entity.ApiEvent;
import io.tapdata.common.postman.entity.ApiInfo;
import io.tapdata.common.postman.entity.ApiMap;
import io.tapdata.common.postman.entity.ApiVariable;

import java.util.List;
import java.util.Map;

public class PostManApiContext  extends TapApiBase {
    public static PostManApiContext create(){
        return new PostManApiContext();
    }

    private ApiInfo info;

    private ApiMap apis;

    private ApiVariable variable;

    private ApiEvent event;

    public ApiInfo info(){
        return this.info;
    }

    public ApiMap apis(){
        return this.apis;
    }

    public ApiVariable variable(){
        return this.variable;
    }

    public ApiEvent event(){
        return this.event;
    }

    public PostManApiContext table(List<String> tables){
        super.tables(tables);
        return this;
    }

    public PostManApiContext info(ApiInfo info){
        this.info = info;
        return this;
    }

    public PostManApiContext apis(ApiMap apis){
        this.apis = apis;
        return this;
    }

    public PostManApiContext variable(ApiVariable variable){
        this.variable = variable;
        return this;
    }

    public PostManApiContext variableAdd(Map<String,Object> variable){
        this.variable.putAll(variable);
        return this;
    }

    public PostManApiContext event(ApiEvent event){
        this.event = event;
        return this;
    }

    public ApiMap.ApiEntity api(String apiUrlOrName, String method){
        return this.apis.quickGet(apiUrlOrName, method);
    }

}
