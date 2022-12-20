//package io.tapdata.quickapi.invoker;
//
//import io.tapdata.pdk.apis.api.APIInvoker;
//import io.tapdata.pdk.apis.api.APIResponse;
//import io.tapdata.pdk.apis.api.APIResponseInterceptor;
//import io.tapdata.pdk.apis.api.comom.APIDocument;
//import io.tapdata.pdk.apis.api.comom.TapApiBase;
//import io.tapdata.common.support.postman.entity.ApiEvent;
//import io.tapdata.common.support.postman.entity.ApiInfo;
//import io.tapdata.common.support.postman.entity.ApiMap;
//import io.tapdata.common.support.postman.entity.ApiVariable;
//
//import java.util.List;
//import java.util.Map;
//
//public abstract class PostManAPIInvoker
//        extends TapApiBase
//        implements APIDocument<PostManAPIInvoker>, APIInvoker {
//    private ApiInfo info;
//    private ApiMap apis;
//    private ApiVariable variable;
//    private ApiEvent event;
//
//    public ApiInfo info(){
//        return this.info;
//    }
//    public ApiMap apis(){
//        return this.apis;
//    }
//    public ApiVariable variable(){
//        return this.variable;
//    }
//    public ApiEvent event(){
//        return this.event;
//    }
//    public PostManAPIInvoker table(List<String> tables){
//        super.tables(tables);
//        return this;
//    }
//    public PostManAPIInvoker info(ApiInfo info){
//        this.info = info;
//        return this;
//    }
//    public PostManAPIInvoker apis(ApiMap apis){
//        this.apis = apis;
//        return this;
//    }
//    public PostManAPIInvoker variable(ApiVariable variable){
//        this.variable = variable;
//        return this;
//    }
//    public PostManAPIInvoker variableAdd(Map<String,Object> variable){
//        this.variable.putAll(variable);
//        return this;
//    }
//    public PostManAPIInvoker event(ApiEvent event){
//        this.event = event;
//        return this;
//    }
//    public ApiMap.ApiEntity api(String apiUrlOrName, String method){
//        return this.apis.quickGet(apiUrlOrName, method);
//    }
//
//    @Override
//    public APIResponse invoke(String uriOrName, String method, Map<String, Object> params) {
//        ApiMap.ApiEntity api = api(uriOrName, method);
//
//        return null;
//    }
//
//    @Override
//    public void setAPIResponseInterceptor(APIResponseInterceptor interceptor) {
//
//    }
//}
