package io.tapdata.common.support.core.emun;


public enum SupportApi {
    POST_MAN("POST_MAN"),
    ;
    String apiName;
    SupportApi(String apiName){
        this.apiName = apiName;
    }
    public String apiName(){
        return this.apiName;
    }
}
