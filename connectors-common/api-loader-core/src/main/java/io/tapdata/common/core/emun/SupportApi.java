package io.tapdata.common.core.emun;

import io.tapdata.common.support.PostManAPIInvoker;

public enum SupportApi {
    POST_MAN("POST_MAN", PostManAPIInvoker.class),
    ;
    String apiName;
    Class aClass;
    SupportApi(String apiName,Class aClass){
        this.apiName = apiName;
        this.aClass = aClass;
    }
    public Class aClass(){return this.aClass;}
    public String apiName(){
        return this.apiName;
    }
}
