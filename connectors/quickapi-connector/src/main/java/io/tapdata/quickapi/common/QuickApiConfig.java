package io.tapdata.quickapi.common;

public class QuickApiConfig {
    public static QuickApiConfig create(){
        return new QuickApiConfig();
    }

    String apiType;
    public String apiType(){
        return this.apiType;
    }
    public QuickApiConfig apiConfig(String apiType){
        this.apiType = apiType;
        return this;
    }

    String jsonTxt;
    public String jsonTxt(){
        return this.jsonTxt;
    }
    public QuickApiConfig jsonTxt(String jsonTxt){
        this.jsonTxt = jsonTxt;
        return this;
    }

    String expireStatus;
    public String expireStatus(){
        return this.expireStatus;
    }
    public QuickApiConfig expireStatus(String expireStatus){
        this.expireStatus = expireStatus;
        return this;
    }

    String tokenParams;
    public String tokenParams(){
        return this.tokenParams;
    }
    public QuickApiConfig tokenParams(String tokenParams){
        this.tokenParams = tokenParams;
        return this;
    }

    String hookText;
    public String hookText(){
        return this.hookText;
    }
    public QuickApiConfig hookText(String hookText){
        this.hookText = hookText;
        return this;
    }
}
