package io.tapdata.pdk.apis.functions.connection.vo;

/**
 * @author GavinXiao
 * @description Website create by Gavin
 * @create 2023/4/27 16:08
 **/
public class Website {
    protected String url;

    public Website url(String url){
        this.url = url;
        return this;
    }

    public String getUrl(){
        return this.url;
    }

    public void setUrl(String url){
        this.url = url;
    }
}
