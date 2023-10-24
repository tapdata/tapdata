package io.tapdata.pdk.apis.functions.connection.vo;

import java.util.List;
import java.util.Map;

/**
 * @author GavinXiao
 * @description TableWebsiteData create by Gavin
 * @create 2023/4/27 16:04
 **/
public class TableWebsiteData extends Website {
    Map<String, Map<String, String>> urls;

    public TableWebsiteData urls(Map<String, Map<String, String>> urls){
        this.urls = urls;
        return this;
    }

    public Map<String, Map<String, String>> getUrls(){
        return this.urls;
    }
    public void setUrls(Map<String, Map<String, String>> urls){
        this.urls = urls;
    }
}
