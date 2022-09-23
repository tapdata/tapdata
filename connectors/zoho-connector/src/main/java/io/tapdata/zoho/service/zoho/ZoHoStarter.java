package io.tapdata.zoho.service.zoho;

import io.tapdata.pdk.apis.context.TapConnectionContext;

public abstract class ZoHoStarter {
    public static final String CONNECTION_URL = "https://%s.coding.net";
    public static final String OPEN_API_URL = "https://%s.coding.net/open-api";//%{s}---》teamName
    public static final String TOKEN_URL = "https://%s.coding.net/api/me";

    protected TapConnectionContext tapConnectionContext;
    protected boolean isVerify;
    protected ZoHoStarter(TapConnectionContext tapConnectionContext){
        this.tapConnectionContext = tapConnectionContext;
        this.isVerify = Boolean.FALSE;
    }

//    protected static Entry entry(String key, Object value){
//        return new Entry(key,value);
//    }
}