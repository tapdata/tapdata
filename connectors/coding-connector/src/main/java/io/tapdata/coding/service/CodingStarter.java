package io.tapdata.coding.service;

import io.tapdata.entity.utils.Entry;
import io.tapdata.pdk.apis.context.TapConnectionContext;

public abstract class CodingStarter {
    public static final String CONNECTION_URL = "https://%s.coding.net";
    public static final String OPEN_API_URL = "https://%s.coding.net/open-api";//%{s}---》teamName
    public static final String TOKEN_URL = "https://%s.coding.net/api/me";

    protected TapConnectionContext tapConnectionContext;
    protected boolean isVerify;
    CodingStarter(TapConnectionContext tapConnectionContext){
        this.tapConnectionContext = tapConnectionContext;
        this.isVerify = Boolean.FALSE;
    }

    protected static Entry entry(String key, Object value){
        return new Entry(key,value);
    }
}
