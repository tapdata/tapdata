package io.tapdata.coding.service;

import io.tapdata.entity.utils.Entry;
import io.tapdata.pdk.apis.context.TapConnectionContext;

public abstract class CodingStarter {
    public static final String CONNECTION_URL = "https://%s.coding.net";
    public static final String OPEN_API_URL = "https://%s.coding.net/open-api";//%{s}---》teamName
    public static final String TOKEN_URL = "https://%s.coding.net/api/me";

    protected TapConnectionContext tapConnectionContext;
    CodingStarter(TapConnectionContext tapConnectionContext){
        this.tapConnectionContext = tapConnectionContext;
    }

    protected Entry entry(String key, Object value){
        return new Entry(key,value);
    }
}
