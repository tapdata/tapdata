package io.tapdata.quickapi.server;

import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.quickapi.common.QuickApiConfig;

import java.util.Objects;

import static io.tapdata.base.ConnectorBase.toJson;

public class QuickApiBase {
    private static final String TAG = QuickApiBase.class.getSimpleName();

    protected TapConnectionContext connectionContext;
    protected QuickApiConfig config;

    protected QuickApiBase(TapConnectionContext connectionContext){
        DataMap connectionConfig = connectionContext.getConnectionConfig();
        config = QuickApiConfig.create();
        this.connectionContext = connectionContext;
        if (Objects.nonNull(connectionConfig)) {
            String apiType = connectionConfig.getString("apiType");
            if (Objects.isNull(apiType)) apiType = "POST_MAN";
            String jsonTxt = connectionConfig.getString("jsonTxt");
            if (Objects.isNull(jsonTxt)){
                throw new RuntimeException("API JSON must be not null or not empty. ");
            }
            try {
                toJson(jsonTxt);
            }catch (Exception e){
                throw new RuntimeException("API JSON only JSON format. ");
            }
            String expireStatus = connectionConfig.getString("expireStatus");
            String tokenParams = connectionConfig.getString("tokenParams");
            config.apiConfig(apiType)
                    .expireStatus(expireStatus)
                    .tokenParams(tokenParams)
                    .jsonTxt(jsonTxt);
        }
    }

}
