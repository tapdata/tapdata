package io.tapdata.proxy.client;

import com.tapdata.constant.ConfigurationCenter;
import io.tapdata.aspect.LoginSuccessfullyAspect;
import io.tapdata.entity.aspect.AspectObserver;
import io.tapdata.entity.aspect.annotations.AspectObserverClass;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.wsclient.modules.imclient.IMClient;
import io.tapdata.wsclient.modules.imclient.IMClientBuilder;

import java.util.ArrayList;
import java.util.List;

@AspectObserverClass(LoginSuccessfullyAspect.class)
public class LoginSuccessfullyObserver implements AspectObserver<LoginSuccessfullyAspect> {
    private static final String TAG = LoginSuccessfullyObserver.class.getSimpleName();
    private IMClient imClient;
    @Override
    public void observe(LoginSuccessfullyAspect aspect) {
        ConfigurationCenter configurationCenter = aspect.getConfigCenter();
        if(configurationCenter == null || configurationCenter.getConfig(ConfigurationCenter.TOKEN) == null) {
            TapLogger.error(TAG, "Access token not found after login successfully, can NOT continue login for websocket channel. ");
            return;
        }
        List<String> baseURLs = aspect.getBaseUrls();
        if(baseURLs == null || baseURLs.isEmpty()) {
            TapLogger.error(TAG, "baseURLs is empty, can NOT continue login for websocket channel");
            return;
        }
        String accessToken = (String) configurationCenter.getConfig(ConfigurationCenter.TOKEN);
        if(imClient == null) {
            synchronized (this) {
                if(imClient == null) {
                    List<String> newBaseUrls = new ArrayList<>();
                    for(String baseUrl : baseURLs) {
                        if(!baseUrl.endsWith("/"))
                            baseUrl = baseUrl + "/";
                        newBaseUrls.add(baseUrl + "proxy?access_token" + accessToken);
                    }
                    imClient = new IMClientBuilder()
                            .withBaseUrl(baseURLs)
                            .withService("engine")
                            .withPrefix("e")
                            .withClientId(ConfigurationCenter.processId)
                            .withTerminal(1)
                            .withToken(accessToken)
                            .build();
                }
            }
        }
    }
}
