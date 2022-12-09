package io.tapdata.proxy.client;

import com.tapdata.constant.ConfigurationCenter;
import io.tapdata.aspect.LoginSuccessfullyAspect;
import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.aspect.AspectObserver;
import io.tapdata.entity.aspect.annotations.AspectObserverClass;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.wsclient.modules.imclient.IMClient;
import io.tapdata.wsclient.modules.imclient.IMClientBuilder;

import java.util.ArrayList;
import java.util.List;

@AspectObserverClass(LoginSuccessfullyAspect.class)
public class LoginSuccessfullyObserver implements AspectObserver<LoginSuccessfullyAspect> {
    private static final String TAG = LoginSuccessfullyObserver.class.getSimpleName();
    @Bean
    private ProxySubscriptionManager proxySubscriptionManager;
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
        proxySubscriptionManager.startIMClient(baseURLs, accessToken);

        proxySubscriptionManager.setProcessId(ConfigurationCenter.processId);
        proxySubscriptionManager.setUserId((String) configurationCenter.getConfig(ConfigurationCenter.USER_ID));

        PDKIntegration.registerMemoryFetcher(ProxySubscriptionManager.class.getSimpleName(), proxySubscriptionManager);
    }


}
