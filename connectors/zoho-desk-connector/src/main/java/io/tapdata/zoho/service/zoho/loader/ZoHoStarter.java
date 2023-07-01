package io.tapdata.zoho.service.zoho.loader;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.zoho.entity.ContextConfig;
import io.tapdata.zoho.entity.HttpEntity;
import io.tapdata.zoho.entity.HttpResult;
import io.tapdata.zoho.entity.RefreshTokenEntity;
import io.tapdata.zoho.enums.HttpCode;
import io.tapdata.zoho.utils.Checker;
import io.tapdata.zoho.utils.ZoHoHttp;

import java.util.Map;

import static io.tapdata.base.ConnectorBase.toJson;

public class ZoHoStarter {
    private static final String TAG = ZoHoStarter.class.getSimpleName();

    protected TapConnectionContext tapConnectionContext;
    protected boolean isVerify;
    public ZoHoStarter(TapConnectionContext tapConnectionContext){
        this.tapConnectionContext = tapConnectionContext;
        this.isVerify = Boolean.FALSE;
    }

    public HttpEntity<String,String> requestHeard(){
        ContextConfig contextConfig = this.veryContextConfigAndNodeConfig();
        String accessToken = this.accessTokenFromConfig();
        HttpEntity<String,String> header = HttpEntity.create().build("Authorization",accessToken);
        String orgId = contextConfig.orgId();
        if (Checker.isNotEmpty(orgId)){
            header.build("orgId",orgId);
        }
        return header;
    }

    public TapConnectionContext getContext() {
        return this.tapConnectionContext;
    }
    /**
     * 校验connectionConfig配置字段
     */
    public void verifyConnectionConfig(){
        if(this.isVerify){
            return;
        }
        if (null == this.tapConnectionContext){
            throw new IllegalArgumentException("TapConnectorContext cannot be null");
        }
        DataMap connectionConfig = this.tapConnectionContext.getConnectionConfig();
        if (null == connectionConfig ){
            throw new IllegalArgumentException("TapTable' DataMap cannot be null");
        }
        String refreshToken = connectionConfig.getString("refreshToken");
        String accessToken = connectionConfig.getString("accessToken");
        accessToken = accessToken.startsWith(ZoHoBase.ZO_HO_ACCESS_TOKEN_PREFIX)?accessToken:ZoHoBase.ZO_HO_ACCESS_TOKEN_PREFIX+accessToken;
        String clientId = connectionConfig.getString("clientID");
        String clientSecret = connectionConfig.getString("clientSecret");
        String orgId = connectionConfig.getString("orgId");
        String generateCode = connectionConfig.getString("generateCode");
        if ( null == refreshToken || "".equals(refreshToken)){
            TapLogger.debug(TAG, "Connection parameter exception: {} ", refreshToken);
        }
        if ( null == clientId || "".equals(clientId) ){
            TapLogger.debug(TAG, "Connection parameter exception: {} ", clientId);
        }
        if ( null == clientSecret || "".equals(clientSecret) ){
            TapLogger.debug(TAG, "Connection parameter exception: {} ", clientSecret);
        }
        if ( null == generateCode || "".equals(generateCode) ){
            TapLogger.debug(TAG, "Connection parameter streamReadType exception: {} ", generateCode);
        }
        if ( null == accessToken || "".equals(accessToken) ){
            TapLogger.debug(TAG, "Connection parameter connectionMode exception: {} ", accessToken);
        }
        this.isVerify = Boolean.TRUE;
    }
    public ContextConfig veryContextConfigAndNodeConfig(){
        this.verifyConnectionConfig();
        DataMap connectionConfigConfigMap = this.tapConnectionContext.getConnectionConfig();

        String refreshToken = connectionConfigConfigMap.getString("refreshToken");
        String accessToken = connectionConfigConfigMap.getString("accessToken");
        accessToken = accessToken.startsWith(ZoHoBase.ZO_HO_ACCESS_TOKEN_PREFIX)?accessToken:ZoHoBase.ZO_HO_ACCESS_TOKEN_PREFIX+accessToken;
        String clientId = connectionConfigConfigMap.getString("clientID");
        String clientSecret = connectionConfigConfigMap.getString("clientSecret");
        String orgId = connectionConfigConfigMap.getString("orgId");
        String generateCode = connectionConfigConfigMap.getString("generateCode");
        String connectionMode = connectionConfigConfigMap.getString("connectionMode");
        ContextConfig config = ContextConfig.create().refreshToken(refreshToken)
                .accessToken(accessToken)
                .clientId(clientId)
                .clientSecret(clientSecret)
                .orgId(orgId)
                .generateCode(generateCode)
                .connectionMode(connectionMode);
        if (this.tapConnectionContext instanceof TapConnectorContext) {

            DataMap nodeConfig = this.tapConnectionContext.getNodeConfig();
            config.fields(nodeConfig.get("customFieldKeys"))
                    .needDetailObj(nodeConfig.get("needDetail"))
                    .sortType(nodeConfig.get("sortType"));
            KVMap<Object> stateMap = ((TapConnectorContext) this.tapConnectionContext).getStateMap();
            if (null != stateMap) {
                Object refreshTokenObj = stateMap.get("refreshToken");
                Object accessTokenObj = stateMap.get("accessToken");
                if ( Checker.isEmpty(refreshTokenObj) ){
                    stateMap.put("refreshToken",refreshToken);
                    stateMap.put("accessToken",accessToken);
                }
            }
        }
        return config;
    }

    /**取AccessToken*/
    public String accessTokenFromConfig(){
        String accessToken = null;
        if (tapConnectionContext instanceof TapConnectorContext){
            TapConnectorContext connectorContext = (TapConnectorContext) this.tapConnectionContext;
            KVMap<Object> stateMap = connectorContext.getStateMap();
            accessToken = Checker.isNotEmpty(stateMap)?(String)(Checker.isEmpty(stateMap.get("accessToken"))?"":stateMap.get("accessToken")):"";
        }
        if(Checker.isEmpty(accessToken)){
            DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
            Object accessTokenObj = connectionConfig.get("accessToken");
            accessToken = Checker.isNotEmpty(accessTokenObj)?(String)accessTokenObj:"";
        }
        return ZoHoBase.builderAccessToken(accessToken);
    }
    /**取refreshToken*/
    public String refreshTokenFromConfig(){
        DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
        Object refreshTokenObj = connectionConfig.get("refreshToken");
        return Checker.isNotEmpty(refreshTokenObj)?(String)refreshTokenObj:"";
    }
    /**添加accessToken和refreshToken到stateMap*/
    public void addTokenToStateMap(){
        if (Checker.isEmpty(this.tapConnectionContext) || !(this.tapConnectionContext instanceof TapConnectorContext)){
            return;
        }
        ContextConfig contextConfig = this.veryContextConfigAndNodeConfig();
        TapConnectorContext connectorContext = (TapConnectorContext)this.tapConnectionContext;
        KVMap<Object> stateMap = connectorContext.getStateMap();
        stateMap.put("refreshToken",contextConfig.refreshToken());
        String accessToken = contextConfig.accessToken();
        stateMap.put("accessToken",ZoHoBase.builderAccessToken(accessToken));
    }
    public void addNewAccessTokenToStateMap(String accessToken){
        if (Checker.isEmpty(this.tapConnectionContext) || !(this.tapConnectionContext instanceof TapConnectorContext)){
            return;
        }
        TapConnectorContext connectorContext = (TapConnectorContext)this.tapConnectionContext;
        KVMap<Object> stateMap = connectorContext.getStateMap();
        stateMap.put("accessToken",accessToken);
    }

    /**
     * @deprecated
     * 刷新AccessToken并返回AccessToken*/
    public String refreshAndBackAccessToken(){
        RefreshTokenEntity refreshTokenEntity = TokenLoader.create(tapConnectionContext).refreshToken();
        String accessToken = refreshTokenEntity.getAccessToken();
        if (Checker.isEmpty(accessToken)){
            throw new CoreException("Refresh accessToken failed.");
        }
        return ZoHoBase.builderAccessToken(accessToken);
    }

    /**
     * 每次执行异常http请求后验证是否AccessToken过期
     * 过期刷新后重试异常
     * 没过期直接返回执行结果
     * */
    public HttpResult readyAccessToken(ZoHoHttp http){
        HttpEntity header = http.getHeard();
        HttpResult httpResult = http.http();
        if (Checker.isEmpty(httpResult) ) {
            TapLogger.debug(TAG,"Try to send once HTTP request, but AccessToken is timeout.");
        }
        String code = httpResult.getCode();
        header.build("Authorization",http.getHeard());
        if (HttpCode.INVALID_OAUTH.getCode().equals(code)){
            //重新获取超时的AccessToken，并添加到stateMap
            String newAccessToken = this.refreshAndBackAccessToken();
            this.addNewAccessTokenToStateMap(newAccessToken);
            header.build("Authorization",newAccessToken);
            httpResult = http.http();
            if (Checker.isEmpty(httpResult) || Checker.isEmpty(httpResult.getResult()) ){// || Checker.isEmpty(((Map<String,Object>)httpResult.getResult()).get("data"))){
                throw new CoreException("AccessToken refresh succeed, but retry http failed. ");
            }
        }
        if ("ERROR".equals(httpResult.getCode()) && (httpResult.httpCode() >= 300 || httpResult.httpCode() < 200)) {
            String msg = toJson(httpResult.getResult());
            throw new CoreException(msg);
        }
        return httpResult;
    }

}