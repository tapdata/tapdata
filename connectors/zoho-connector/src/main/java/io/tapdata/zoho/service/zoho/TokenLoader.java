package io.tapdata.zoho.service.zoho;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.zoho.entity.*;
import io.tapdata.zoho.enums.HttpCode;
import io.tapdata.zoho.utils.ZoHoHttp;

public class TokenLoader extends ZoHoStarter implements ZoHoBase {
    private static final String TAG = TokenLoader.class.getSimpleName();
    protected TokenLoader(TapConnectionContext tapConnectionContext) {
        super(tapConnectionContext);
    }
    public static TokenLoader create(TapConnectionContext tapConnectionContext){
        return new TokenLoader(tapConnectionContext);
    }
    /**
     *  刷新accessToken
     * @user Gavin
     * */
    public RefreshTokenEntity refreshToken(){
        HttpResult post = refresh();
        String code = post.getCode();
        if (HttpCode.SUCCEED.getCode().equals(code)){
            return RefreshTokenEntity.create(post.getResult());
        }else {
            TapLogger.error(TAG,"{} | {}",code,post.getResult().get(HttpCode.ERROR.getCode()));
            throw new CoreException(code+"|"+post.getResult().get(HttpCode.ERROR.getCode()));
        }
    }
    public HttpResult refresh(){
        ContextConfig contextConfig = super.veryContextConfigAndNodeConfig();
        HttpEntity<String,Object> form = HttpEntity.create()
                .build("refresh_token",contextConfig.getRefreshToken())
                .build("client_id",contextConfig.getClientId())
                .build("client_secret",contextConfig.getClientSecret())
                .build("scope",ZO_HO_BASE_SCOPE)
                .build("redirect_uri","https://www.zylker.com/oauthgrant")
                .build("grant_type","refresh_token");
        ZoHoHttp http = ZoHoHttp.create(String.format(ZO_HO_BASE_TOKEN_URL,"/oauth/v2/token"), HttpType.POST).form(form);
        TapLogger.debug(TAG,"Try to refresh AccessToken.");
        HttpResult post = http.post();
        return post;
    }

    public TokenEntity getToken(){
        ContextConfig contextConfig = super.veryContextConfigAndNodeConfig();
        HttpEntity<String,Object> form = HttpEntity.create()
                .build("code",contextConfig.getGenerateCode())
                .build("client_id",contextConfig.getClientId())
                .build("client_secret",contextConfig.getClientSecret())
                .build("redirect_uri","https://www.zylker.com/oauthgrant")
                .build("grant_type","authorization_code");
        ZoHoHttp http = ZoHoHttp.create(String.format(ZO_HO_BASE_TOKEN_URL,"/oauth/v2/token"), HttpType.POST).form(form);
        TapLogger.debug(TAG,"Try to get AccessToken and RefreshToken.");
        return TokenEntity.create().entity(http.post()) ;
    }
}
