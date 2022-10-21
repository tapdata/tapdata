package io.tapdata.zoho.service.zoho.loader;

import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.zoho.entity.HttpEntity;
import io.tapdata.zoho.entity.HttpResult;
import io.tapdata.zoho.entity.HttpType;
import io.tapdata.zoho.enums.ZoHoTestItem;
import io.tapdata.zoho.utils.Checker;
import io.tapdata.zoho.utils.ZoHoHttp;

import java.util.Map;

import static io.tapdata.base.ConnectorBase.testItem;

public class ZoHoConnectionTest extends ZoHoStarter implements ZoHoBase {
    protected ZoHoConnectionTest(TapConnectionContext tapConnectionContext) {
        super(tapConnectionContext);
    }
    public static ZoHoConnectionTest create(TapConnectionContext tapConnectionContext){
        return new ZoHoConnectionTest(tapConnectionContext);
    }

    public TestItem testToken(){
        try {
            //随便掉一个接口看看accessToken是否过期
            String url = "/api/v1/organizations";
            HttpEntity<String,String> heard = HttpEntity.create().build("Authorization",accessTokenFromConfig());
            ZoHoHttp get = ZoHoHttp.create(String.format(ZO_HO_BASE_URL, url), HttpType.GET, heard);
            HttpResult httpResult = get.get();
            if (httpResult.isInvalidOauth()){
                try {
                    String newAccessToken = this.refreshAndBackAccessToken();
                    heard.build("Authorization", newAccessToken);
                    httpResult = get.get();
                    if (Checker.isEmpty(httpResult) || Checker.isEmpty(httpResult.getResult()) || Checker.isEmpty(((Map<String,Object>)httpResult.getResult()).get("data"))) {
                        return testItem(ZoHoTestItem.TOKEN_TEST.getContent(), TestItem.RESULT_FAILED, "Expired token.");
                    }
                    return testItem(ZoHoTestItem.TOKEN_TEST.getContent(),TestItem.RESULT_SUCCESSFULLY);
                }catch (Throwable throwable){
                    return testItem(ZoHoTestItem.TOKEN_TEST.getContent(), TestItem.RESULT_FAILED, "Expired token.");
                }
            }
            return testItem(ZoHoTestItem.TOKEN_TEST.getContent(),TestItem.RESULT_SUCCESSFULLY);
        }catch (Exception e){
            return testItem(ZoHoTestItem.TOKEN_TEST.getContent(),TestItem.RESULT_FAILED,"Test failed.");
        }
    }

    @Override
    public TapConnectionContext getContext() {
        return this.tapConnectionContext;
    }
}
