package io.tapdata.zoho.service.zoho;

import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.zoho.entity.HttpEntity;
import io.tapdata.zoho.entity.HttpResult;
import io.tapdata.zoho.entity.HttpType;
import io.tapdata.zoho.enums.ZoHoTestItem;
import io.tapdata.zoho.utils.ZoHoHttp;

import static io.tapdata.base.ConnectorBase.testItem;

public class ZoHoConnectionTest extends ZoHoStarter implements ZoHoBase  {
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
            HttpResult httpResult = ZoHoHttp.create(String.format(ZO_HO_BASE_URL, url), HttpType.GET, heard).get();
            if (httpResult.isInvalidOauth()){
                return testItem(ZoHoTestItem.TOKEN_TEST.getContent(),TestItem.RESULT_FAILED,"Expired token.");
            }
            return testItem(ZoHoTestItem.TOKEN_TEST.getContent(),TestItem.RESULT_SUCCESSFULLY);
        }catch (Exception e){
            return testItem(ZoHoTestItem.TOKEN_TEST.getContent(),TestItem.RESULT_FAILED,e.getMessage());
        }
    }

}
