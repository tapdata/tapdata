package io.tapdata.zoho.service.zoho.loader;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.zoho.entity.HttpEntity;
import io.tapdata.zoho.entity.HttpResult;
import io.tapdata.zoho.entity.HttpType;
import io.tapdata.zoho.utils.Checker;
import io.tapdata.zoho.utils.ZoHoHttp;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProductsOpenApi extends ZoHoStarter implements ZoHoBase {
    public final static String GET_PRODUCT_URL = "/api/v1/products/{product_id}";
    public final static String LIST_PRODUCT_URL = "/api/v1/products";

    private static final String TAG = ProductsOpenApi.class.getSimpleName();
    protected ProductsOpenApi(TapConnectionContext tapConnectionContext) {
        super(tapConnectionContext);
    }
    public ProductsOpenApi create(TapConnectionContext tapConnectionContext){
        return new ProductsOpenApi(tapConnectionContext);
    }

    @Override
    public TapConnectionContext getContext() {
        return this.tapConnectionContext;
    }

    public Map<String,Object> get(String productId){
        if (Checker.isEmpty(productId)){
            TapLogger.debug(TAG,"Department Id can not be null or not be empty.");
        }
        String accessToken = this.accessTokenFromConfig();
        HttpEntity<String,String> header = HttpEntity.create()
                .build("Authorization",accessToken);
        HttpEntity<String,String> resetFull = HttpEntity.create().build("product_id",productId);
        ZoHoHttp http = ZoHoHttp.create(String.format(ZO_HO_BASE_URL,GET_PRODUCT_URL), HttpType.GET,header).resetFull(resetFull);
        HttpResult httpResult = this.readyAccessToken(http);
        TapLogger.debug(TAG,"Get Product list succeed.");
        Map<String,Object> data = (Map<String,Object>)httpResult.getResult();
        return Checker.isEmpty(data)? new HashMap<>():data;
    }
    private List<Map<String,Object>> list(
            Integer from,
            Integer limit,
            Long deprecated,
            Long departmentId,
            Long ownerId,
            Long viewId,
            String sortBy,
            String fields,
            List<String> include
    ){
        HttpEntity<String,Object> form = HttpEntity.create();

        return list(form);
    }

    public List<Map<String,Object>> list(Integer from,Integer limit,String sortBy){
        return list(from, limit,null,0L,null,null,sortBy,null,null);
    }
    private List<Map<String,Object>> list(HttpEntity<String,Object> form){
        return null;
    }
}
