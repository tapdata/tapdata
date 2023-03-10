package io.tapdata.zoho.service.zoho.loader;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.zoho.entity.ContextConfig;
import io.tapdata.zoho.entity.HttpEntity;
import io.tapdata.zoho.entity.HttpResult;
import io.tapdata.zoho.entity.HttpType;
import io.tapdata.zoho.utils.Checker;
import io.tapdata.zoho.utils.ZoHoHttp;

import java.util.ArrayList;
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
    public static ProductsOpenApi create(TapConnectionContext tapConnectionContext){
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
        HttpEntity<String, String> header = requestHeard();
        HttpEntity<String,String> resetFull = HttpEntity.create().build("product_id",productId);
        ZoHoHttp http = ZoHoHttp.create(String.format(ZO_HO_BASE_URL,GET_PRODUCT_URL), HttpType.GET,header).resetFull(resetFull);
        HttpResult httpResult = this.readyAccessToken(http);
        TapLogger.debug(TAG,"Get Product list succeed.");
        Map<String,Object> data = (Map<String,Object>)httpResult.getResult();
        return Checker.isEmpty(data)? new HashMap<>():data;
    }

    public static final int MAX_PAGE_LIMIT = 100;
    public static final int MIN_PAGE_LIMIT = 1;
    public static final int DEFAULT_PAGE_LIMIT = 10;
    public static final int MIN_FROM = 0;
    private List<Map<String,Object>> page(
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
        if (Checker.isEmpty(from) || from < MIN_FROM) from = MIN_FROM;
        if (Checker.isEmpty(limit) || limit < MIN_PAGE_LIMIT || limit > MAX_PAGE_LIMIT) limit = DEFAULT_PAGE_LIMIT;
        form.build("from",from);
        form.build("limit",limit);
        if (Checker.isNotEmpty(deprecated)) form.build("deprecated",deprecated);
        if (Checker.isNotEmpty(departmentId)) form.build("departmentId",departmentId);
        if (Checker.isNotEmpty(ownerId)) form.build("ownerId",ownerId);
        if (Checker.isNotEmpty(viewId)) form.build("viewId",viewId);
        if (Checker.isNotEmpty(sortBy)) form.build("sortBy",sortBy);
        if (Checker.isNotEmpty(fields)) form.build("fields",fields);
        if (Checker.isNotEmpty(include)) form.build("include",include);
        return list(form);
    }

    public List<Map<String,Object>> page(Integer from,Integer limit,String sortBy){
        return page(from, limit,null,0L,null,null,sortBy,null,null);
    }
    private List<Map<String,Object>> list(HttpEntity<String,Object> form){
        HttpEntity<String, String> header = requestHeard();
        ZoHoHttp http = ZoHoHttp.create(String.format(ZO_HO_BASE_URL,LIST_PRODUCT_URL), HttpType.GET,header).header(header).form(form);
        HttpResult httpResult = this.readyAccessToken(http);
        TapLogger.debug(TAG,"Get product page succeed.");
        Object data = ((Map<String,Object>)httpResult.getResult()).get("data");
        return Checker.isEmpty(data)?new ArrayList<>():(List<Map<String,Object>>)data;
    }

    public static enum SortBy{
        /**
         * Sort by a specific attribute : productName, productCode, unitPrice, createdTime or modifiedTime.
         * The default sorting order is ascending.
         * A - prefix denotes descending order of sorting.
         * */
        PRODUCT_NAME("productName"),
        PRODUCT_CODE("productCode"),
        UNIT_PRICE("unitPrice"),
        CREATED_TIME("createdTime"),
        MODIFIED_TIME("modifiedTime"),
        ;
        String sortBy;
        SortBy(String sortBy){
            this.sortBy = sortBy;
        }

        public String getSortBy() {
            return sortBy;
        }
        public String descSortBy(){
            return "-"+sortBy;
        }

        public void setSortBy(String sortBy) {
            this.sortBy = sortBy;
        }
    }
}
