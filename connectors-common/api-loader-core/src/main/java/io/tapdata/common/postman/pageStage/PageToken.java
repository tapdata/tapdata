package io.tapdata.common.postman.pageStage;

import io.tapdata.common.postman.entity.params.Api;
import io.tapdata.common.support.APIInvoker;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.common.support.entitys.APIResponse;
import io.tapdata.common.support.core.emun.TapApiTag;
import io.tapdata.common.postman.entity.ApiMap;
import io.tapdata.common.postman.enums.PostParam;
import io.tapdata.common.postman.util.ApiMapUtil;

import java.util.*;
import java.util.function.BiConsumer;

public class PageToken implements PageStage {
    private static final String TAG = PageToken.class.getSimpleName();
    @Override
    public void page(TapPage tapPage) {
        ApiMap.ApiEntity api = tapPage.api();
        Api requestApi = api.api();
        APIInvoker invoker = tapPage.invoker();
        BiConsumer<List<TapEvent>, Object> consumer = tapPage.consumer();

        String apiName = api.name();
        String apiMethod = api.method();
        Map<String, Object> param = tapPage.apiParam();

        //@TODO verify param not null
        List<Map<String, Object>> query = api.api().request().url().query();
        String sizeKeyName = null;
        String tokenName = null;
        String hasNextName = null;
        int sizeValue = 0;
        String tokenValue = null;

        for (Map<String, Object> queryMap : query) {
            Object queryParamDescription = queryMap.get(PostParam.DESCRIPTION);
            Object keyObj = queryMap.get(PostParam.KEY);
            Object valueObj = queryMap.get(PostParam.VALUE);
            String objKeyName = Objects.isNull(keyObj)?null:String.valueOf(keyObj);
            if (Objects.equals(queryParamDescription, TapApiTag.TAP_PAGE_SIZE.tagName())) {
                sizeKeyName = objKeyName;
                sizeValue = Integer.parseInt(Objects.isNull(valueObj)?"20":String.valueOf(valueObj));
            }
            if (Objects.equals(queryParamDescription, TapApiTag.TAP_PAGE_TOKEN.tagName())) {
                tokenName = objKeyName;
                tokenValue = Objects.isNull(valueObj)?"":String.valueOf(valueObj);
            }
            if(Objects.equals(queryParamDescription, TapApiTag.TAP_HAS_MORE_PAGE.tagName())){
                hasNextName = objKeyName;
            }
        }

        if (Objects.isNull(sizeKeyName) ){
            throw new CoreException("TAP_PAGE_SIZE is selected as the paging mode for table ["+tapPage.tableName()+"], but the table level paging start value field of \"TAP_PAGE_SIZE\" is not used in the paging parameters ");
        }
        if (Objects.isNull(hasNextName) ){
            throw new CoreException("TAP_HAS_MORE_PAGE is selected as the paging mode for table ["+tapPage.tableName()+"], but the table level paging start value field of \"TAP_HAS_MORE_PAGE\" is not used in the paging parameters ");
        }
        if (Objects.isNull(tokenName) ){
            throw new CoreException("TAP_PAGE_TOKEN is selected as the paging mode for table ["+tapPage.tableName()+"], but the table level paging start value field of \"TAP_PAGE_TOKEN\" is not used in the paging parameters ");
        }
        String pageResultPath = requestApi.pageResultPath();
        if (Objects.isNull(pageResultPath)){
            throw new CoreException("The table data source field is not specified in the interface return result.");
        }
        //获取首次请求结果
        if(Objects.isNull(param)){
            param = new HashMap<>();
        }
        param.put(sizeKeyName,sizeValue);
        APIResponse apiResponse = invoker.invoke(apiName, param, apiMethod,true);
        Map<String, Object> result = apiResponse.result();
        while (this.accept(result,tapPage,pageResultPath)){
            apiResponse = invoker.invoke(apiName, param, apiMethod,true);
            result = apiResponse.result();
            param.put(tokenName, tokenValue = getPageToken(result,tokenName));
            if(!hasNext(result,hasNextName)){
                break;
            }
        }
    }

    private String getPageToken(Map<String, Object> result,String keyName){
        if (Objects.isNull(result)) return "";
        return (String) ApiMapUtil.depthSearchParamFromMap(result,keyName);
    }

    private Boolean hasNext(Map<String, Object> result,String keyName){
        if (Objects.isNull(result)) return false;
        try {
            if (keyName.contains(".")) {
                return (Boolean) ApiMapUtil.getKeyFromMap(result, keyName);
            }else {
                return (Boolean) ApiMapUtil.depthSearchParamFromMap(result,keyName);
            }
        }catch (Exception e){
            return false;
        }
    }
}
