package io.tapdata.common.postman.pageStage;

import io.tapdata.common.support.APIInvoker;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.common.support.entitys.APIResponse;
import io.tapdata.common.support.core.emun.TapApiTag;
import io.tapdata.common.postman.entity.ApiMap;
import io.tapdata.common.postman.entity.params.Api;
import io.tapdata.common.postman.enums.PostParam;

import java.util.*;
import java.util.function.BiConsumer;

public class PageLimit implements PageStage {
    private static final String TAG = PageLimit.class.getSimpleName();
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
        String offsetKeyName = null;
        String limitKeyName = null;
        int offsetValue = 0;
        int limitValue = 0;

        for (Map<String, Object> queryMap : query) {
            Object queryParamDescription = queryMap.get(PostParam.DESCRIPTION);
            Object keyObj = queryMap.get(PostParam.KEY);
            Object valueObj = queryMap.get(PostParam.VALUE);
            String objKeyName = Objects.isNull(keyObj)?null:String.valueOf(keyObj);
            if (Objects.equals(queryParamDescription, TapApiTag.TAP_PAGE_OFFSET.tagName())) {
                offsetKeyName = objKeyName;
                offsetValue = Integer.parseInt(Objects.isNull(valueObj)?"1":String.valueOf(valueObj));
            }
            if (Objects.equals(queryParamDescription, TapApiTag.TAP_PAGE_LIMIT.tagName())) {
                limitKeyName = objKeyName;
                limitValue = Integer.parseInt(Objects.isNull(valueObj)?"20":String.valueOf(valueObj));
            }
        }
        if (Objects.isNull(offsetKeyName) || Objects.isNull(limitKeyName)){
            throw new CoreException("FROM_TO is selected as the paging mode for table ["+tapPage.tableName()+"], but the table level paging start value field of \"TAP_PAGE_OFFSET\" is not used in the paging parameters or the paging end field is not marked with \"TAP_PAGE_LIMIT\" ");
        }
        String pageResultPath = requestApi.pageResultPath();
        if (Objects.isNull(pageResultPath)){
            throw new CoreException("The table data source field is not specified in the interface return result.");
        }
        //获取首次请求结果
        //获取首次请求结果
        if(Objects.isNull(param)){
            param = new HashMap<>();
        }
        param.put(offsetKeyName,offsetValue);
        param.put(limitKeyName,limitValue);
        APIResponse apiResponse = invoker.invoke(apiName, param, apiMethod,true);
        Map<String, Object> result = apiResponse.result();
        while (Objects.nonNull(result) && this.accept(result,tapPage,pageResultPath)){
            param.put(offsetKeyName , offsetValue = (offsetValue + limitValue));
            apiResponse = invoker.invoke(apiName, param, apiMethod,true);
            result = apiResponse.result();
        }
    }
}
