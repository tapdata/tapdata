package io.tapdata.quickapi.support.postman.pageStage;

import cn.hutool.json.JSONUtil;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.pdk.apis.api.APIResponse;
import io.tapdata.quickapi.core.emun.TapApiTag;
import io.tapdata.quickapi.support.postman.PostManAnalysis;
import io.tapdata.quickapi.support.postman.entity.ApiMap;
import io.tapdata.quickapi.support.postman.entity.params.Api;
import io.tapdata.quickapi.support.postman.enums.PostParam;
import io.tapdata.quickapi.support.postman.util.ApiMapUtil;

import java.util.*;
import java.util.function.BiConsumer;

public class PageToken implements PageStage{
    private static final String TAG = PageToken.class.getSimpleName();
    @Override
    public void page(TapPage tapPage) {
        ApiMap.ApiEntity api = tapPage.api();
        Api requestApi = api.api();
        PostManAnalysis invoker = tapPage.invoker();
        BiConsumer<List<TapEvent>, Object> consumer = tapPage.consumer();

        String apiName = api.name();
        String apiMethod = api.method();
        Map<String, Object> param = tapPage.apiParam();

        //@TODO verify param not null
        List<Map<String, Object>> query = api.api().request().url().query();
        String fromKeyName = null;
        String toKeyName = null;
        int fromValue = 0;
        int toValue = 0;

        for (Map<String, Object> queryMap : query) {
            Object queryParamDescription = queryMap.get(PostParam.DESCRIPTION);
            Object keyObj = queryMap.get(PostParam.KEY);
            Object valueObj = queryMap.get(PostParam.VALUE);
            String objKeyName = Objects.isNull(keyObj)?null:String.valueOf(keyObj);
            if (Objects.equals(queryParamDescription, TapApiTag.TAP_PAGE_FROM.tagName())) {
                fromKeyName = objKeyName;
                fromValue = Integer.parseInt(Objects.isNull(valueObj)?"1":String.valueOf(valueObj));
            }
            if (Objects.equals(queryParamDescription, TapApiTag.TAP_PAGE_TO.tagName())) {
                toKeyName = objKeyName;
                toValue = Integer.parseInt(Objects.isNull(valueObj)?"20":String.valueOf(valueObj));
            }
        }
        final int pageSize = toValue - fromValue;
        if (Objects.isNull(fromKeyName) || Objects.isNull(toKeyName)){
            throw new CoreException("FROM_TO is selected as the paging mode for table ["+tapPage.tableName()+"], but the table level paging start value field of \"TAP_PAGE_FROM\" is not used in the paging parameters or the paging end field is not marked with \"TAP_PAGE_TO\" ");
        }
        String pageResultPath = requestApi.pageResultPath();
        if (Objects.isNull(pageResultPath)){
            throw new CoreException("The table data source field is not specified in the interface return result.");
        }
        //获取首次请求结果
        if(Objects.isNull(param)){
            param = new HashMap<>();
        }
        param.put(fromKeyName,toValue+1);
        param.put(toKeyName,toValue+pageSize);
        APIResponse apiResponse = invoker.invoke(apiName, apiMethod, param);
        Map<String, Object> result = apiResponse.result();
        while (this.accept(result,tapPage,pageResultPath)){
            param.put(fromKeyName,toValue+1);
            param.put(toKeyName,toValue+pageSize);
            apiResponse = invoker.invoke(apiName, apiMethod, param);
            result = apiResponse.result();
        }
    }
    private boolean accept(Map<String, Object> result,TapPage tapPage,String pageResultPath){
        Object pageResult = ApiMapUtil.getKeyFromMap(result, pageResultPath);
        if (Objects.isNull(pageResult)){
            throw new CoreException(String.format("The value of the [%s] parameter was not found in the request result, the interface call failed, or check whether the parameter key is correct.",pageResultPath));
        }
        List<TapEvent> tapEvents = new ArrayList<>();
        BiConsumer<List<TapEvent>, Object> consumer = tapPage.consumer();
        int size = tapPage.batchCount();
        if (pageResult instanceof Collection){
            Collection entity = (Collection)pageResult;
            for (Object ent : entity) {
                if(!tapPage.isAlive()) return tapPage.isAlive();
                if (Objects.isNull(ent)) continue;
                try {
                    Map<String,Object> after = (Map<String, Object>) ent;
                    tapEvents.add(TapSimplify.insertRecordEvent(after,tapPage.tableName()).referenceTime(System.currentTimeMillis()));
                }catch (Exception e){
                    continue;
                }
                if (tapEvents.size() == size){
                    consumer.accept(tapEvents, tapPage.offset());
                    tapEvents = new ArrayList<>();
                }
            }
            if (!tapEvents.isEmpty()){
                consumer.accept(tapEvents, tapPage.offset());
            }
            return !entity.isEmpty();
        }else if(pageResult instanceof Map){
            Map<String,Object> entity = (Map<String,Object>)pageResult;
            tapEvents.add(TapSimplify.insertRecordEvent(entity,tapPage.tableName()).referenceTime(System.currentTimeMillis()));
            consumer.accept(tapEvents, tapPage.offset());
            return true;
        }else {
            TapLogger.info(TAG, "pageResultPath :\n"+ JSONUtil.toJsonStr(pageResult));
            throw new CoreException(String.format("The data obtained from %s is not recognized as table data.",pageResultPath));
        }
    }
}
