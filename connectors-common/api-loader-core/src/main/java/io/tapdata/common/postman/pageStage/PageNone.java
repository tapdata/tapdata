package io.tapdata.common.postman.pageStage;

import io.tapdata.common.support.APIInvoker;
import io.tapdata.common.support.entitys.APIResponse;
import io.tapdata.common.postman.entity.ApiMap;
import io.tapdata.common.postman.entity.params.Api;
import io.tapdata.common.postman.util.ApiMapUtil;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.simplify.TapSimplify;

import java.util.*;
import java.util.function.BiConsumer;

import static io.tapdata.entity.simplify.TapSimplify.toJson;

public class PageNone implements PageStage {
    private static final String TAG = PageNone.class.getSimpleName();
    @Override
    public void page(TapPage tapPage) {
        ApiMap.ApiEntity api = tapPage.api();
        Api requestApi = api.api();
        APIInvoker invoker = tapPage.invoker();
        BiConsumer<List<TapEvent>, Object> consumer = tapPage.consumer();

        String apiName = api.name();
        String apiMethod = api.method();
        Map<String, Object> param = tapPage.apiParam();
        APIResponse apiResponse = invoker.invoke(apiName, param, apiMethod,true);
        int size = tapPage.batchCount();

        String pageResultPath = requestApi.pageResultPath();
        String pageResultPathTemp = this.pageResultPath(pageResultPath);
        Map<String, Object> apiResult = apiResponse.result();
        Object pageResult = ApiMapUtil.getKeyFromMap(apiResult, pageResultPathTemp);
        if (Objects.isNull(pageResult)){
            throw new CoreException(String.format("The value of the [%s] parameter was not found in the request result, the interface call failed, or check whether the parameter key is correct.", pageResultPath));
        }
        List<TapEvent> tapEvents = new ArrayList<>();
        if (pageResult instanceof Collection){
            Collection<Object> entity = (Collection<Object>)pageResult;
            for (Object ent : entity) {
                if(!tapPage.isAlive()) return;
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
        }else if(pageResult instanceof Map){
            Map<String,Object> entity = (Map<String,Object>)pageResult;
            tapEvents.add(TapSimplify.insertRecordEvent(entity,tapPage.tableName()).referenceTime(System.currentTimeMillis()));
            consumer.accept(tapEvents, tapPage.offset());
        }else {
            TapLogger.info(TAG, "pageResultPath :\n"+toJson(pageResult));
            throw new CoreException(String.format("The data obtained from %s is not recognized as table data.", pageResultPath));
        }
    }
}
