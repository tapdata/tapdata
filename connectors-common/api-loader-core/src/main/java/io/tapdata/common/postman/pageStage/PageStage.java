package io.tapdata.common.postman.pageStage;

import io.tapdata.common.postman.entity.params.Api;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.common.postman.util.ApiMapUtil;

import java.util.*;
import java.util.function.BiConsumer;

import static io.tapdata.entity.simplify.TapSimplify.toJson;

public interface PageStage {
    static final String TAG = PageStage.class.getSimpleName();
    public static String stagePackageName(String pageTag){
        char[] chars = pageTag.toCharArray();
        StringBuilder builder = new StringBuilder(PageStage.class.getPackage().getName());
        builder.append(".").append((""+((char)chars[0])).toUpperCase(Locale.ROOT));
        for (int i = 1; i < chars.length; i++) {
            char aChar = chars[i];
            if(aChar == '_' && i+1 >= chars.length){
                break;
            }
            builder.append((""+(aChar)).toLowerCase(Locale.ROOT));
            if(i+1 >= chars.length) break;
            if (chars.length <= i+2 && '_' == chars[i+1]){
                break;
            }
            if (chars.length > i+1 && '_' == chars[i+1]){
                builder.append((""+((char)chars[i=i+2])).toUpperCase(Locale.ROOT));
            }
        }
        return builder.toString();
    }
    public static PageStage stage(String pageTag){
        if (Objects.isNull(pageTag)) return null;
        try {
            //noinspection unchecked
            Class<? extends PageStage> pageStage = (Class<? extends PageStage>) Class.forName(PageStage.stagePackageName(pageTag));
            return pageStage.newInstance();
        } catch (Exception ignored){

        }
        return null;
    }
    public void page(TapPage tapPage);

    public default boolean accept(Map<String, Object> result,TapPage tapPage, String pageResultPath){
        String pageResultPathTemp = this.pageResultPath(pageResultPath);
        Object pageResult = ApiMapUtil.getKeyFromMap(result, pageResultPathTemp);
        if (Objects.isNull(pageResult)){
            TapLogger.info(TAG,String.format("Batch read may be over,The value of the [%s] parameter was not found in the request result, the interface call failed, or check whether the parameter key is correct.",pageResultPath));
            return false;
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
            TapLogger.info(TAG, "pageResultPath :\n"+ toJson(pageResult));
            throw new CoreException(String.format("The data obtained from %s is not recognized as table data.",pageResultPath));
        }
    }

    public default String pageResultPath(String pageResultPath){
        return Api.PAGE_RESULT_PATH_DEFAULT_PATH + (
                Objects.isNull(pageResultPath) || "".equals(pageResultPath.trim())? "" : "." + pageResultPath
        );
    }
}
