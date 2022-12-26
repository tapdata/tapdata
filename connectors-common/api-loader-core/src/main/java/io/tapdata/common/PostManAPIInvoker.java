package io.tapdata.common;

import io.tapdata.common.postman.PostManAnalysis;
import io.tapdata.common.postman.PostManApiContext;
import io.tapdata.common.postman.entity.ApiEvent;
import io.tapdata.common.postman.entity.ApiInfo;
import io.tapdata.common.postman.entity.ApiMap;
import io.tapdata.common.postman.entity.ApiVariable;
import io.tapdata.common.postman.enums.PostParam;
import io.tapdata.common.postman.pageStage.PageStage;
import io.tapdata.common.postman.pageStage.TapPage;
import io.tapdata.common.postman.util.ApiMapUtil;
import io.tapdata.common.postman.util.FileUtil;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.javascript.APIInvoker;
import io.tapdata.pdk.apis.javascript.APIIterateInterceptor;
import io.tapdata.pdk.apis.javascript.APIResponseInterceptor;
import io.tapdata.pdk.apis.javascript.comom.APIDocument;
import io.tapdata.pdk.apis.javascript.comom.APIIterateError;
import io.tapdata.pdk.apis.javascript.core.annotation.ApiType;
import io.tapdata.pdk.apis.javascript.core.emun.TapApiTag;
import io.tapdata.pdk.apis.javascript.entitys.APIEntity;
import io.tapdata.pdk.apis.javascript.entitys.APIResponse;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static io.tapdata.base.ConnectorBase.fromJson;

@ApiType
public class PostManAPIInvoker
        implements APIDocument<APIInvoker>, APIInvoker{
    private static final String TAG = PostManAPIInvoker.class.getSimpleName();

    private PostManAnalysis analysis ;
    public PostManAPIInvoker analysis(PostManAnalysis analysis){
        this.analysis = analysis;
        return this;
    }

    APIResponseInterceptor interceptor;
    @Override
    public void setAPIResponseInterceptor(APIResponseInterceptor interceptor) {
        this.interceptor = interceptor;
    }

    public static PostManAPIInvoker create(){
        return new PostManAPIInvoker().analysis(PostManAnalysis.create());
    }

    @Override
    public APIInvoker analysis(String apiJson, Map<String, Object> params) {
        Map<String, Object> json = null;
        if (Objects.nonNull(apiJson)) {
            json = (Map<String, Object>) fromJson(apiJson);
        }else {
            if (!FileUtil.fileExists(this.analysis.sourcePath())){
                TapLogger.error(TAG,"Can't analysis api JSON document, please save this JSON file into " + this.analysis.sourcePath() + ". ");
                return this;
            }
            try {
                json = (Map<String, Object>)fromJson(FileUtil.readString(this.analysis.sourcePath()));
            }catch (Throwable ignored){
                TapLogger.error(TAG,"Can't analysis api JSON document, please save this JSON file into " + this.analysis.sourcePath() + ". ");
                return this;
            }
        }

        List<Object> item = this.analysis.getList(PostParam.ITEM, json);
        Map<String, List<Object>> listMap = this.analysis.item(item);
        ApiMap apiMap = ApiMap.create();
        listMap.forEach((key,apiItem)->{
            apiItem.stream().filter(api->{
                if (null == api) return false;
                if (!this.analysis.filterUselessApi()) return true;
                try {
                    Map<String,Object> apiDetail = (Map<String,Object>)api;
                    String name = (String)apiDetail.get(PostParam.NAME);
                    return TapApiTag.isLabeled(name);
                }catch (Exception e){
                    return false;
                }
            }).forEach(api->{
                try {
                    Map<String,Object> apiDetail = (Map<String,Object>)api;
                    String name = (String)apiDetail.get(PostParam.NAME);
                    Map<String,Object> request = (Map<String,Object>) apiDetail.get(PostParam.REQUEST);
                    Object urlObj = request.get(PostParam.URL);
                    String url = null;
                    if (urlObj instanceof String){
                        url = (String) urlObj;
                    }else if(urlObj instanceof Map){
                        url = (String) ((Map<String,Object>)urlObj).get(PostParam.RAW);
                    }
                    String method = (String)request.get(PostParam.METHOD);
                    apiMap.add(ApiMap.ApiEntity.create(url,name,ApiMap.ApiEntity.generateApiEntity(apiDetail)).method(method));
                }catch (Exception ignored){ }
            });
        });

        if (Objects.isNull(this.analysis.apiContext())){
            this.analysis.apiContext(PostManApiContext.create()) ;
        }
        this.analysis.apiContext().info(ApiInfo.create(this.analysis.getMap(PostParam.INFO,json)))
                .event(ApiEvent.create(this.analysis.getList(PostParam.EVENT,json)))
                .variable(ApiVariable.create(this.analysis.getList(PostParam.VARIABLE,json)))
                .apis(apiMap)
                .table(
                    apiMap.stream()
                    .filter(entity -> Objects.nonNull(entity) && Objects.nonNull(entity.name()) && TapApiTag.isTableName(entity.name()))
                    .map(entity -> TapApiTag.analysisTableName(entity.name()) ).collect(Collectors.toList())
                );
        if ( Objects.nonNull(params) && !params.isEmpty() ) {
            this.analysis.apiContext().variableAdd(params);
        }
        return this;
    }

    @Override
    public APIResponse invoke(String uriOrName, String method, Map<String, Object> params, boolean invoker) {
        if (Objects.isNull(params)){
            params = new HashMap<>();
        }
        APIResponse response = this.analysis.http(uriOrName, method, params);
        if (invoker) {
            response = this.interceptor.intercept(response, uriOrName, method, params);
        }
        return response;
    }

    @Override
    public List<APIEntity> tableApis() {
        List<ApiMap.ApiEntity> apiEntities = ApiMapUtil.tableApis(this.analysis.apiContext().apis());
        if (apiEntities.isEmpty()) return new ArrayList<>();
        return apiEntities.stream().filter(Objects::nonNull).map(entity-> APIEntity.create(entity.name(),entity.url(),entity.method())).collect(Collectors.toList());
    }

    @Override
    public List<String> tables() {
        return this.analysis.apiContext().tapTable();
    }

    @Override
    public Map<String, Object> variable() {
        return this.analysis.apiContext().variable();
    }

    @Override
    public void pageStage(TapConnectorContext connectorContext,
                               TapTable table,
                               Object offset,
                               int batchCount,
                               AtomicBoolean task,
                               BiConsumer<List<TapEvent>, Object> consumer) {
        List<ApiMap.ApiEntity> tables = ApiMapUtil.tableApis(this.analysis.apiContext().apis());
        if (tables.isEmpty()) {
            throw new CoreException("Please use TAP_TABLE on the API document format label specifies at least one table data add in for the data source.");
        }
        if (Objects.isNull(table)){
            throw new CoreException("Table must not be null or empty.");
        }
        Map<String,ApiMap.ApiEntity> apiGroupByTableName = tables.stream()
                .filter(api-> Objects.nonNull(api) && TapApiTag.isTableName(api.name())).collect(Collectors.toMap(api-> {
                    String name = api.name();
                    return TapApiTag.analysisTableName(name);
                },api->api,(a1,a2)->a1));
        String currentTable = table.getId();
        ApiMap.ApiEntity api = apiGroupByTableName.get(currentTable);
        if (Objects.isNull(api)){
            throw new CoreException("Can not get table api by table id "+currentTable+".");
        }
        if (Objects.isNull(offset)){
            offset = new Object();
        }
        PageStage stage = PageStage.stage(api.api().pageStage());
        if (Objects.isNull(stage)){
            throw new CoreException(String.format(" The paging type [%s] is unrecognized or temporarily not supported. ",api.api().pageStage()));
        }
        TapPage tapPage = TapPage.create()
                .api(api)
                .offset(offset)
                .batchCount(batchCount)
                .invoker(this)
                .tableName(currentTable)
                .task(task)
                .consumer(consumer);
        stage.page(tapPage);
    }

    @Override
    public void iterateAllData(String urlOrName, String method, Object offset, APIIterateInterceptor interceptor) {
        if (Objects.isNull(urlOrName)){
            throw new CoreException(" Please specify the corresponding paging API name or URL .");
        }
        Map<String,Object> param = new HashMap<>();
        if (offset instanceof Map){
            param.putAll((Map<String, Object>) offset);
        }
        APIResponse invoke = this.invoke(urlOrName, method, param, true);
        Map<String, Object> result = invoke.result();
        interceptor.iterate(result,offset, APIIterateError.error());
    }
}
