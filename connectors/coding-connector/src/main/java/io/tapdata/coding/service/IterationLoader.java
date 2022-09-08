package io.tapdata.coding.service;

import cn.hutool.json.JSONUtil;
import io.tapdata.coding.utils.http.CodingHttp;
import io.tapdata.coding.utils.http.HttpEntity;
import io.tapdata.coding.utils.tool.Checker;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class IterationLoader extends CodingStarter {
    Map<String,Object> queryMap ;
    public static IterationLoader create(TapConnectionContext tapConnectionContext,Map<String,Object> queryMap){
        return new IterationLoader(tapConnectionContext, queryMap);
    }
    IterationLoader(TapConnectionContext tapConnectionContext,Map<String,Object> queryMap) {
        super(tapConnectionContext);
        this.queryMap = queryMap;
    }


    public List<Object> loadIteration(List<String> filterTable, Consumer<List<TapTable>> consumer, int tableSize){
        if (null == consumer) {
            throw new IllegalArgumentException("Consumer cannot be null");
        }
        //queryAllIteration();
        return null;
    }
    public HttpEntity<String,Object> actionSetter(String action,HttpEntity<String,Object> requestBody){
//        if (Checker.isEmpty(action)){
//            throw new CoreException("Action must be not null or not empty.");
//        }
//        if (Checker.isEmpty(requestBody)) requestBody = HttpEntity.create();
//        Object iterationKeyWordsObj = this.queryMap.get("key");
//        switch (action){
//            case "list":{
//                requestBody.builder("",action);
//                break;
//            }
//            case "search":{
//                if (Checker.isNotEmpty(iterationKeyWordsObj)){
//                    requestBody.builder("keywords",String.valueOf(iterationKeyWordsObj));
//                }
//                break;
//            }
//            default:throw new CoreException("Action only support [list] or [search] now.");
//        }
//        return requestBody;
        return null;
    }
    public HttpEntity<String,Object> commandSetter(String command,HttpEntity<String,Object> requestBody){
        if (Checker.isEmpty(command)){
            throw new CoreException("Command must be not null or not empty.");
        }
        if (Checker.isEmpty(requestBody)) requestBody = HttpEntity.create();
        DataMap connectionConfig = this.tapConnectionContext.getConnectionConfig();
        if (Checker.isEmpty(this.queryMap)){
            throw new CoreException("QueryMap is must be null or empty");
        }
        Object pageObj = this.queryMap.get("page");
        if (Checker.isEmpty(pageObj)){
            throw new CoreException("Page is must be null or empty");
        }
        Object sizeObj = this.queryMap.get("size");
        if (Checker.isEmpty(sizeObj)){
            throw new CoreException("Size is must be null or empty");
        }
        Integer page = Integer.parseInt(pageObj.toString());//第几页，从一开始
        Integer size = Integer.parseInt(sizeObj.toString());
        Object keyWordsObj = this.queryMap.get("key");
        switch (command){
            case "DescribeIterationList":{
                requestBody.builder("Limit",size)
                        .builder("Offset",page-1)
                        .builder("Action","DescribeIterationList");
                if (Checker.isNotEmpty(keyWordsObj)){
                    requestBody.builder("keywords",String.valueOf(keyWordsObj).trim());
                }
                break;
            }
            case "DescribeCodingProjects":{
                requestBody.builder("PageSize",size)
                        .builder("PageNumber",page)
                        .builder("Action","DescribeCodingProjects");
                if (Checker.isNotEmpty(keyWordsObj)){
                    requestBody.builder("ProjectName",String.valueOf(keyWordsObj).trim());
                }
                break;
            }
            default:throw new CoreException("Command only support [DescribeIterationList] or [DescribeCodingProjects] now.");
        }
        return requestBody;
    }

    private List<Map> queryAllIteration(int tableSize) throws Exception {
        DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
        String projectName = connectionConfig.getString("projectName");
        String token = connectionConfig.getString("token");
        String teamName = connectionConfig.getString("teamName");

        int currentQueryCount = 0,queryIndex = 0;

        List<Map> matterList = new ArrayList<>();
        do{
                HttpEntity<String,String> header = HttpEntity.create()
                    .builder("Authorization",token);
                HttpEntity<String,Object> body = HttpEntity.create()
                    .builder("Action","DescribeIterationList")
                    .builder("ProjectName",  projectName)
                    .builder("Limit", tableSize)
                    .builder("Offset",++queryIndex);
            Map<String,Object> resultMap = CodingHttp.create(
                    header.getEntity(),
                    body.getEntity(),
                    String.format(OPEN_API_URL,teamName)
            ).post();
            Object response = resultMap.get("Response");
            Map<String,Object> responseMap = null !=  response? JSONUtil.parseObj(response) : null;
            if (null == response){
                if (queryIndex > 1) {
                    queryIndex -= 1;
                    break;
                }else {
                    throw new Exception("discover error");
                }
            }

            currentQueryCount = Integer.parseInt(String.valueOf(resultMap.get("PageSize")));
            Map<String,Object> dataMap = null != responseMap.get("data") ? JSONUtil.parseObj(responseMap.get("data")) : null;
            if (null == dataMap || null == dataMap.get("List")){
                break;
            }
            List<Map> list = JSONUtil.toList(JSONUtil.parseArray(dataMap.get("List")),Map.class);


            matterList.addAll(list);
            //if (null == responseMap || null == responseMap.get("Project")){
            //    throw new Exception("Incorrect project name entered!");
            //}
        }while (currentQueryCount<tableSize);

        return matterList;
    }

}
