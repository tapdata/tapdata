package io.tapdata.coding.service;

import cn.hutool.core.text.csv.CsvUtil;
import cn.hutool.core.text.csv.CsvWriter;
import cn.hutool.core.util.CharsetUtil;
import cn.hutool.http.HttpRequest;
import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import io.tapdata.coding.entity.CodingOffset;
import io.tapdata.coding.entity.ContextConfig;
import io.tapdata.coding.enums.Constants;
import io.tapdata.coding.enums.IssueType;
import io.tapdata.coding.utils.collection.MapUtil;
import io.tapdata.coding.utils.http.CodingHttp;
import io.tapdata.coding.utils.http.HttpEntity;
import io.tapdata.coding.utils.tool.Checker;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.Entry;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;

import javax.jws.Oneway;
import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static io.tapdata.entity.simplify.TapSimplify.list;
import static io.tapdata.entity.simplify.TapSimplify.map;


/**
 * @author GavinX
 * @Description
 * @create 2022-08-26 11:49
 **/
public class IssueLoader extends CodingStarter {
    private static final String TAG = IssueLoader.class.getSimpleName();

    public static IssueLoader create(TapConnectionContext tapConnectionContext) {
        return new IssueLoader(tapConnectionContext);
    }

    int tableSize;

    public IssueLoader(TapConnectionContext tapConnectionContext) {
        super(tapConnectionContext);
    }

    public IssueLoader setTableSize(int tableSize) {
        this.tableSize = tableSize;
        return this;
    }


    public void discoverMatterOldVersion(List<String> filterTable, Consumer<List<TapTable>> consumer) {
        if (null == consumer) {
            throw new IllegalArgumentException("Consumer cannot be null");
        }

    }

    /**
     * 校验connectionConfig配置字段
     */
    public void verifyConnectionConfig(){
        if(this.isVerify){
            return;
        }
        if (null == this.tapConnectionContext){
            throw new IllegalArgumentException("TapConnectorContext cannot be null");
        }
        DataMap connectionConfig = this.tapConnectionContext.getConnectionConfig();
        if (null == connectionConfig ){
            throw new IllegalArgumentException("TapTable' DataMap cannot be null");
        }
        String projectName = connectionConfig.getString("projectName");
        String token = connectionConfig.getString("token");
        String teamName = connectionConfig.getString("teamName");
        String streamReadType = connectionConfig.getString("streamReadType");
        String connectionMode = connectionConfig.getString("connectionMode");
        if ( null == projectName || "".equals(projectName)){
            TapLogger.info(TAG, "Connection parameter exception: {} ", projectName);
        }
        if ( null == token || "".equals(token) ){
            TapLogger.info(TAG, "Connection parameter exception: {} ", token);
        }
        if ( null == teamName || "".equals(teamName) ){
            TapLogger.info(TAG, "Connection parameter exception: {} ", teamName);
        }
        if ( null == streamReadType || "".equals(streamReadType) ){
            TapLogger.info(TAG, "Connection parameter streamReadType exception: {} ", token);
        }
        if ( null == connectionMode || "".equals(connectionMode) ){
            TapLogger.info(TAG, "Connection parameter connectionMode exception: {} ", teamName);
        }
        this.isVerify = Boolean.TRUE;
    }
    /**一次获取事项分页查询并返回Map结果
     * @auth GavinX
     * @param url
     * @return
     */
    public Map<String,Object> getIssuePage(
            Map<String,String> header,
            Map<String,Object> body,
            String url){
        Map<String,Object> resultMap = CodingHttp.create(header, body, url).post();
        Object response = resultMap.get("Response");
        Map<String,Object> responseMap = (Map<String, Object>) response;
        if (null == response ){
            TapLogger.info(TAG, "HTTP request exception, Issue list acquisition failed: {} ", CodingStarter.OPEN_API_URL+"?Action=DescribeIssueListWithPage");
            throw new RuntimeException("HTTP request exception, Issue list acquisition failed: " + CodingStarter.OPEN_API_URL+"?Action=DescribeIssueListWithPage");
        }
        Object data = responseMap.get("Data");
        return null != data ? (Map<String,Object>)data: null;
    }

    /**
     * @auth GavinX
     * @param projectName
     * @param teamName
     * @param issueDetail
     */
    public void composeIssue(String projectName, String teamName, Map<String, Object> issueDetail) {
        this.addParamToBatch(issueDetail);//给自定义字段赋值
        issueDetail.put("ProjectName",projectName);
        issueDetail.put("TeamName",   teamName);
    }

    /**
     * @auth GavinX
     * 向事项详细信息返回结果中添加部分指定字段值
     * @param batchMap
     */
    public void addParamToBatch(Map<String,Object> batchMap){
        this.putObject(batchMap,"IssueTypeDetail","Id",  "IssueTypeDetailId");
        this.putObject(batchMap,"Assignee",       "Id",  "AssigneeId");
        this.putObject(batchMap,"Creator",        "Id",  "CreatorId");
        this.putObject(batchMap,"ProjectModule",  "Id",  "ProjectModuleId");
        this.putObject(batchMap,"Parent",         "Code","ParentCode");
        this.putObject(batchMap,"Epic",           "Code","EpicCode");
        this.putObject(batchMap,"Iteration",      "Code","IterationCode");
        this.putObject(batchMap,"Watchers",       "Id",  "WatcherIdArr");
        this.putObject(batchMap,"Labels",         "Id",  "LabelIdArr");
        this.putObject(batchMap,"Files",          "Id",  "FileIdArr");
        this.putObject(batchMap,"SubTasks",       "Code","SubTaskCodeArr");
    }

    /**
     * @auth GavinX
     * @param batchMap
     * @param fromObj
     * @param fromKey
     * @param targetKey
     */
    private void putObject(Map<String,Object> batchMap,String fromObj,String fromKey,String targetKey){
        Object obj = batchMap.get(fromObj);
        if (null != obj && obj instanceof Map){
            Map<String,Object> fromObjMap = (Map<String,Object>)obj;
            batchMap.put(targetKey,fromObjMap.get(fromKey));
        }
        if (null != obj && obj instanceof List){
            List<Object> fromObjList = (List)obj;
            if ( null != fromObjList && fromObjList.size()>0){
                List<Object> keyArr = new ArrayList<>();
                fromObjList.forEach(o->{
                    Object key = ((Map<String,Object>)o).get(fromKey);
                    if (null!= key) keyArr.add(key);
                });
                batchMap.put(targetKey,keyArr);
            }
            batchMap.put(targetKey,new ArrayList<Integer>());
        }
    }

    public String longToDateStr(Long date){
        if (null == date) return "1000-01-01";
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        String dateStr = formatter.format(new Date(date));
        return dateStr.length()>10?"9999-12-31":dateStr;
    }

    public Long dateStrToLong(String date){
        return null;
    }

    public Map<String,Object> readIssueDetail(
            HttpEntity<String,Object> issueDetailBody,
            CodingHttp authorization,
            HttpRequest requestDetail,
            Integer code,
            String projectName,
            String teamName
    ){
        //查询事项详情
        issueDetailBody.builder("IssueCode", code);
        Map<String,Object> issueDetailResponse = authorization.body(issueDetailBody.getEntity()).post(requestDetail);
        if (null == issueDetailResponse){
            TapLogger.info(TAG, "HTTP request exception, Issue Detail acquisition failed: {} ", CodingStarter.OPEN_API_URL+"?Action=DescribeIssue&IssueCode="+code);
            throw new RuntimeException("HTTP request exception, Issue Detail acquisition failed: "+CodingStarter.OPEN_API_URL+"?Action=DescribeIssue&IssueCode="+code);
        }
        issueDetailResponse = (Map<String,Object>)issueDetailResponse.get("Response");
        if (null == issueDetailResponse){
            TapLogger.info(TAG, "HTTP request exception, Issue Detail acquisition failed: {} ", CodingStarter.OPEN_API_URL+"?Action=DescribeIssue&IssueCode="+code);
            throw new RuntimeException("HTTP request exception, Issue Detail acquisition failed: "+CodingStarter.OPEN_API_URL+"?Action=DescribeIssue&IssueCode="+code);
        }
        Map<String,Object> issueDetail = (Map<String,Object>)issueDetailResponse.get("Issue");
        if (null == issueDetail){
            TapLogger.info(TAG, "Issue Detail acquisition failed: IssueCode {} ", code);
            return null;
            //throw new RuntimeException("Issue Detail acquisition failed: IssueCode "+code);
        }
        this.composeIssue(projectName, teamName, issueDetail);
        return issueDetail;
    }

    Map<String,Map<Integer,String>> allCustomFieldMap;
    private Map<Integer,String> getIssueCustomFieldMap(String issueType,ContextConfig contextConfig){
        if(null == allCustomFieldMap){
            allCustomFieldMap = new HashMap<>();
        }
        if (null!=allCustomFieldMap.get(issueType)){
            return allCustomFieldMap.get(issueType);
        }
        Map<Integer,String> customFieldMap = new HashMap<>();

        HttpEntity<String,String> heard = HttpEntity.create().builder("Authorization",contextConfig.getToken());
        HttpEntity<String,Object> body = HttpEntity.create()
                .builder("Action","DescribeProjectIssueFieldList")
                .builder("ProjectName",contextConfig.getProjectName())
                .builder("IssueType",issueType);
        Map<String, Object> post = CodingHttp.create(heard.getEntity(), body.getEntity(), String.format(CodingStarter.OPEN_API_URL)).post();
        Object response = post.get("Response");
        Map<String,Object> responseMap = (Map<String, Object>) response;
        if (null == response ){
            TapLogger.warn(TAG, "HTTP request exception, Issue CustomField acquisition failed: {} ", CodingStarter.OPEN_API_URL+"?Action=DescribeProjectIssueFieldList");
            throw new CoreException("HTTP request exception, Issue CustomField acquisition failed: " + CodingStarter.OPEN_API_URL+"?Action=DescribeProjectIssueFieldList");
        }
        Object data = responseMap.get("ProjectIssueFieldList");
        if (null != data && data instanceof JSONArray){
            List<Map<String,Object>> list = (ArrayList)data;
            list.forEach(field->{
                Object fieldObj = field.get("IssueField");
                if (null != fieldObj){
                    Map<String, Object> fieldDetial = (Map<String,Object>)fieldObj;
                    Object fieldIdObj = fieldDetial.get("Id");
                    Object fieldNameObj = fieldDetial.get("Name");
                    customFieldMap.put(Integer.parseInt(String.valueOf(fieldIdObj)),String.valueOf(fieldNameObj));
                }
            });
        }
        allCustomFieldMap.put(issueType,customFieldMap);
        return customFieldMap;
    }
    private String getCustomFieldName(String issueType,Integer customId,ContextConfig contextConfig){
        Map<Integer,String> customFields = this.getIssueCustomFieldMap(issueType,contextConfig);
        if (null == customFields || customFields.size()<1){
            return null;
        }
        return customFields.get(customId);
    }


    public ContextConfig veryContextConfigAndNodeConfig(){
        this.verifyConnectionConfig();
        DataMap connectionConfigConfigMap = this.tapConnectionContext.getConnectionConfig();
        String projectName = connectionConfigConfigMap.getString("projectName");
        String token = connectionConfigConfigMap.getString("token");
        String teamName = connectionConfigConfigMap.getString("teamName");
        String streamReadType = connectionConfigConfigMap.getString("streamReadType");
        String connectionMode = connectionConfigConfigMap.getString("connectionMode");
        ContextConfig config = ContextConfig.create().projectName(projectName)
                .teamName(teamName)
                .token(token)
                .streamReadType(streamReadType)
                .connectionMode(connectionMode);
        if (this.tapConnectionContext instanceof TapConnectorContext) {
            DataMap nodeConfigMap = ((TapConnectorContext)this.tapConnectionContext).getNodeConfig();
            if (null == nodeConfigMap) {
                config.issueType(IssueType.ALL);
                config.iterationCodes("-1");
                TapLogger.debug(TAG,"TapTable' NodeConfig is empty. ");
                //throw new IllegalArgumentException("TapTable' NodeConfig cannot be null");
            }else{
                //iterationName is Multiple selection values separated by commas
                String iterationCodeArr = nodeConfigMap.getString("DescribeIterationList");//iterationCodes
                if (null != iterationCodeArr) iterationCodeArr = iterationCodeArr.trim();
                String issueType = nodeConfigMap.getString("issueType");
                if (null != issueType) issueType = issueType.trim();

                if (null == iterationCodeArr || "".equals(iterationCodeArr)) {
                    TapLogger.info(TAG, "Connection node config iterationName exception: {} ", projectName);
                }
                if (null == issueType || "".equals(issueType)) {
                    TapLogger.info(TAG, "Connection node config issueType exception: {} ", token);
                }
                config.issueType(issueType).iterationCodes(iterationCodeArr);
            }
        }
        return config;
    }

    public List<Map<String,Object>> getAllIssueType(){
        ContextConfig contextConfig = this.veryContextConfigAndNodeConfig();
        HttpEntity<String,String> header = HttpEntity.create().builder("Authorization",contextConfig.getToken());
        String projectName = contextConfig.getProjectName();
        HttpEntity<String,Object> pageBody = HttpEntity.create().builder("Action","DescribeTeamIssueTypeList");
        Map<String, Object> issueResponse = CodingHttp.create(
                header.getEntity(),
                pageBody.getEntity(),
                String.format(CodingStarter.OPEN_API_URL, contextConfig.getTeamName())
        ).post();
        if (Checker.isEmpty(issueResponse)){
            return null;
        }
        Object response = issueResponse.get("Response");
        if (null == response){
            return null;
        }
        Object issueTypes = ((Map<String,Object>)response).get("IssueTypes");
        if (Checker.isEmpty(issueTypes)){
            return null;
        }
        return (List<Map<String,Object>>)issueTypes;
    }
}
