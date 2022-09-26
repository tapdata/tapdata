package io.tapdata.coding.service;

import io.tapdata.coding.utils.http.CodingHttp;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Consumer;


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
    public void verifyConnectionConfig(TapConnectionContext connectionContext){
        if (null == connectionContext){
            throw new IllegalArgumentException("TapConnectorContext cannot be null");
        }
        DataMap connectionConfig = connectionContext.getConnectionConfig();
        if (null == connectionConfig ){
            throw new IllegalArgumentException("TapTable' DataMap cannot be null");
        }
        String projectName = connectionConfig.getString("projectName");
        String token = connectionConfig.getString("token");
        String teamName = connectionConfig.getString("teamName");
        if ( null == projectName || "".equals(projectName)){
            TapLogger.debug(TAG, "Connection parameter exception: {} ", projectName);
        }
        if ( null == token || "".equals(token) ){
            TapLogger.debug(TAG, "Connection parameter exception: {} ", token);
        }
        if ( null == teamName || "".equals(teamName) ){
            TapLogger.debug(TAG, "Connection parameter exception: {} ", teamName);
        }
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
            TapLogger.debug(TAG, "HTTP request exception, Issue list acquisition failed: {} ", url+"?Action=DescribeIssueListWithPage");
            throw new RuntimeException("HTTP request exception, Issue list acquisition failed: " + url+"?Action=DescribeIssueListWithPage");
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
}
