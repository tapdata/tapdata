package io.tapdata.coding.service.loader;

import cn.hutool.http.HttpRequest;
import cn.hutool.json.JSONArray;
import io.tapdata.coding.entity.CodingOffset;
import io.tapdata.coding.entity.ContextConfig;
import io.tapdata.coding.entity.param.IssueParam;
import io.tapdata.coding.enums.CodingEvent;
import io.tapdata.coding.enums.IssueType;
import io.tapdata.coding.service.connectionMode.CSVMode;
import io.tapdata.coding.service.connectionMode.ConnectionMode;
import io.tapdata.coding.utils.collection.MapUtil;
import io.tapdata.coding.utils.http.CodingHttp;
import io.tapdata.coding.utils.http.HttpEntity;
import io.tapdata.coding.utils.tool.Checker;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static io.tapdata.entity.simplify.TapSimplify.entry;
import static io.tapdata.entity.simplify.TapSimplify.map;
import static io.tapdata.coding.enums.TapEventTypes.*;

/**
 * @author GavinX
 * @Description
 * @create 2022-08-26 11:49
 **/
public class IssuesLoader extends CodingStarter implements CodingLoader<IssueParam>{
    private static final String TAG = IssuesLoader.class.getSimpleName();
    public static final String TABLE_NAME = "Issues";

    public static IssuesLoader create(TapConnectionContext tapConnectionContext) {
        return new IssuesLoader(tapConnectionContext);
    }

    private final long streamExecutionGap = 5000;//util: ms
    private int batchReadPageSize = 500;//coding page 1~500,

    private Long lastTimePoint;
    private List<Integer> lastTimeSplitIssueCode = new ArrayList<>();//hash code list
    int tableSize;
    ContextConfig contextConfig ;
    private AtomicBoolean stopRead = new AtomicBoolean(false);
    public void stopRead(){
        stopRead.set(true);
    }

    public IssuesLoader(TapConnectionContext tapConnectionContext) {
        super(tapConnectionContext);
        this.contextConfig = this.veryContextConfigAndNodeConfig();
    }

    public IssuesLoader setTableSize(int tableSize) {
        this.tableSize = tableSize;
        return this;
    }


    public void discoverMatterOldVersion(List<String> filterTable, Consumer<List<TapTable>> consumer) {
        if (null == consumer) {
            throw new IllegalArgumentException("Consumer cannot be null");
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
        //this.addParamToBatch(issueDetail);//给自定义字段赋值
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
        CodingHttp codingHttp = authorization.body(issueDetailBody.getEntity());
        Map<String,Object> issueDetailResponse = codingHttp.post(requestDetail);
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
            TapLogger.info(TAG, "Issue Detail acquisition failed: IssueCode {} - {}", code, codingHttp.errorMsg(issueDetail));
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

    public List<Map<String,Object>> getAllIssueType(){
        HttpEntity<String,String> header = HttpEntity.create().builder("Authorization",this.contextConfig.getToken());
        String projectName = this.contextConfig.getProjectName();
        HttpEntity<String,Object> pageBody = HttpEntity.create().builder("Action","DescribeTeamIssueTypeList");
        Map<String, Object> issueResponse = CodingHttp.create(
                header.getEntity(),
                pageBody.getEntity(),
                String.format(CodingStarter.OPEN_API_URL, this.contextConfig.getTeamName())
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

    @Override
    public Long streamReadTime() {
        return 1*60*1000l;
    }

    @Override
    public List<Map<String,Object>> list(IssueParam param) {
        Map<String,Object> resultMap = this.codingHttp(param).post();
        Object response = resultMap.get("Response");
        if (null == response){
            return null;
        }
        Map<String,Object> responseMap = (Map<String,Object>)response;
        Object dataObj = responseMap.get("Data");
        if (null == dataObj){
            return null;
        }
        Map<String,Object> data = (Map<String,Object>)dataObj;
        Object listObj = data.get("List");
        return null !=  listObj? (List<Map<String, Object>>) listObj : null;
    }

    @Override
    public List<Map<String, Object>> all(IssueParam param) {
        return null;
    }

    @Override
    public CodingHttp codingHttp(IssueParam param) {
        param.action("DescribeIterationList");
        HttpEntity<String,String> header = HttpEntity.create()
                .builder("Authorization",this.contextConfig.getToken());
        HttpEntity<String,Object> body = HttpEntity.create()
                .builderIfNotAbsent("Action","DescribeIssueListWithPage")
                .builder("ProjectName",this.contextConfig.getProjectName())
                .builder("IssueType",param.issueType().getName())
                .builder("PageNumber",param.offset())
                .builder("PageSize",param.limit())
                .builderIfNotAbsent("Conditions",param.conditions())
                .builderIfNotAbsent("SortKey",param.sortKey())
                .builderIfNotAbsent("SortValue",param.sortValue());
        return CodingHttp.create(
                header.getEntity(),
                body.getEntity(),
                String.format(OPEN_API_URL,this.contextConfig.getTeamName()));
    }

    @Override
    public Map<String,Object> get(IssueParam param) {
        HttpEntity<String,String> header = HttpEntity.create()
                .builder("Authorization",this.contextConfig.getToken());
        HttpEntity<String,Object> body = HttpEntity.create()
                .builderIfNotAbsent("Action","DescribeIssue")
                .builder("ProjectName",this.contextConfig.getProjectName())
                .builder("IssueCode",param.issueCode());
        Map<String,Object> resultMap = CodingHttp.create(
                header.getEntity(),
                body.getEntity(),
                String.format(OPEN_API_URL,this.contextConfig.getTeamName())).post();
        Object response = resultMap.get("Response");
        if (null == response){
            return null;
        }
        Map<String,Object> responseMap = (Map<String,Object>)response;
        Object dataObj = responseMap.get("Issue");
        Map<String, Object> result = null !=  dataObj? (Map<String, Object>) dataObj : null;
        if (Checker.isNotEmpty(result)) {
            this.composeIssue(this.contextConfig.getProjectName(), this.contextConfig.getTeamName(), result);
        }
        return result;
    }

    @Override
    public void batchRead(Object offset, int batchCount, BiConsumer<List<TapEvent>, Object> consumer) {
        Long readEnd = System.currentTimeMillis();
        CodingOffset codingOffset =  new CodingOffset();
        //current read end as next read begin
        codingOffset.setTableUpdateTimeMap(new HashMap<String,Long>(){{ put(TABLE_NAME,readEnd);}});
        this.verifyConnectionConfig();
        this.readV2(null,readEnd,batchCount,codingOffset,consumer);
    }

    @Override
    public int batchCount() throws Throwable {
        int count = 0;
        IssuesLoader issuesLoader = IssuesLoader.create(this.tapConnectionContext);
        issuesLoader.verifyConnectionConfig();
        try {
            DataMap connectionConfig = this.tapConnectionContext.getConnectionConfig();
            String token = connectionConfig.getString("token");
            token = issuesLoader.tokenSetter(token);
            HttpEntity<String,String> header = HttpEntity.create()
                    .builder("Authorization",token);
            HttpEntity<String,Object> body = HttpEntity.create()
                    .builder("Action",       "DescribeIssueListWithPage")
                    .builder("ProjectName",  connectionConfig.getString("projectName"))
                    .builder("PageSize",     1)
                    .builder("PageNumber",   1);
            try {
                DataMap nodeConfigMap = ((TapConnectorContext)this.tapConnectionContext).getNodeConfig();

                String iterationCodes = nodeConfigMap.getString("DescribeIterationList");//iterationCodes
                if (null != iterationCodes) iterationCodes = iterationCodes.trim();
                String issueType = nodeConfigMap.getString("issueType");
                if (null != issueType ) issueType = issueType.trim();

                body.builder("IssueType",    IssueType.verifyType(issueType));

                if (null != iterationCodes && !"".equals(iterationCodes) && !",".equals(iterationCodes) && !"-1".equals(iterationCodes)){
                    //String[] iterationCodeArr = iterationCodes.split(",");
                    //@TODO 输入的迭代编号需要验证，否则，查询事项列表时作为查询条件的迭代不存在时，查询会报错
                    //选择的迭代编号不需要验证
                    body.builder(
                            "Conditions",
                            io.tapdata.entity.simplify.TapSimplify.list(map(entry("Key","ITERATION"),entry("Value",iterationCodes)))
                    );
                }
            }catch (Exception e){
                TapLogger.debug(TAG,"Count table error: {}" ,TABLE_NAME, e.getMessage());
            }
            Map<String,Object> dataMap = issuesLoader.getIssuePage(
                    header.getEntity(),
                    body.getEntity(),
                    String.format(CodingStarter.OPEN_API_URL,connectionConfig.getString("teamName"))
            );
            if (null!= dataMap){
                Object obj = dataMap.get("TotalCount");
                if (null != obj ) count = (Integer)obj;
            }
        } catch (Exception e) {
            throw new RuntimeException();
        }
        TapLogger.debug(TAG,"Batch count is " + count);
        return count;
    }

    @Override
    public void streamRead(List<String> tableList, Object offsetState, int recordSize, StreamReadConsumer consumer) {
        CodingOffset codingOffset =
                null != offsetState && offsetState instanceof CodingOffset
                        ? (CodingOffset)offsetState : new CodingOffset();
        Map<String, Long> tableUpdateTimeMap = codingOffset.getTableUpdateTimeMap();
        if (null == tableUpdateTimeMap || tableUpdateTimeMap.isEmpty()){
            TapLogger.warn(TAG,"offsetState is Empty or not Exist!");
            return;
        }
        String currentTable = tableList.get(0);
        consumer.streamReadStarted();
        long current = tableUpdateTimeMap.get(currentTable);
        Long last = Long.MAX_VALUE;
        this.read(current, last,  recordSize, codingOffset, consumer);
    }

    @Override
    public List<TapEvent> rawDataCallbackFilterFunction(Map<String, Object> issueEventData) {
        CodingEvent issueEvent = this.getRowDataCallBackEvent(issueEventData);
        if (null == issueEvent || !TABLE_NAME.equals(issueEvent.getEventGroup())) return null;//拒绝处理非此表相关事件
        String eventType = issueEvent.getEventType();
        Object issueObj = issueEventData.get("issue");
        if (Checker.isEmpty(issueObj)) {
            TapLogger.debug(TAG, "An event with Issue Data is null or empty,this callBack is stop.The data has been discarded. Data detial is:" + issueEventData);
            return null;
        }
        Map<String, Object> issueMap = (Map<String, Object>) issueObj;
        Object codeObj = issueMap.get("code");
        if (Checker.isEmpty(codeObj)) {
            TapLogger.debug(TAG, "An event with Issue Code is be null or be empty,this callBack is stop.The data has been discarded. Data detial is:" + issueEventData);
            return null;
        }
        IssueType issueType = this.contextConfig.getIssueType();
        if (Checker.isNotEmpty(issueType)) {
            String issueTypeName = issueType.getName();
            Object o = issueMap.get("type");
            if (Checker.isNotEmpty(o) && !"ALL".equals(issueTypeName) && !issueTypeName.equals(o)) {
                return null;
            }
        }
        String iterationCodes = this.contextConfig.getIterationCodes();
        Object iterationObj = issueMap.get("iteration");
        if ( Checker.isNotEmpty(iterationCodes) && !"-1".equals(iterationCodes) ) {
            if(Checker.isNotEmpty(iterationObj)) {
                Object iteration = ((Map<String, Object>) iterationObj).get("code");
                if (Checker.isNotEmpty(iteration) && !iterationCodes.matches(".*" + String.valueOf(iteration) + ".*")) {
                    return null;
                }
            }else {
                return null;
            }
        }
        TapEvent event = null;
        Object referenceTimeObj = issueMap.get("updated_at");
        Long referenceTime = null;
        if (Checker.isNotEmpty(referenceTimeObj)) {
            referenceTime = (Long) referenceTimeObj;
        }
        Map<String, Object> issueDetail = issueMap;
        this.composeIssue(contextConfig.getProjectName(),contextConfig.getTeamName(),issueMap);
        if (!DELETED_EVENT.equals(eventType)) {
            HttpEntity<String, String> header = HttpEntity.create().builder("Authorization", this.contextConfig.getToken());
            HttpEntity<String, Object> issueDetialBody = HttpEntity.create()
                    .builder("Action", "DescribeIssue")
                    .builder("ProjectName", this.contextConfig.getProjectName());
            CodingHttp authorization = CodingHttp.create(header.getEntity(), String.format(CodingStarter.OPEN_API_URL, this.contextConfig.getTeamName()));
            HttpRequest requestDetail = authorization.createHttpRequest();
            issueDetail = this.readIssueDetail(
                    issueDetialBody,
                    authorization,
                    requestDetail,
                    (codeObj instanceof Integer) ? (Integer) codeObj : Integer.parseInt(codeObj.toString()),
                    this.contextConfig.getProjectName(),
                    this.contextConfig.getTeamName());
            if (Checker.isEmptyCollection(issueDetail)){
                return null;
            }
//            String modeName = this.tapConnectionContext.getConnectionConfig().getString("connectionMode");
//            ConnectionMode instance = ConnectionMode.getInstanceByName(this.tapConnectionContext, modeName);
//            if (null == instance){
//                throw new CoreException("Connection Mode is not empty or not null.");
//            }
            //if (instance instanceof CSVMode) {
            //    issueDetail = instance.attributeAssignment(issueDetail);
            //}else {
            //}
        }
        switch (eventType){
            case DELETED_EVENT:{
                issueDetail = (Map<String, Object>) issueObj;
                this.composeIssue(this.contextConfig.getProjectName(), this.contextConfig.getTeamName(), issueDetail);
//                issueDetail.put("teamName",this.contextConfig.getTeamName());
//                issueDetail.put("projectName",this.contextConfig.getProjectName());
                event = TapSimplify.deleteDMLEvent(issueDetail, TABLE_NAME).referenceTime(referenceTime)  ;
            }break;
            case UPDATE_EVENT:{
                event = TapSimplify.updateDMLEvent(null,issueDetail, TABLE_NAME).referenceTime(referenceTime) ;
            }break;
            case CREATED_EVENT:{
                event = TapSimplify.insertRecordEvent(issueDetail, TABLE_NAME).referenceTime(referenceTime)  ;
            }break;
        }
        TapLogger.debug(TAG,"From WebHook coding completed a event [{}] for [{}] table: event data is - {}",eventType,TABLE_NAME,issueDetail);
        return Collections.singletonList(event);
    }


    public void readV2(
            Long readStartTime,
            Long readEndTime,
            int readSize,
            Object offsetState,
            BiConsumer<List<TapEvent>, Object> consumer){
        final int MAX_THREAD = 10;
        Queue<Map<String,Object>> queuePage = new ConcurrentLinkedQueue();
        AtomicBoolean pageFlag = new AtomicBoolean(true);

        Queue<Map<String,Object>> queueItem = new ConcurrentLinkedQueue();
        AtomicInteger itemThreadCount = new AtomicInteger(0);

        List<Map<String,Object>> coditions = io.tapdata.entity.simplify.TapSimplify.list(map(
                entry("Key","UPDATED_AT"),
                entry("Value", this.longToDateStr(readStartTime)+"_"+ this.longToDateStr(readEndTime)))
        );

        final List<TapEvent>[] events = new List[]{new CopyOnWriteArrayList()};
        HttpEntity<String,String> header = HttpEntity.create().builder("Authorization",contextConfig.getToken());
        String projectName = contextConfig.getProjectName();
        HttpEntity<String,Object> pageBody = HttpEntity.create()
                .builder("Action","DescribeIssueListWithPage")
                .builder("ProjectName",projectName)
                .builder("SortKey","UPDATED_AT")
                .builder("PageSize",readSize)
                .builder("SortValue","ASC");
        if (Checker.isNotEmpty(contextConfig) && Checker.isNotEmpty(contextConfig.getIssueType())){
            pageBody.builder("IssueType", IssueType.verifyType(contextConfig.getIssueType().getName()));
        }else {
            pageBody.builder("IssueType","ALL");
        }

        String iterationCodes = contextConfig.getIterationCodes();
        if (null != iterationCodes && !"".equals(iterationCodes) && !",".equals(iterationCodes)) {
            if (!"-1".equals(iterationCodes)) {
                //-1时表示全选
                //String[] iterationCodeArr = iterationCodes.split(",");
                //@TODO 输入的迭代编号需要验证，否则，查询事项列表时作为查询条件的迭代不存在时，查询会报错
                //选择的迭代编号不需要验证
                coditions.add(map(entry("Key", "ITERATION"), entry("Value", iterationCodes)));
            }
        }
        pageBody.builder("Conditions",coditions);
        String teamName = contextConfig.getTeamName();
        if (Checker.isEmpty(offsetState)) {
            offsetState = new CodingOffset();
        }
        CodingOffset offset = (CodingOffset)offsetState;

        AtomicInteger total = new AtomicInteger(-1);
        //分页线程
        new Thread(()->{
            pageFlag.set(true);
            int currentQueryCount = 0,queryIndex = 0 ;
            do{
                /**
                 * start page ,and add page to queuePage;
                 * */
                pageBody.builder("PageNumber",queryIndex++);
                Map<String,Object> dataMap = this.getIssuePage(header.getEntity(),pageBody.getEntity(),String.format(CodingStarter.OPEN_API_URL,teamName));
                if (null == dataMap || null == dataMap.get("List")) {
                    TapLogger.error(TAG, "Paging result request failed, the Issue list is empty: page index = {}",queryIndex);
                    pageFlag.set(false);
                    throw new RuntimeException("Paging result request failed, the Issue list is empty: "+CodingStarter.OPEN_API_URL+"?Action=DescribeIssueListWithPage");
                }
                List<Map<String,Object>> resultList = (List<Map<String,Object>>) dataMap.get("List");
                currentQueryCount = resultList.size();
                batchReadPageSize = null != dataMap.get("PageSize") ? (int)(dataMap.get("PageSize")) : batchReadPageSize;
                if (total.get()<0){
                    total .set((int)(dataMap.get("TotalCount")));
                }
                queuePage.addAll(resultList);
            }while (currentQueryCount >= batchReadPageSize );

            pageFlag.set(false);
        },"PAGE_THREAD").start();

        Runnable runnable = ()->{
            itemThreadCount.getAndAdd(1);
            /**
             * start page ,and add page to queuePage;
             * */
            while (!queuePage.isEmpty() || pageFlag.get()){
                Map<String, Object> peek = queuePage.poll();
                Object code = peek.get("Code");
                Map<String,Object> issueDetail = this.get(IssueParam.create().issueCode((Integer)code));
                if (Checker.isNotEmpty(issueDetail)){
                    queueItem.add(issueDetail);
                }
            }
            itemThreadCount.getAndAdd(-1);
        };
        //详情查询线程
        while (true){
            if (!pageFlag.get() && queuePage.isEmpty()){
                break;
            }
            if (!queuePage.isEmpty()){
                int threadCount = total.get()/1000;
                threadCount = Math.min(threadCount, MAX_THREAD);
                final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(threadCount + 1);
                for (int i = 0; i < threadCount; i++) {
                    executor.schedule(runnable,1, TimeUnit.SECONDS);
                }
                break;
            }
        }

        //主线程生成事件
        while (pageFlag.get() || itemThreadCount.get()>0 || !queuePage.isEmpty() || !queueItem.isEmpty()){
            /**
             * 从queueItem取数据生成事件
             * **/
            if (queueItem.isEmpty()) continue;
            Map<String,Object> issueDetail = queueItem.poll();
            if (Checker.isNotEmptyCollection(issueDetail)){
                Long referenceTime = (Long) issueDetail.get("UpdatedAt");
                Long currentTimePoint = referenceTime - referenceTime % (24 * 60 * 60 * 1000);//时间片段
                Integer issueDetialHash = MapUtil.create().hashCode(issueDetail);

                //issueDetial的更新时间字段值是否属于当前时间片段，并且issueDiteal的hashcode是否在上一次批量读取同一时间段内
                //如果不在，说明时全新增加或修改的数据，需要在本次读取这条数据
                //如果在，说明上一次批量读取中以及读取了这条数据，本次不在需要读取 !currentTimePoint.equals(lastTimePoint) &&
                if (!lastTimeSplitIssueCode.contains(issueDetialHash)) {
                    events[0].add(TapSimplify.insertRecordEvent(issueDetail, TABLE_NAME).referenceTime(System.currentTimeMillis()));

                    if (null == currentTimePoint || !currentTimePoint.equals(lastTimePoint)) {
                        lastTimePoint = currentTimePoint;
                        lastTimeSplitIssueCode = new ArrayList<Integer>();
                    }
                    lastTimeSplitIssueCode.add(issueDetialHash);
                }

                if (Checker.isEmpty(offsetState)) {
                    offsetState = new CodingOffset();
                }
                ((CodingOffset) offsetState).getTableUpdateTimeMap().put(TABLE_NAME, referenceTime);


                if (events[0].size() == readSize) {
                    consumer.accept(events[0], offsetState);
                    events[0] = new ArrayList<>();
                }
            }
        }

        if (!events[0].isEmpty()) {
            consumer.accept(events[0], offset);
        }
    }
    /**
     * 分页读取事项列表，并依次查询事项详情
     * @param readStartTime
     * @param readEndTime
     * @param readSize
     * @param consumer
     */
    public void read(
            Long readStartTime,
            Long readEndTime,
            int readSize,
            Object offsetState,
            BiConsumer<List<TapEvent>, Object> consumer){
        int currentQueryCount = 0,queryIndex = 0 ;
        final List<TapEvent>[] events = new List[]{new CopyOnWriteArrayList()};
        HttpEntity<String,String> header = HttpEntity.create().builder("Authorization",this.contextConfig.getToken());
        String projectName = this.contextConfig.getProjectName();
        HttpEntity<String,Object> pageBody = HttpEntity.create()
                .builder("Action","DescribeIssueListWithPage")
                .builder("ProjectName",projectName)
                .builder("SortKey","UPDATED_AT")
                .builder("PageSize",readSize)
                .builder("SortValue","ASC");
        if (Checker.isNotEmpty(this.contextConfig) && Checker.isNotEmpty(this.contextConfig.getIssueType())){
            pageBody.builder("IssueType", IssueType.verifyType(this.contextConfig.getIssueType().getName()));
        }else {
            pageBody.builder("IssueType","ALL");
        }
        List<Map<String,Object>> coditions = io.tapdata.entity.simplify.TapSimplify.list(map(
                entry("Key","UPDATED_AT"),
                entry("Value", this.longToDateStr(readStartTime)+"_"+ this.longToDateStr(readEndTime)))
        );
        String iterationCodes = this.contextConfig.getIterationCodes();
        if (null != iterationCodes && !"".equals(iterationCodes) && !",".equals(iterationCodes)) {
            if (!"-1".equals(iterationCodes)) {
                //-1时表示全选
                //String[] iterationCodeArr = iterationCodes.split(",");
                //@TODO 输入的迭代编号需要验证，否则，查询事项列表时作为查询条件的迭代不存在时，查询会报错
                //选择的迭代编号不需要验证
                coditions.add(map(entry("Key", "ITERATION"), entry("Value", iterationCodes)));
            }
        }
        pageBody.builder("Conditions",coditions);
        String teamName = this.contextConfig.getTeamName();
//        String modeName = this.contextConfig.getConnectionMode();
//        ConnectionMode instance = ConnectionMode.getInstanceByName(nodeContext,modeName);
//        if (null == instance){
//            throw new CoreException("Connection Mode is not empty or not null.");
//        }
        if (Checker.isEmpty(offsetState)) {
            offsetState = new CodingOffset();
        }
        CodingOffset offset = (CodingOffset)offsetState;
        do{
            pageBody.builder("PageNumber",queryIndex++);
            Map<String,Object> dataMap = this.getIssuePage(header.getEntity(),pageBody.getEntity(),String.format(CodingStarter.OPEN_API_URL,teamName));
            if (null == dataMap || null == dataMap.get("List")) {
                TapLogger.error(TAG, "Paging result request failed, the Issue list is empty: page index = {}",queryIndex);
                throw new RuntimeException("Paging result request failed, the Issue list is empty: "+CodingStarter.OPEN_API_URL+"?Action=DescribeIssueListWithPage");
            }
            List<Map<String,Object>> resultList = (List<Map<String,Object>>) dataMap.get("List");
            currentQueryCount = resultList.size();
            batchReadPageSize = null != dataMap.get("PageSize") ? (int)(dataMap.get("PageSize")) : batchReadPageSize;
            for (Map<String, Object> stringObjectMap : resultList) {
                Object code = stringObjectMap.get("Code");

                //Map<String,Object> issueDetail = instance.attributeAssignment(stringObjectMap);
                Map<String,Object> issueDetail = this.get(IssueParam.create().issueCode((Integer)code));// stringObjectMap;
                //if (null == issueDetail){
                //    events[0].add(TapSimplify.insertRecordEvent(stringObjectMap, TABLE_NAME).referenceTime(System.currentTimeMillis()));
                //    events[0].add(TapSimplify.deleteDMLEvent(stringObjectMap, TABLE_NAME).referenceTime(System.currentTimeMillis()));
                //}else
                if (Checker.isNotEmptyCollection(issueDetail)){
                    Long referenceTime = (Long) issueDetail.get("UpdatedAt");
                    Long currentTimePoint = referenceTime - referenceTime % (24 * 60 * 60 * 1000);//时间片段
                    Integer issueDetialHash = MapUtil.create().hashCode(issueDetail);

                    //issueDetial的更新时间字段值是否属于当前时间片段，并且issueDiteal的hashcode是否在上一次批量读取同一时间段内
                    //如果不在，说明时全新增加或修改的数据，需要在本次读取这条数据
                    //如果在，说明上一次批量读取中以及读取了这条数据，本次不在需要读取 !currentTimePoint.equals(lastTimePoint) &&
                    if (!lastTimeSplitIssueCode.contains(issueDetialHash)) {
                        events[0].add(TapSimplify.insertRecordEvent(issueDetail, TABLE_NAME).referenceTime(System.currentTimeMillis()));

                        if (null == currentTimePoint || !currentTimePoint.equals(this.lastTimePoint)) {
                            this.lastTimePoint = currentTimePoint;
                            lastTimeSplitIssueCode = new ArrayList<Integer>();
                        }
                        lastTimeSplitIssueCode.add(issueDetialHash);
                    }

                    if (Checker.isEmpty(offsetState)) {
                        offsetState = new CodingOffset();
                    }
                    ((CodingOffset) offsetState).getTableUpdateTimeMap().put(TABLE_NAME, referenceTime);


                    if (events[0].size() == readSize) {
                        consumer.accept(events[0], offsetState);
                        events[0] = new ArrayList<>();
                    }
                }
        }
        }while (currentQueryCount >= batchReadPageSize && !stopRead.get());
        if (!events[0].isEmpty()) {
            consumer.accept(events[0], offset);
        }
        //startRead.set(false);
    }


    public synchronized void run(Map<String,Object> issueDetail,Map<String,Object> stringObjectMap,List<TapEvent>[] events,Integer issueDetialHash,CodingOffset offsetState,int readSize,BiConsumer<List<TapEvent>, Object> consumer){
            if (null == issueDetail) {
                events[0].add(TapSimplify.insertRecordEvent(stringObjectMap, TABLE_NAME).referenceTime(System.currentTimeMillis()));
                events[0].add(TapSimplify.deleteDMLEvent(stringObjectMap, TABLE_NAME).referenceTime(System.currentTimeMillis()));
            } else {
                Long referenceTime = (Long) issueDetail.get("UpdatedAt");
                Long currentTimePoint = referenceTime - referenceTime % (24 * 60 * 60 * 1000);
                if (!lastTimeSplitIssueCode.contains(issueDetialHash)) {
                    events[0].add(TapSimplify.insertRecordEvent(issueDetail, TABLE_NAME).referenceTime(System.currentTimeMillis()));

                    if (null == currentTimePoint || !currentTimePoint.equals(this.lastTimePoint)) {
                        this.lastTimePoint = currentTimePoint;
                        lastTimeSplitIssueCode = new ArrayList<Integer>();
                    }
                    lastTimeSplitIssueCode.add(issueDetialHash);
                }
                offsetState.getTableUpdateTimeMap().put(TABLE_NAME, referenceTime);
            }
            if (events[0].size() == readSize) {
                consumer.accept(events[0], offsetState);
                synchronized (events[0]){
                    if (!events[0].isEmpty())
                        events[0] = new ArrayList<>();
                }
            }
    }
}
