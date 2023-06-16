package io.tapdata.coding.service.loader;

import cn.hutool.http.HttpRequest;
import cn.hutool.json.JSONArray;
import io.tapdata.coding.CodingConnector;
import io.tapdata.coding.entity.CodingOffset;
import io.tapdata.coding.entity.ContextConfig;
import io.tapdata.coding.entity.param.IssueParam;
import io.tapdata.coding.enums.CodingEvent;
import io.tapdata.coding.enums.IssueType;
import io.tapdata.coding.utils.collection.MapUtil;
import io.tapdata.coding.utils.http.CodingHttp;
import io.tapdata.coding.utils.http.ErrorHttpException;
import io.tapdata.coding.utils.http.HttpEntity;
import io.tapdata.coding.utils.tool.Checker;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.Entry;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static io.tapdata.coding.enums.TapEventTypes.*;
import static io.tapdata.entity.simplify.TapSimplify.map;
import static io.tapdata.entity.simplify.TapSimplify.toJson;

/**
 * @author GavinX
 * @Description
 * @create 2022-08-26 11:49
 **/
public class IssuesLoader extends CodingStarter implements CodingLoader<IssueParam> {
    private static final String TAG = IssuesLoader.class.getSimpleName();
    public static final String TABLE_NAME = "Issues";

    public static IssuesLoader create(TapConnectionContext tapConnectionContext, AtomicReference<String> accessToken) {
        return new IssuesLoader(tapConnectionContext, accessToken);
    }

    private final long streamExecutionGap = 5000;//util: ms
    private int batchReadPageSize = 500;//coding page 1~500,
    private Long lastTimePoint;
    private Set<String> lastTimeSplitIssueCode = new HashSet<>();//hash code list
    int tableSize;
    private AtomicBoolean stopRead = new AtomicBoolean(false);

    public IssuesLoader(TapConnectionContext tapConnectionContext, AtomicReference<String> accessToken) {
        super(tapConnectionContext, accessToken);
        this.codingConfig = this.veryContextConfigAndNodeConfig();
    }

    public CodingStarter connectorInit(CodingConnector codingConnector) {
        super.connectorInit(codingConnector);
        this.lastTimeSplitIssueCode.addAll(codingConnector.lastTimeSplitIssueCode());
        this.lastTimePoint = codingConnector.issuesLastTimePoint();
        return this;
    }

    public CodingStarter connectorOut() {
        this.codingConnector.lastTimeSplitIssueCode(this.lastTimeSplitIssueCode);
        this.codingConnector.issuesLastTimePoint(this.lastTimePoint);
        return super.connectorOut();
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

    /**
     * 一次获取事项分页查询并返回Map结果
     *
     * @param url
     * @return
     * @auth GavinX
     */
    public Map<String, Object> getIssuePage(
            Map<String, String> header,
            Map<String, Object> body,
            String url) {
        Map<String, Object> resultMap = CodingHttp.create(header, body, url).post();
        Object response = resultMap.get("Response");
        Map<String, Object> responseMap = (Map<String, Object>) response;
        if (null == response) {
            throw new RuntimeException("HTTP request exception, Issue list acquisition failed: " + Optional.ofNullable(responseMap.get(CodingHttp.ERROR_KEY)).orElse(url) + "?Action=DescribeIssueListWithPage");
        }
        Object data = responseMap.get("Data");
        if (null == data) {
            throw new CoreException("Can't get issues page, the response's 'Data' is empty.");
        }
        return (Map<String, Object>) data;
    }

    /**
     * @param projectName
     * @param teamName
     * @param issueDetail
     * @auth GavinX
     */
    public void composeIssue(String projectName, String teamName, Map<String, Object> issueDetail) {
        //this.addParamToBatch(issueDetail);//给自定义字段赋值
        issueDetail.put("ProjectName", projectName);
        issueDetail.put("TeamName", teamName);
    }

    /**
     * @param batchMap
     * @auth GavinX
     * 向事项详细信息返回结果中添加部分指定字段值
     */
    public void addParamToBatch(Map<String, Object> batchMap) {
        this.putObject(batchMap, "IssueTypeDetail", "Id", "IssueTypeDetailId");
        this.putObject(batchMap, "Assignee", "Id", "AssigneeId");
        this.putObject(batchMap, "Creator", "Id", "CreatorId");
        this.putObject(batchMap, "ProjectModule", "Id", "ProjectModuleId");
        this.putObject(batchMap, "Parent", "Code", "ParentCode");
        this.putObject(batchMap, "Epic", "Code", "EpicCode");
        this.putObject(batchMap, "Iteration", "Code", "IterationCode");
        this.putObject(batchMap, "Watchers", "Id", "WatcherIdArr");
        this.putObject(batchMap, "Labels", "Id", "LabelIdArr");
        this.putObject(batchMap, "Files", "Id", "FileIdArr");
        this.putObject(batchMap, "SubTasks", "Code", "SubTaskCodeArr");
    }

    /**
     * @param batchMap
     * @param fromObj
     * @param fromKey
     * @param targetKey
     * @auth GavinX
     */
    private void putObject(Map<String, Object> batchMap, String fromObj, String fromKey, String targetKey) {
        Object obj = batchMap.get(fromObj);
        if (null != obj && obj instanceof Map) {
            Map<String, Object> fromObjMap = (Map<String, Object>) obj;
            batchMap.put(targetKey, fromObjMap.get(fromKey));
        }
        if (null != obj && obj instanceof List) {
            List<Object> fromObjList = (List) obj;
            if (null != fromObjList && fromObjList.size() > 0) {
                List<Object> keyArr = new ArrayList<>();
                fromObjList.forEach(o -> {
                    Object key = ((Map<String, Object>) o).get(fromKey);
                    if (null != key) keyArr.add(key);
                });
                batchMap.put(targetKey, keyArr);
            }
            batchMap.put(targetKey, new ArrayList<Integer>());
        }
    }

    public Map<String, Object> readIssueDetail(
            HttpEntity<String, Object> issueDetailBody,
            CodingHttp authorization,
            HttpRequest requestDetail,
            Integer code,
            String projectName,
            String teamName) {
        //查询事项详情
        issueDetailBody.builder("IssueCode", code);
        CodingHttp codingHttp = authorization.body(issueDetailBody.getEntity());
        Map<String, Object> issueDetailResponse = codingHttp.post(requestDetail);
        if (null == issueDetailResponse) {
            TapLogger.info(TAG, "HTTP request exception, Issue Detail acquisition failed: {} ", CodingStarter.OPEN_API_URL + "?Action=DescribeIssue&IssueCode=" + code);
            throw new RuntimeException("HTTP request exception, Issue Detail acquisition failed: " + CodingStarter.OPEN_API_URL + "?Action=DescribeIssue&IssueCode=" + code);
        }
        issueDetailResponse = (Map<String, Object>) issueDetailResponse.get("Response");
        if (null == issueDetailResponse) {
            throw new RuntimeException("HTTP request exception, Issue Detail acquisition failed: " + CodingStarter.OPEN_API_URL + "?Action=DescribeIssue&IssueCode=" + code + ". " + Optional.ofNullable(issueDetailResponse.get(CodingHttp.ERROR_KEY)).orElse(""));
        }
        Map<String, Object> issueDetail = (Map<String, Object>) issueDetailResponse.get("Issue");
        if (null == issueDetail) {
            throw new RuntimeException("Cant't get 'Issue' in response. Issue Detail acquisition failed: IssueCode " + code);
        }
        this.composeIssue(projectName, teamName, issueDetail);
        return issueDetail;
    }

    Map<String, Map<Integer, String>> allCustomFieldMap;

    private Map<Integer, String> getIssueCustomFieldMap(String issueType, ContextConfig contextConfig) {
        if (null == allCustomFieldMap) {
            allCustomFieldMap = new HashMap<>();
        }
        if (null != allCustomFieldMap.get(issueType)) {
            return allCustomFieldMap.get(issueType);
        }
        Map<Integer, String> customFieldMap = new HashMap<>();

        HttpEntity<String, String> heard = HttpEntity.create().builder("Authorization", this.accessToken().get());
        HttpEntity<String, Object> body = HttpEntity.create()
                .builder("Action", "DescribeProjectIssueFieldList")
                .builder("ProjectName", contextConfig.getProjectName())
                .builder("IssueType", issueType);
        Map<String, Object> post = CodingHttp.create(heard.getEntity(), body.getEntity(), String.format(CodingStarter.OPEN_API_URL)).post();
        Object response = post.get("Response");
        Map<String, Object> responseMap = (Map<String, Object>) response;
        if (null == response) {
            throw new CoreException("HTTP request exception, Issue CustomField acquisition failed: " + CodingStarter.OPEN_API_URL + "?Action=DescribeProjectIssueFieldList. " + Optional.ofNullable(post.get(CodingHttp.ERROR_KEY)).orElse(""));
        }
        Object data = responseMap.get("ProjectIssueFieldList");
        if (null != data && data instanceof JSONArray) {
            List<Map<String, Object>> list = (ArrayList) data;
            list.forEach(field -> {
                Object fieldObj = field.get("IssueField");
                if (null != fieldObj) {
                    Map<String, Object> fieldDetial = (Map<String, Object>) fieldObj;
                    Object fieldIdObj = fieldDetial.get("Id");
                    Object fieldNameObj = fieldDetial.get("Name");
                    customFieldMap.put(Integer.parseInt(String.valueOf(fieldIdObj)), String.valueOf(fieldNameObj));
                }
            });
        }
        allCustomFieldMap.put(issueType, customFieldMap);
        return customFieldMap;
    }

    private String getCustomFieldName(String issueType, Integer customId, ContextConfig contextConfig) {
        Map<Integer, String> customFields = this.getIssueCustomFieldMap(issueType, contextConfig);
        if (null == customFields || customFields.size() < 1) {
            throw new CoreException("Can't get custom fields.");
        }
        return customFields.get(customId);
    }

    public List<Map<String, Object>> getAllIssueType() {
        HttpEntity<String, String> header = HttpEntity.create().builder("Authorization", this.accessToken().get());
        HttpEntity<String, Object> pageBody = HttpEntity.create().builder("Action", "DescribeTeamIssueTypeList");
        Map<String, Object> issueResponse = CodingHttp.create(
                header.getEntity(),
                pageBody.getEntity(),
                String.format(CodingStarter.OPEN_API_URL, this.codingConfig.getTeamName())
        ).post();
        if (Checker.isEmpty(issueResponse)) {
            throw new CoreException("Can't get all issues types, the http response is empty.");
        }
        Object response = issueResponse.get("Response");
        if (null == response) {
            throw new CoreException("Can't get all issues types, the 'Response' is empty. " + Optional.ofNullable(issueResponse.get(CodingHttp.ERROR_KEY)).orElse(""));
        }
        Object issueTypes = ((Map<String, Object>) response).get("IssueTypes");
        if (Checker.isEmpty(issueTypes)) {
            throw new CoreException("Can't get all issues types, the 'IssueTypes' is empty.");
        }
        return (List<Map<String, Object>>) issueTypes;
    }

    @Override
    public Long streamReadTime() {
        return 2 * 60 * 1000L;
    }

    @Override
    public List<Map<String, Object>> list(IssueParam param) {
        Map<String, Object> resultMap = this.codingHttp(param).post();
        Object response = resultMap.get("Response");
        if (null == response) {
            throw new CoreException("Can't get all issues types, the 'Response' is empty." + ". " + Optional.ofNullable(resultMap.get(CodingHttp.ERROR_KEY)).orElse(""));
        }
        Map<String, Object> responseMap = (Map<String, Object>) response;
        Object dataObj = responseMap.get("Data");
        if (null == dataObj) {
            throw new CoreException("Can't get all issues types, the 'Data' is empty.");
        }
        Map<String, Object> data = (Map<String, Object>) dataObj;
        Object listObj = data.get("List");
        if (Checker.isEmpty(listObj)) {
            throw new CoreException("Can't get all issues types, the 'List' is empty.");
        }
        return (List<Map<String, Object>>) listObj;
    }

    @Override
    public List<Map<String, Object>> all(IssueParam param) {
        return null;
    }

    @Override
    public CodingHttp codingHttp(IssueParam param) {
        param.action("DescribeIterationList");
        HttpEntity<String, String> header = HttpEntity.create()
                .builder("Authorization", this.accessToken().get());
        HttpEntity<String, Object> body = HttpEntity.create()
                .builderIfNotAbsent("Action", "DescribeIssueListWithPage")
                .builder("ProjectName", this.codingConfig.getProjectName())
                .builder("IssueType", param.issueType().getName())
                .builder("PageNumber", param.offset())
                .builder("PageSize", param.limit())
                .builderIfNotAbsent("Conditions", param.conditions())
                .builderIfNotAbsent("SortKey", param.sortKey())
                .builderIfNotAbsent("SortValue", param.sortValue());
        return CodingHttp.create(
                header.getEntity(),
                body.getEntity(),
                String.format(OPEN_API_URL, this.codingConfig.getTeamName()));
    }

    @Override
    public Map<String, Object> get(IssueParam param) {
        HttpEntity<String, String> header = HttpEntity.create()
                .builder("Authorization", this.accessToken().get());
        HttpEntity<String, Object> body = HttpEntity.create()
                .builderIfNotAbsent("Action", "DescribeIssue")
                .builder("ProjectName", this.codingConfig.getProjectName())
                .builder("IssueCode", param.issueCode());
        Map<String, Object> resultMap = CodingHttp.create(
                header.getEntity(),
                body.getEntity(),
                String.format(OPEN_API_URL, this.codingConfig.getTeamName())).needRetry(true).isAlive(this.stopRead).post();
        Object response = resultMap.get("Response");
        if (null == response) {
            throw new CoreException(String.format("Cannot get the result which name is 'Response' from http response body, request url - %s,param - %s, request body - %s, msg - %s",
                    String.format(OPEN_API_URL, this.codingConfig.getTeamName()),
                    toJson(param),
                    toJson(resultMap),
                    Optional.ofNullable(resultMap.get(CodingHttp.ERROR_KEY)).orElse("")));
        }
        Map<String, Object> responseMap = (Map<String, Object>) response;
        Object dataObj = responseMap.get("Issue");
        if (Checker.isEmpty(dataObj)) {
            throw new CoreException(String.format("Cannot get the result which name is 'Issue' from http response body, request url - %s,param - %s, request body - %s",
                    String.format(OPEN_API_URL, this.codingConfig.getTeamName()),
                    toJson(param),
                    toJson(resultMap)));
        }
        Map<String, Object> result = (Map<String, Object>) dataObj;
        if (Checker.isNotEmpty(result)) {
            this.composeIssue(this.codingConfig.getProjectName(), this.codingConfig.getTeamName(), result);
        }
        return result;
    }

    @Override
    public void batchRead(Object offset, int batchCount, BiConsumer<List<TapEvent>, Object> consumer) {
        Long readEnd = System.currentTimeMillis();
        CodingOffset codingOffset = new CodingOffset();
        //current read end as next read begin
        codingOffset.setTableUpdateTimeMap(new HashMap<String, Long>() {{
            put(TABLE_NAME, readEnd);
        }});
        this.verifyConnectionConfig();
        this.readV2(null, readEnd, batchCount, codingOffset, consumer);
    }

    @Override
    public int batchCount() throws Throwable {
        int count = 0;
        IssuesLoader issuesLoader = IssuesLoader.create(this.tapConnectionContext, super.accessToken());
        issuesLoader.verifyConnectionConfig();
        try {
            DataMap connectionConfig = this.tapConnectionContext.getConnectionConfig();
            String token = accessToken().get();
            HttpEntity<String, String> header = HttpEntity.create()
                    .builder("Authorization", token);
            HttpEntity<String, Object> body = HttpEntity.create()
                    .builder("Action", "DescribeIssueListWithPage")
                    .builder("ProjectName", connectionConfig.getString("projectName"))
                    .builder("PageSize", 1)
                    .builder("ShowSubIssues", true)
                    .builder("PageNumber", 1);
            try {
                DataMap nodeConfigMap = this.tapConnectionContext.getNodeConfig();
                if (null != nodeConfigMap) {
                    String iterationCodes = nodeConfigMap.getString("DescribeIterationList");//iterationCodes
                    if (null != iterationCodes) iterationCodes = iterationCodes.trim();
                    String issueType = nodeConfigMap.getString("issueType");
                    if (null != issueType) issueType = issueType.trim();

                    body.builder("IssueType", IssueType.verifyType(issueType));

                    if (null != iterationCodes && !"".equals(iterationCodes) && !",".equals(iterationCodes) && !"-1".equals(iterationCodes)) {
                        //String[] iterationCodeArr = iterationCodes.split(",");
                        //@TODO 输入的迭代编号需要验证，否则，查询事项列表时作为查询条件的迭代不存在时，查询会报错
                        //选择的迭代编号不需要验证
                        body.builder(
                                "Conditions",
                                io.tapdata.entity.simplify.TapSimplify.list(map(entry("Key", "ITERATION"), entry("Value", iterationCodes)))
                        );
                    }
                } else {
                    body.builder("IssueType", "ALL");
                }
            } catch (Exception e) {
                TapLogger.debug(TAG, "Count table error: {}", TABLE_NAME, e.getMessage());
            }
            Map<String, Object> dataMap = issuesLoader.getIssuePage(
                    header.getEntity(),
                    body.getEntity(),
                    String.format(CodingStarter.OPEN_API_URL, connectionConfig.getString("teamName"))
            );
            if (null != dataMap) {
                Object obj = dataMap.get("TotalCount");
                if (null != obj) count = (Integer) obj;
            }
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
        TapLogger.debug(TAG, "Batch count is " + count);
        return count;
    }

    @Override
    public void streamRead(List<String> tableList, Object offsetState, int recordSize, StreamReadConsumer consumer) {
        CodingOffset codingOffset =
                offsetState instanceof CodingOffset
                        ? (CodingOffset) offsetState : new CodingOffset();
        Map<String, Long> tableUpdateTimeMap = codingOffset.getTableUpdateTimeMap();
        if (null == tableUpdateTimeMap || tableUpdateTimeMap.isEmpty()) {
            TapLogger.warn(TAG, "offsetState is Empty or not Exist. Stop to stream read.");
            return;
        }
        //String currentTable = tableList.get(0);
        //consumer.streamReadStarted();
        long current = tableUpdateTimeMap.get(TABLE_NAME);
        Long last = Long.MAX_VALUE;
        this.read(current, last, recordSize, codingOffset, consumer, true);
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
        IssueType issueType = this.codingConfig.getIssueType();
        if (Checker.isNotEmpty(issueType)) {
            String issueTypeName = issueType.getName();
            Object o = issueMap.get("type");
            if (Checker.isNotEmpty(o) && !"ALL".equals(issueTypeName) && !issueTypeName.equals(o)) {
                TapLogger.info(TAG, "The current event is not within the processing range of this data source and will not be processed");
                return null;
            }
        }
        String iterationCodes = this.codingConfig.getIterationCodes();
        Object iterationObj = issueMap.get("iteration");
        if (Checker.isNotEmpty(iterationCodes) && !"-1".equals(iterationCodes)) {
            if (Checker.isNotEmpty(iterationObj)) {
                Object iteration = ((Map<String, Object>) iterationObj).get("code");
                if (Checker.isNotEmpty(iteration) && !iterationCodes.matches(".*" + String.valueOf(iteration) + ".*")) {
                    TapLogger.info(TAG, " The current event is not within the iteration range selected by this data source and will not be processed .");
                    return null;
                }
            } else {
                TapLogger.info(TAG, "The current event does not belong to any iteration and is not in the selected filter range");
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
        this.composeIssue(codingConfig.getProjectName(), codingConfig.getTeamName(), issueMap);
        if (!DELETED_EVENT.equals(eventType)) {
            HttpEntity<String, String> header = HttpEntity.create().builder("Authorization", this.codingConfig.getToken());
            HttpEntity<String, Object> issueDetialBody = HttpEntity.create()
                    .builder("Action", "DescribeIssue")
                    .builder("ProjectName", this.codingConfig.getProjectName());
            CodingHttp authorization = CodingHttp.create(header.getEntity(), String.format(CodingStarter.OPEN_API_URL, this.codingConfig.getTeamName()));
            HttpRequest requestDetail = authorization.createHttpRequest();
            issueDetail = this.readIssueDetail(
                    issueDetialBody,
                    authorization,
                    requestDetail,
                    (codeObj instanceof Integer) ? (Integer) codeObj : Integer.parseInt(codeObj.toString()),
                    this.codingConfig.getProjectName(),
                    this.codingConfig.getTeamName());
            if (Checker.isEmptyCollection(issueDetail)) {
                TapLogger.info(TAG, "The details of the event are not found. The current event may have been deleted recently. Please check and confirm.Issues code = {}", codeObj);
                return null;
            }
        }
        switch (eventType) {
            case DELETED_EVENT: {
                issueDetail = (Map<String, Object>) issueObj;
                Map<String, Object> deleteMap = map(entry("Code", issueDetail.get("code")));
                this.composeIssue(this.codingConfig.getProjectName(), this.codingConfig.getTeamName(), deleteMap);
                event = TapSimplify.deleteDMLEvent(deleteMap, TABLE_NAME).referenceTime(referenceTime);
            }
            break;
            case UPDATE_EVENT: {
                event = TapSimplify.updateDMLEvent(null, issueDetail, TABLE_NAME).referenceTime(referenceTime);
            }
            break;
            case CREATED_EVENT: {
                event = TapSimplify.insertRecordEvent(issueDetail, TABLE_NAME).referenceTime(referenceTime);
            }
            break;
        }
        TapLogger.debug(TAG, "From WebHook coding completed a event [{}] for [{}] table: event data is - {}", eventType, TABLE_NAME, issueDetail);
        return Collections.singletonList(event);
    }

    private String sortKey(boolean isStreamRead) {
        return isStreamRead ? "UPDATED_AT" : "CREATED_AT";
    }

    public void defineHttpAttributes(Long readStartTime,
                                     Long readEndTime,
                                     int readSize,
                                     HttpEntity<String, String> header,
                                     HttpEntity<String, Object> pageBody,
                                     boolean isStreamRead) {
        List<Map<String, Object>> coditions = io.tapdata.entity.simplify.TapSimplify.list(map(
                entry("Key", this.sortKey(isStreamRead)),
                entry("Value", this.longToDateStr(readStartTime) + "_" + this.longToDateStr(readEndTime)))
        );
        header.builder("Authorization", this.accessToken().get());
        String projectName = codingConfig.getProjectName();
        String iterationCodes = codingConfig.getIterationCodes();
        if (null != iterationCodes && !"".equals(iterationCodes) && !",".equals(iterationCodes) && !"-1".equals(iterationCodes)) {
            //-1时表示全选
            //String[] iterationCodeArr = iterationCodes.split(",");
            //@TODO 输入的迭代编号需要验证，否则，查询事项列表时作为查询条件的迭代不存在时，查询会报错
            //选择的迭代编号不需要验证
            coditions.add(map(entry("Key", "ITERATION"), entry("Value", iterationCodes)));
        }
        pageBody.builder("Action", "DescribeIssueListWithPage")
                .builder("ProjectName", projectName)
                .builder("SortKey", this.sortKey(isStreamRead))
                .builder("PageSize", readSize)
                .builder("SortValue", "ASC")
                .builder("ShowSubIssues", true)
                .builder("IssueType",
                        Checker.isNotEmpty(codingConfig) && Checker.isNotEmpty(codingConfig.getIssueType()) ?
                                IssueType.verifyType(codingConfig.getIssueType().getName())
                                : "ALL")
                .builder("Conditions", coditions);

    }

    public void defineHttpAttributesV2(int readSize, HttpEntity<String, String> header, HttpEntity<String, Object> pageBody, boolean isStreamRead) {
        List<Map<String, Object>> coditions = io.tapdata.entity.simplify.TapSimplify.list();
        header.builder("Authorization", this.accessToken().get());
        String projectName = codingConfig.getProjectName();
        String iterationCodes = codingConfig.getIterationCodes();
        if (null != iterationCodes && !"".equals(iterationCodes) && !",".equals(iterationCodes) && !"-1".equals(iterationCodes)) {
            //-1时表示全选
            //String[] iterationCodeArr = iterationCodes.split(",");
            //@TODO 输入的迭代编号需要验证，否则，查询事项列表时作为查询条件的迭代不存在时，查询会报错
            //选择的迭代编号不需要验证
            coditions.add(map(entry("Key", "ITERATION"), entry("Value", iterationCodes)));
            pageBody.builder("Conditions", coditions);
        }
        pageBody.builder("Action", "DescribeIssueListWithPage")
                .builder("ProjectName", projectName)
                .builder("SortKey", this.sortKey(isStreamRead))
                .builder("PageSize", readSize)
                .builder("PageNumber", 1)
                .builder("SortValue", "ASC")
                .builder("ShowSubIssues", true)
                .builder("IssueType",
                        Checker.isNotEmpty(codingConfig) && Checker.isNotEmpty(codingConfig.getIssueType()) ?
                                IssueType.verifyType(codingConfig.getIssueType().getName())
                                : "ALL");
    }

    public void readV2(
            Long readStartTime,
            Long readEndTime,
            int readSize,
            Object offsetState,
            BiConsumer<List<TapEvent>, Object> consumer) {
        final int MAX_THREAD = 20;
        Queue<Map.Entry<Integer, Integer>> queuePage = new ConcurrentLinkedQueue<>();
        Queue<Map<String, Object>> queueItem = new ConcurrentLinkedQueue<>();
        AtomicInteger itemThreadCount = new AtomicInteger(0);

        String teamName = codingConfig.getTeamName();
        List<TapEvent> events = new ArrayList<>();
        CodingOffset offset = (CodingOffset) (Checker.isEmpty(offsetState) ? new CodingOffset() : offsetState);
        HttpEntity<String, String> header = HttpEntity.create();
        HttpEntity<String, Object> pageBody = HttpEntity.create();
        this.defineHttpAttributes(readStartTime, readEndTime, readSize, header, pageBody, false);
        AtomicInteger total = new AtomicInteger(-1);
        Map<Object, Object> offsetMap = Optional.ofNullable(offset.offset()).orElse(new HashMap<>());
        //分页线程
        Thread pageThread = new Thread(() -> {
            int currentQueryCount = batchReadPageSize, queryIndex = (Integer) (Optional.ofNullable(offset.offset().get("PAGE_NUMBER_BATCH_READ")).orElse(1));
            while (currentQueryCount >= batchReadPageSize && this.sync()) {
                /**
                 * start page ,and add page to queuePage;
                 * */
                pageBody.builder("PageNumber", queryIndex);
                Map<String, Object> dataMap = null;
                try {
                    dataMap = this.getIssuePage(header.getEntity(), pageBody.getEntity(), String.format(CodingStarter.OPEN_API_URL, teamName));
                } catch (Exception e) {
                    offsetMap.put("PAGE_NUMBER_BATCH_READ", queryIndex);
                    throw new ErrorHttpException(e.getMessage());
                }
                if (null == dataMap || null == dataMap.get("List")) {
                    throw new RuntimeException("Paging result request failed, the Issue list is empty: " + CodingStarter.OPEN_API_URL + "?Action=DescribeIssueListWithPage");
                }
                List<Map<String, Object>> resultList = (List<Map<String, Object>>) dataMap.get("List");
                currentQueryCount = resultList.size();
                batchReadPageSize = null != dataMap.get("PageSize") ? (int) (dataMap.get("PageSize")) : batchReadPageSize;
                if (total.get() < 0) {
                    total.set((int) (dataMap.get("TotalCount")));
                }
                int finalQueryIndex = queryIndex;
                queuePage.addAll(resultList.stream().map(obj -> new AbstractMap.SimpleEntry<>((Integer) (obj.get("Code")), finalQueryIndex)).collect(Collectors.toList()));
                queryIndex++;
            }
        }, "PAGE_THREAD");
        pageThread.start();

        //详情查询线程
        while (this.sync()) {
            if (!pageThread.isAlive() && queuePage.isEmpty()) break;
            if (!queuePage.isEmpty()) {
                int threadCount = total.get() / 800;
                threadCount = Math.min(threadCount, MAX_THREAD);
                threadCount = Math.max(threadCount, 1);
                final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(threadCount + 1, (ThreadFactory) Thread::new);
                for (int i = 0; i < threadCount; i++) {
                    executor.schedule(() -> {
                        itemThreadCount.getAndAdd(1);
                        /**
                         * start page ,and add page to queuePage;
                         * */
                        try {
                            while ((!queuePage.isEmpty() || pageThread.isAlive())) {
                                synchronized (codingConnector) {
                                    if (!codingConnector.isAlive()) {
                                        break;
                                    }
                                }
                                Map.Entry<Integer, Integer> peekId = queuePage.poll();
                                if (Objects.isNull(peekId)) continue;
                                Map<String, Object> issueDetail = null;
                                try {
                                    issueDetail = this.get(IssueParam.create().issueCode(peekId.getKey()));
                                } catch (Exception e) {
                                    offsetMap.put("PAGE_NUMBER_BATCH_READ", peekId.getValue());
                                    throw new ErrorHttpException(e.getMessage());
                                }
                                if (Checker.isEmpty(issueDetail)) continue;
                                queueItem.add(issueDetail);
                            }
                        } catch (Exception e) {
                            throw e;
                        } finally {
                            itemThreadCount.getAndAdd(-1);
                        }
                    }, 1, TimeUnit.SECONDS);
                }
                break;
            }
        }
        //主线程生成事件
        while ((!queuePage.isEmpty() || pageThread.isAlive() || itemThreadCount.get() > 0 || !queueItem.isEmpty())) {
            if (!this.sync()) {
                this.connectorOut();
                break;
            }
            /**
             * 从queueItem取数据生成事件
             * **/
            if (queueItem.isEmpty()) {
                continue;
            }
            Map<String, Object> issueDetail = queueItem.poll();
            if (Checker.isEmptyCollection(issueDetail)) continue;

            Long referenceTime = (Long) issueDetail.get("CreatedAt");
            Long updateAt = (Long) issueDetail.get("UpdatedAt");
            Long currentTimePoint = referenceTime - referenceTime % (24 * 60 * 60 * 1000);//时间片段
            String issueDetailHash = this.key(issueDetail, referenceTime, updateAt);
            //issueDetial的更新时间字段值是否属于当前时间片段，并且issueDetail的hashcode是否在上一次批量读取同一时间段内
            //如果不在，说明时全新增加或修改的数据，需要在本次读取这条数据
            //如果在，说明上一次批量读取中以及读取了这条数据，本次不在需要读取 !currentTimePoint.equals(lastTimePoint) &&

            if (!lastTimeSplitIssueCode.contains(issueDetailHash)) {
                events.add(TapSimplify.insertRecordEvent(issueDetail, TABLE_NAME).referenceTime(System.currentTimeMillis()));
                //eventCount.getAndAdd(1);
                if (!currentTimePoint.equals(lastTimePoint)) {
                    lastTimePoint = currentTimePoint;
                    lastTimeSplitIssueCode = new HashSet<>();
                }
                lastTimeSplitIssueCode.add(issueDetailHash);
            }
            //else {
            //    TapLogger.warn("{} will be ignore.", issueDetailHash);
            //}
            offset.getTableUpdateTimeMap().put(TABLE_NAME, referenceTime);
            if (events.size() != readSize) continue;
            consumer.accept(events, offset);
            events = new ArrayList<>();
        }
        if (events.isEmpty()) return;
        consumer.accept(events, offset);
    }

    /**
     * 分页读取事项列表，并依次查询事项详情
     *
     * @param readStartTime
     * @param readEndTime
     * @param readSize
     * @param consumer
     */
    public void read(
            Long readStartTime,
            Long readEndTime,
            int readSize,
            CodingOffset offsetState,
            BiConsumer<List<TapEvent>, Object> consumer,
            boolean isStreamRead) {
        //if (Checker.isEmpty(offsetState)) {
        //    offsetState = new CodingOffset();
        //}
        //CodingOffset offset = (CodingOffset) offsetState;
        long readStart = System.currentTimeMillis();
        if (isStreamRead) {
            TapLogger.info(TAG, "Stream read is starting at {}. Everything be ready.", longToDateTimeStr(readStart));
        }
        int currentQueryCount = 0, queryIndex = 0;
        final List<TapEvent>[] events = new List[]{new CopyOnWriteArrayList()};
        HttpEntity<String, String> header = HttpEntity.create().builder("Authorization", this.accessToken().get());
        String projectName = this.codingConfig.getProjectName();
        HttpEntity<String, Object> pageBody = HttpEntity.create()
                .builder("Action", "DescribeIssueListWithPage")
                .builder("ProjectName", projectName)
                .builder("SortKey", "UPDATED_AT")
                .builder("PageSize", readSize)
                .builder("ShowSubIssues", true)
                .builder("SortValue", "ASC");
        if (Checker.isNotEmpty(this.codingConfig) && Checker.isNotEmpty(this.codingConfig.getIssueType())) {
            pageBody.builder("IssueType", IssueType.verifyType(this.codingConfig.getIssueType().getName()));
        } else {
            pageBody.builder("IssueType", "ALL");
        }
        List<Map<String, Object>> coditions = io.tapdata.entity.simplify.TapSimplify.list(map(
                entry("Key", "UPDATED_AT"),
                entry("Value", this.longToDateStr(readStartTime) + "_" + this.longToDateStr(readEndTime)))
        );
        String iterationCodes = this.codingConfig.getIterationCodes();
        if (null != iterationCodes && !"".equals(iterationCodes) && !",".equals(iterationCodes)) {
            if (!"-1".equals(iterationCodes)) {
                //-1时表示全选
                //String[] iterationCodeArr = iterationCodes.split(",");
                //输入的迭代编号需要验证，否则，查询事项列表时作为查询条件的迭代不存在时，查询会报错
                //选择的迭代编号不需要验证
                coditions.add(map(entry("Key", "ITERATION"), entry("Value", iterationCodes)));
            }
        }
        pageBody.builder("Conditions", coditions);
        String teamName = this.codingConfig.getTeamName();
        int totalCount = 0;
        do {
            pageBody.builder("PageNumber", queryIndex++);
            Map<String, Object> dataMap = null;
            try {
                dataMap = this.getIssuePage(header.getEntity(), pageBody.getEntity(), String.format(CodingStarter.OPEN_API_URL, teamName));
            } catch (Exception e) {
                throw new ErrorHttpException(e.getMessage());
            }
            if (null == dataMap || null == dataMap.get("List")) {
                throw new RuntimeException("Paging result request failed, the Issue list is empty: " + CodingStarter.OPEN_API_URL + "?Action=DescribeIssueListWithPage");
            }
            List<Map<String, Object>> resultList = (List<Map<String, Object>>) dataMap.get("List");
            currentQueryCount = resultList.size();
            batchReadPageSize = null != dataMap.get("PageSize") ? (int) (dataMap.get("PageSize")) : batchReadPageSize;
            for (Map<String, Object> stringObjectMap : resultList) {
                Object code = stringObjectMap.get("Code");
                if (Objects.isNull(code)) {
                    TapLogger.warn(TAG, String.format("Cannot get issue's Code from issue, issue is %s.", toJson(stringObjectMap)));
                    continue;
                }
                Map<String, Object> issueDetail = null;
                try {
                    issueDetail = this.get(IssueParam.create().issueCode((Integer) code));
                } catch (Exception e) {
                    throw new ErrorHttpException(e.getMessage());
                }
                if (Checker.isNotEmptyCollection(issueDetail)) {
                    Long referenceTime = (Long) issueDetail.get("UpdatedAt");
                    Long createdAt = (Long) issueDetail.get("CreatedAt");

                    //过滤增量时间点前的数据
                    if (referenceTime < readStartTime && createdAt < readStartTime ){
                        continue;
                    }

                    Long currentTimePoint = referenceTime - referenceTime % (24 * 60 * 60 * 1000);//时间片段
                    String issueDetailHash = this.key(issueDetail, createdAt, referenceTime);
                    //issueDetailHash的更新时间字段值是否属于当前时间片段，并且issueDetailHash的hashcode是否在上一次批量读取同一时间段内
                    //如果不在，说明时全新增加或修改的数据，需要在本次读取这条数据
                    //如果在，说明上一次批量读取中以及读取了这条数据，本次不在需要读取 !currentTimePoint.equals(lastTimePoint) &&
                    if (currentTimePoint >= this.lastTimePoint && !lastTimeSplitIssueCode.contains(issueDetailHash) ) {
                        if (referenceTime > createdAt) {
                            events[0].add(TapSimplify.updateDMLEvent(null, issueDetail, TABLE_NAME).referenceTime(referenceTime));
                        } else {
                            events[0].add(TapSimplify.insertRecordEvent(issueDetail, TABLE_NAME).referenceTime(createdAt));
                        }
                        totalCount += 1;
                        if (!currentTimePoint.equals(this.lastTimePoint)) {
                            this.lastTimePoint = currentTimePoint;
                            lastTimeSplitIssueCode = new HashSet<>();
                        }
                        lastTimeSplitIssueCode.add(issueDetailHash);
                    }
                    //if (Checker.isEmpty(offsetState)) {
                    //    offsetState = new CodingOffset();
                    //}
                    offsetState.getTableUpdateTimeMap().put(TABLE_NAME, referenceTime);
                    if (events[0].size() == readSize) {
                        consumer.accept(events[0], offsetState);
                        events[0] = new ArrayList<>();
                    }
                }
            }
        } while (currentQueryCount >= batchReadPageSize && !stopRead.get());
        if (!events[0].isEmpty()) {
            consumer.accept(events[0], offsetState);
        }
        long readEnd = System.currentTimeMillis();
        if (isStreamRead) {
            TapLogger.info(TAG,
                    totalCount > 0 ?
                            "Stream read is ending at {}, it took {} ms accumulatively to process {} issues"
                            : "Stream read is ending at {}, it took {} ms ,but {} issues were processed. Currently, no issue changes were detected.",
                    longToDateTimeStr(readEnd),
                    readEnd - readStart,
                    totalCount);
        }
    }

    public String key(Map<String, Object> issue, Long createTime, Long updateTime) {
        String code = String.valueOf(issue.get("Code"));
        String projectName = String.valueOf(issue.get("ProjectName"));
        String teamName = String.valueOf(issue.get("TeamName"));
        return code + projectName + teamName + createTime + updateTime;
    }
}
