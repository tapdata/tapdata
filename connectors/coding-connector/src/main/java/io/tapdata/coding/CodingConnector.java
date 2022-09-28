package io.tapdata.coding;

import cn.hutool.core.date.DateUtil;
import cn.hutool.http.HttpRequest;
import cn.hutool.json.JSONUtil;
import io.tapdata.base.ConnectorBase;
import io.tapdata.coding.entity.CodingOffset;
import io.tapdata.coding.entity.ContextConfig;
import io.tapdata.coding.enums.CodingEvent;
import io.tapdata.coding.enums.Constants;
import io.tapdata.coding.enums.IssueEventTypes;
import io.tapdata.coding.enums.IssueType;
import io.tapdata.coding.service.CodingStarter;
import io.tapdata.coding.service.IssueLoader;
import io.tapdata.coding.service.IterationLoader;
import io.tapdata.coding.service.TestCoding;
import io.tapdata.coding.service.connectionMode.CSVMode;
import io.tapdata.coding.service.connectionMode.ConnectionMode;
import io.tapdata.coding.utils.collection.MapUtil;
import io.tapdata.coding.utils.http.CodingHttp;
import io.tapdata.coding.utils.http.HttpEntity;
import io.tapdata.coding.utils.tool.Checker;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.Entry;
import io.tapdata.kit.StringKit;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.CommandInfo;
import io.tapdata.pdk.apis.entity.CommandResult;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.tapdata.coding.enums.IssueEventTypes.*;
import static io.tapdata.entity.simplify.TapSimplify.list;
import static io.tapdata.entity.simplify.TapSimplify.map;
import static io.tapdata.entity.utils.JavaTypesToTapTypes.*;

@TapConnectorClass("spec.json")
public class CodingConnector extends ConnectorBase {
	private static final String TAG = CodingConnector.class.getSimpleName();

	private final Object streamReadLock = new Object();
	private final long streamExecutionGap = 5000;//util: ms
	private int batchReadPageSize = 500;//coding page 1~500,

	private Long lastTimePoint;
	private List<Integer> lastTimeSplitIssueCode = new ArrayList<>();//hash code list

	@Override
	public void onStart(TapConnectionContext connectionContext) throws Throwable {
		//isConnectorStarted(connectionContext, connectorContext -> {});
		IssueLoader.create(connectionContext).verifyConnectionConfig();
		DataMap connectionConfig = connectionContext.getConnectionConfig();
		String streamReadType = connectionConfig.getString("streamReadType");
		if (Checker.isEmpty(streamReadType)){
			throw new CoreException("Error in connection parameter [streamReadType], please go to verify");
		}
		switch (streamReadType){
			case "Polling":this.connectorFunctions.supportStreamRead(null);break;
			case "WebHook":this.connectorFunctions.supportRawDataCallbackFilterFunction(null);break;
//			default:
//				throw new CoreException("Error in connection parameters [streamReadType],just be [WebHook] or [Polling], please go to verify");
		}
	}

	@Override
	public void onStop(TapConnectionContext connectionContext) throws Throwable {
		synchronized (this) {
			this.notify();
		}
		TapLogger.debug(TAG, "Stop connector");
	}

	private ConnectorFunctions connectorFunctions;
	@Override
	public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
		connectorFunctions.supportBatchRead(this::batchRead)
				.supportBatchCount(this::batchCount)
				.supportTimestampToStreamOffset(this::timestampToStreamOffset)
				.supportStreamRead(this::streamRead)
				.supportRawDataCallbackFilterFunction(this::rawDataCallbackFilterFunction)
				.supportCommandCallbackFunction(this::handleCommand)
		;
		this.connectorFunctions = connectorFunctions;
	}

	private CommandResult handleCommand(TapConnectionContext tapConnectionContext, CommandInfo commandInfo) {
		String command = commandInfo.getCommand();
		if(Checker.isEmpty(command)){
			throw new CoreException("Command can not be NULL or not be empty.");
		}

		String action = commandInfo.getAction();
		Map<String, Object> argMap = commandInfo.getArgMap();
		String token = null;
		String teamName = null;
		String projectName = null;

		Map<String,Object> connectionConfig = commandInfo.getConnectionConfig();
		if (Checker.isEmpty(connectionConfig)){
			throw new IllegalArgumentException("ConnectionConfig cannot be null");
		}

		Object tokenObj = connectionConfig.get("token");
		Object teamNameObj = connectionConfig.get("teamName");
		if (Checker.isNotEmpty(tokenObj)){
			token = (tokenObj instanceof String)?(String) tokenObj : String.valueOf(tokenObj);
		}
		if (Checker.isNotEmpty(teamNameObj)){
			teamName = (teamNameObj instanceof String)?(String) teamNameObj : String.valueOf(teamNameObj);
		}

		Object projectNameObj = connectionConfig.get("projectName");
		if (Checker.isNotEmpty(projectNameObj)){
			projectName = (projectNameObj instanceof String)?(String) projectNameObj : String.valueOf(projectNameObj);
		}
		if ("DescribeIterationList".equals(command) && Checker.isEmpty(projectName)){
			throw new CoreException("ProjectName must be not Empty or not null.");
		}
		if (Checker.isEmpty(token)){
			throw new CoreException("token must be not null or not empty.");
		}
		if (Checker.isEmpty(teamName)){
			throw new CoreException("teamName must be not null or not empty.");
		}

		HttpEntity<String,String> header = HttpEntity.create().builder("Authorization",token);
		HttpEntity<String,Object> body = IterationLoader.create(tapConnectionContext,argMap)
				.commandSetter(command,HttpEntity.create());
		if ("DescribeIterationList".equals(command) && Checker.isNotEmpty(projectName)) {
			body.builder("ProjectName", projectName);
		}

		CodingHttp http = CodingHttp.create(header.getEntity(),body.getEntity(), String.format(CodingStarter.OPEN_API_URL, teamName ));
		Map<String, Object> postResult = http.post();

		Object response = postResult.get("Response");
		Map<String,Object> responseMap = (Map<String, Object>) response;
		if (Checker.isEmpty(response)){
			//TapLogger.info(TAG, "HTTP request exception, list acquisition failed: {} ", CodingStarter.OPEN_API_URL+"?Action="+command);
			throw new RuntimeException("Get list failed: " + CodingStarter.OPEN_API_URL+"?Action="+command);
		}

		Map<String,Object> pageResult = new HashMap<>();
		Object dataObj = responseMap.get("Data");
		if(Checker.isEmpty(dataObj)){
			Object errorObj = responseMap.get("Error");
			String message = "";
			if (Checker.isNotEmpty(errorObj)){
				message = String.valueOf(((Map<String,Object>)errorObj).get("Message"));
			}
			if ("ProjectIssueNotInit".equals(message)) {
				throw new CoreException(" " );
			}else {
				throw new CoreException("Project list is empty, please ensure your params are correct: " + message);
			}
		}
		Map<String,Object> data = (Map<String,Object>)dataObj;
		if ("DescribeIterationList".equals(command)){
			Object listObj = data.get("List");
			List<Map<String,Object>> searchList = new ArrayList<>();
			if (Checker.isNotEmpty(listObj)){
				searchList = (List<Map<String,Object>>)listObj;
			}
			Integer page = Checker.isEmpty(data.get("Page"))?0:Integer.parseInt(data.get("Page").toString());
			Integer size = Checker.isEmpty(data.get("PageSize"))?0:Integer.parseInt(data.get("PageSize").toString());
			Integer total = Checker.isEmpty(data.get("TotalPage"))?0:Integer.parseInt(data.get("TotalPage").toString());
			Integer rows = Checker.isEmpty(data.get("TotalRow"))?0:Integer.parseInt(data.get("TotalRow").toString());
			pageResult.put("page",page);
			pageResult.put("size",size);
			pageResult.put("total",total);
			pageResult.put("rows",rows);
			List<Map<String,Object>> resultList = new ArrayList<>();
			searchList.forEach(map->{
				resultList.add(map(entry("label",map.get("Name")),entry("value",map.get("Code"))));
			});
			pageResult.put("items",resultList);
		}else if("DescribeCodingProjects".equals(command)){
			Object listObj = data.get("ProjectList");
			List<Map<String,Object>> searchList = new ArrayList<>();
			if (Checker.isNotEmpty(listObj)){
				searchList = (List<Map<String,Object>>)listObj;
			}
			Integer page = Checker.isEmpty(data.get("PageNumber"))?0:Integer.parseInt(data.get("PageNumber").toString());
			Integer size = Checker.isEmpty(data.get("PageSize"))?0:Integer.parseInt(data.get("PageSize").toString());
			Integer total = Checker.isEmpty(data.get("TotalCount"))?0:Integer.parseInt(data.get("TotalCount").toString());
			pageResult.put("page",page);
			pageResult.put("size",size);
			pageResult.put("total",total);
			List<Map<String,Object>> resultList = new ArrayList<>();
			searchList.forEach(map->{
				resultList.add(map(entry("label",map.get("DisplayName")),entry("value",map.get("Name"))));
			});
			pageResult.put("items",resultList);
		}else {
			throw new CoreException("Command only support [DescribeIterationList] or [DescribeCodingProjects].");
		}
		return new CommandResult().result(pageResult);
	}

	private TapEvent rawDataCallbackFilterFunction(TapConnectorContext connectorContext, Map<String, Object> issueEventData) {
		if (Checker.isEmpty(issueEventData)){
			TapLogger.debug(TAG,"An event with Event Data is null or empty,this callBack is stop.The data has been discarded. Data detial is:"+issueEventData);
			return null;
		}
		Object issueObj = issueEventData.get("issue");
		if (Checker.isEmpty(issueObj)){
			TapLogger.debug(TAG,"An event with Issue Data is null or empty,this callBack is stop.The data has been discarded. Data detial is:"+issueEventData);
			return null;
		}
		String webHookEventType = String.valueOf(issueEventData.get("event"));
		Map<String, Object> issueMap = (Map<String, Object>)issueObj;
		Object codeObj = issueMap.get("code");
		if (Checker.isEmpty(codeObj)){
			TapLogger.debug(TAG,"An event with Issue Code is be null or be empty,this callBack is stop.The data has been discarded. Data detial is:"+issueEventData);
			return null;
		}
		IssueLoader loader = IssueLoader.create(connectorContext);
		ContextConfig contextConfig = loader.veryContextConfigAndNodeConfig();

		IssueType issueType = contextConfig.getIssueType();
		if (Checker.isNotEmpty(issueType)){
			String issueTypeName = issueType.getName();
			Object o = issueMap.get("type");
			if (Checker.isNotEmpty(o) && !"ALL".equals(issueTypeName) && !issueTypeName.equals(o)){
				return null;
			}
		}
		String iterationCodes = contextConfig.getIterationCodes();
		if (Checker.isNotEmpty(iterationCodes)){
			Object iterationObj = issueMap.get("iteration");
			if (Checker.isNotEmpty(iterationObj)) {
				Object iteration = ((Map<String, Object>) iterationObj).get("code");
				if (Checker.isNotEmpty(iteration) && !iterationCodes.matches(".*"+String.valueOf(iteration)+".*")) {
					return null;
				}
			}
		}

		//TapLogger.debug(TAG, "Start {} stream read [WebHook]", "Issues");
		TapEvent event = null;
		long referenceTime = System.currentTimeMillis();
		CodingEvent issueEvent = CodingEvent.event(webHookEventType);

		Map<String, Object> issueDetail = null;
		if (!DELETED_EVENT.equals(issueEvent)) {
			HttpEntity<String, String> header = HttpEntity.create().builder("Authorization", contextConfig.getToken());
			HttpEntity<String, Object> issueDetialBody = HttpEntity.create()
					.builder("Action", "DescribeIssue")
					.builder("ProjectName", contextConfig.getProjectName());
			CodingHttp authorization = CodingHttp.create(header.getEntity(), String.format(CodingStarter.OPEN_API_URL, contextConfig.getTeamName()));
			HttpRequest requestDetail = authorization.createHttpRequest();

			issueDetail = loader.readIssueDetail(
							issueDetialBody,
							authorization,
							requestDetail,
							(codeObj instanceof Integer) ? (Integer) codeObj : Integer.parseInt(codeObj.toString()),
							contextConfig.getProjectName(),
							contextConfig.getTeamName());
			String modeName = connectorContext.getConnectionConfig().getString("connectionMode");
			ConnectionMode instance = ConnectionMode.getInstanceByName(connectorContext, modeName);
			if (null == instance){
				throw new CoreException("Connection Mode is not empty or not null.");
			}
			if (instance instanceof CSVMode) {
				issueDetail = instance.attributeAssignment(issueDetail);
			}
		}
		if (Checker.isNotEmpty(issueEvent)){
			String evenType = issueEvent.getEventType();
			switch (evenType){
				case DELETED_EVENT:{
					issueDetail = (Map<String, Object>) issueObj;
					issueDetail.put("teamName",contextConfig.getTeamName());
					issueDetail.put("projectName",contextConfig.getProjectName());
					event = deleteDMLEvent(issueDetail, "Issues").referenceTime(referenceTime)  ;
				};break;
				case UPDATE_EVENT:{
					event = updateDMLEvent(null,issueDetail, "Issues").referenceTime(referenceTime) ;
				};break;
				case CREATED_EVENT:{
					event = insertRecordEvent(issueDetail, "Issues").referenceTime(referenceTime)  ;
				};break;
			}
			//TapLogger.debug(TAG, "End {} stream read [WebHook]", "Issues");
		}else {
			TapLogger.debug(TAG,"An event type with unknown origin was found and cannot be processed - ["+event+"]. The data has been discarded. Data to be processed:"+issueDetail);
		}
		return event;
	}

	private void streamRead(
			TapConnectorContext nodeContext,
			List<String> tableList,
			Object offsetState,
			int recordSize,
			StreamReadConsumer consumer ) {
		if (null == tableList || tableList.size()==0){
			throw new RuntimeException("tableList not Exist.");
		}

		CodingOffset codingOffset =
				null != offsetState && offsetState instanceof CodingOffset
						? (CodingOffset)offsetState : new CodingOffset();
		Map<String, Long> tableUpdateTimeMap = codingOffset.getTableUpdateTimeMap();
		if (null == tableUpdateTimeMap || tableUpdateTimeMap.size()==0){
			TapLogger.warn(TAG,"offsetState is Empty or not Exist!");
			return;
		}

		String currentTable = tableList.get(0);
		if (null == currentTable){
			throw new RuntimeException("TableList is Empty or not Exist!");
		}

		consumer.streamReadStarted();
		while (isAlive()) {
			long current = tableUpdateTimeMap.get(tableList.get(0));
			Long last = Long.MAX_VALUE;
			//TapLogger.debug(TAG, "start {} stream read [Polling]", currentTable);
			this.read(nodeContext, current, last, currentTable, recordSize, codingOffset, consumer,tableList.get(0));
			//TapLogger.debug(TAG, "compile {} once stream read [Polling]", currentTable);
			synchronized (this) {
				try {
					this.wait(streamExecutionGap);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}


	private Object timestampToStreamOffset(TapConnectorContext tapConnectorContext, Long time) {
		Long date = time != null ? time: System.currentTimeMillis();
		return CodingOffset.create(new HashMap<String,Long>(){{put("Issues", date);}});
	}

	/**
	 * Batch read
	 * @auth GavinX
	 * @param connectorContext
	 * @param table
	 * @param offset
	 * @param batchCount
	 * @param consumer
	 */
	private void batchRead(
			TapConnectorContext connectorContext,
			TapTable table,
			Object offset,
			int batchCount,
			BiConsumer<List<TapEvent>, Object> consumer) {
		Long readEnd = System.currentTimeMillis();
		CodingOffset codingOffset =  new CodingOffset();
		//current read end as next read begin
		codingOffset.setTableUpdateTimeMap(new HashMap<String,Long>(){{ put(table.getId(),readEnd);}});
		IssueLoader.create(connectorContext).verifyConnectionConfig();
		DataMap connectionConfig = connectorContext.getConnectionConfig();
		String projectName = connectionConfig.getString("projectName");
		String token = connectionConfig.getString("token");
		String teamName = connectionConfig.getString("teamName");
		this.read(connectorContext,null,readEnd,table.getId(),batchCount,codingOffset,consumer,table.getId());
	}

	private long batchCount(TapConnectorContext tapConnectorContext, TapTable tapTable) throws Throwable {
		long count = 0;
		IssueLoader issueLoader = IssueLoader.create(tapConnectorContext);
		issueLoader.verifyConnectionConfig();
		try {
			DataMap connectionConfig = tapConnectorContext.getConnectionConfig();

			HttpEntity<String,String> header = HttpEntity.create()
					.builder("Authorization",connectionConfig.getString("token"));
			HttpEntity<String,Object> body = HttpEntity.create()
					.builder("Action",       "DescribeIssueListWithPage")
					.builder("ProjectName",  connectionConfig.getString("projectName"))
					.builder("PageSize",     1)
					.builder("PageNumber",   1);
			try {
				DataMap nodeConfigMap = tapConnectorContext.getNodeConfig();

				String iterationCodes = nodeConfigMap.getString("DescribeIterationList");//iterationCodes
				if (null != iterationCodes) iterationCodes = iterationCodes.trim();
				String issueType = nodeConfigMap.getString("issueType");
				if (null != issueType ) issueType = issueType.trim();

				body.builder("IssueType",    IssueType.verifyType(issueType));

				if (null != iterationCodes && !"".equals(iterationCodes) && !",".equals(iterationCodes) && !"-1".equals(iterationCodes)){
					//String[] iterationCodeArr = iterationCodes.split(",");
					//@TODO 输入的迭代编号需要验证，否则，查询事项列表时作为查询条件的迭代不存在时，查询会报错
					//选择的迭代编号不需要验证
					body.builder("Conditions",list(map(entry("Key","ITERATION"),entry("Value",iterationCodes))));
				}
			}catch (Exception e){
				TapLogger.debug(TAG,"Count table error: {}" ,tapTable.getName(), e.getMessage());
			}
			Map<String,Object> dataMap = issueLoader.getIssuePage(
					header.getEntity(),
					body.getEntity(),
					String.format(CodingStarter.OPEN_API_URL,connectionConfig.getString("teamName"))
			);
			if (null!= dataMap){
				Object obj = dataMap.get("TotalCount");
				if (null != obj ) count = Long.parseLong(String.valueOf(obj));
			}
		} catch (Exception e) {
			throw new RuntimeException();
		}
		TapLogger.debug(TAG,"Batch count is " + count);
		return count;
	}

	@Override
	public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
		//return TapTable for each project. Issue
		//IssueLoader.create(connectionContext).setTableSize(tableSize).discoverIssue(tables,consumer);
		String modeName = connectionContext.getConnectionConfig().getString("connectionMode");
		ConnectionMode connectionMode = ConnectionMode.getInstanceByName(connectionContext, modeName);
		if (null == connectionMode){
			throw new CoreException("Connection Mode is not empty or not null.");
		}
		List<TapTable> tapTables = connectionMode.discoverSchema(tables, tableSize);
		if (null != tapTables){
			consumer.accept(tapTables);
		}
	}

	@Override
	public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) throws Throwable {
		ConnectionOptions connectionOptions = ConnectionOptions.create();

		TestCoding testConnection= TestCoding.create(connectionContext);
		TestItem testItem = testConnection.testItemConnection();
		consumer.accept(testItem);
		if (testItem.getResult()==TestItem.RESULT_FAILED){
			return connectionOptions;
		}

		TestItem testToken = testConnection.testToken();
		consumer.accept(testToken);
		if (testToken.getResult()==TestItem.RESULT_FAILED){
			return connectionOptions;
		}

		TestItem testProject = testConnection.testProject();
		consumer.accept(testProject);
		return connectionOptions;
	}

	@Override
	public int tableCount(TapConnectionContext connectionContext) throws Throwable {
		//check how many projects
		return 1;
	}

	/**
	 * 分页读取事项列表，并依次查询事项详情
	 * @param nodeContext
	 * @param readStartTime
	 * @param readEndTime
	 * @param readTable
	 * @param readSize
	 * @param consumer
	 */
	public void read(
			TapConnectorContext nodeContext,
			Long readStartTime,
			Long readEndTime,
			String readTable,
			int readSize,
			Object offsetState,
			BiConsumer<List<TapEvent>, Object> consumer,
			String table ){
		IssueLoader issueLoader = IssueLoader.create(nodeContext);
		ContextConfig contextConfig = issueLoader.veryContextConfigAndNodeConfig();

		int currentQueryCount = 0,queryIndex = 0 ;
		final List<TapEvent>[] events = new List[]{new ArrayList<>()};
		HttpEntity<String,String> header = HttpEntity.create().builder("Authorization",contextConfig.getToken());
		String projectName = contextConfig.getProjectName();
		HttpEntity<String,Object> pageBody = HttpEntity.create()
				.builder("Action","DescribeIssueListWithPage")
				.builder("ProjectName",projectName)
				.builder("SortKey","UPDATED_AT")
				.builder("PageSize",readSize)
				.builder("SortValue","ASC");
		if (Checker.isNotEmpty(contextConfig) && Checker.isNotEmpty(contextConfig.getIssueType())){
			pageBody.builder("IssueType",IssueType.verifyType(contextConfig.getIssueType().getName()));
		}else {
			pageBody.builder("IssueType","ALL");
		}
		List<Map<String,Object>> coditions = list(map(
				entry("Key","UPDATED_AT"),
				entry("Value",issueLoader.longToDateStr(readStartTime)+"_"+issueLoader.longToDateStr(readEndTime)))
		);
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
		String modeName = contextConfig.getConnectionMode();
		ConnectionMode instance = ConnectionMode.getInstanceByName(nodeContext,modeName);
		if (null == instance){
			throw new CoreException("Connection Mode is not empty or not null.");
		}
		do{
			pageBody.builder("PageNumber",queryIndex++);
			Map<String,Object> dataMap = issueLoader.getIssuePage(header.getEntity(),pageBody.getEntity(),String.format(CodingStarter.OPEN_API_URL,teamName));
			if (null == dataMap || null == dataMap.get("List")) {
				TapLogger.error(TAG, "Paging result request failed, the Issue list is empty: page index = {}",queryIndex);
				throw new RuntimeException("Paging result request failed, the Issue list is empty: "+CodingStarter.OPEN_API_URL+"?Action=DescribeIssueListWithPage");
			}
			List<Map<String,Object>> resultList = (List<Map<String,Object>>) dataMap.get("List");
			currentQueryCount = resultList.size();
			batchReadPageSize = null != dataMap.get("PageSize") ? (int)(dataMap.get("PageSize")) : batchReadPageSize;
			for (Map<String, Object> stringObjectMap : resultList) {
				Map<String,Object> issueDetail = instance.attributeAssignment(stringObjectMap);
				Long referenceTime = (Long)issueDetail.get("UpdatedAt");
				Long currentTimePoint = referenceTime - referenceTime % (24*60*60*1000);//时间片段
				Integer issueDetialHash = MapUtil.create().hashCode(issueDetail);

				//issueDetial的更新时间字段值是否属于当前时间片段，并且issueDiteal的hashcode是否在上一次批量读取同一时间段内
				//如果不在，说明时全新增加或修改的数据，需要在本次读取这条数据
				//如果在，说明上一次批量读取中以及读取了这条数据，本次不在需要读取 !currentTimePoint.equals(lastTimePoint) &&
				if (!lastTimeSplitIssueCode.contains(issueDetialHash)) {
					events[0].add(insertRecordEvent(issueDetail, readTable).referenceTime(referenceTime));

					if (null == currentTimePoint || !currentTimePoint.equals(this.lastTimePoint)){
						this.lastTimePoint = currentTimePoint;
						lastTimeSplitIssueCode = new ArrayList<Integer>();
					}
					lastTimeSplitIssueCode.add(issueDetialHash);
				}

				((CodingOffset)offsetState).getTableUpdateTimeMap().put(table,referenceTime);
				if (events[0].size() == readSize) {
					consumer.accept(events[0], offsetState);
					events[0] = new ArrayList<>();
				}
			}
		}while (currentQueryCount >= batchReadPageSize);
		if (events[0].size() > 0)  consumer.accept(events[0], offsetState);
	}
}
