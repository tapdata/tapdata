package io.tapdata.coding;

import cn.hutool.http.HttpRequest;
import io.tapdata.base.ConnectorBase;
import io.tapdata.coding.entity.CodingOffset;
import io.tapdata.coding.entity.ContextConfig;
import io.tapdata.coding.entity.param.Param;
import io.tapdata.coding.enums.CodingEvent;
import io.tapdata.coding.enums.IssueType;
import io.tapdata.coding.service.command.Command;
import io.tapdata.coding.service.loader.*;
import io.tapdata.coding.service.connectionMode.CSVMode;
import io.tapdata.coding.service.connectionMode.ConnectionMode;
import io.tapdata.coding.service.schema.SchemaStart;
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
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.message.CommandInfo;
import io.tapdata.pdk.apis.entity.CommandResult;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static io.tapdata.coding.enums.TapEventTypes.*;
import static io.tapdata.entity.simplify.TapSimplify.list;
import static io.tapdata.entity.simplify.TapSimplify.map;

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
		IssuesLoader.create(connectionContext).verifyConnectionConfig();
		DataMap connectionConfig = connectionContext.getConnectionConfig();
		String streamReadType = connectionConfig.getString("streamReadType");
		if (Checker.isEmpty(streamReadType)){
			throw new CoreException("Error in connection parameter [streamReadType], please go to verify");
		}
		switch (streamReadType){
			//反向赋空，如果使用webhook那么取消polling能力，如果使用polling南无取消webhook能力,一山不容二虎.--------GavinXiao
			case "WebHook":this.connectorFunctions.supportStreamRead(null);break;
			case "Polling":this.connectorFunctions.supportRawDataCallbackFilterFunction(null);break;
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
				//.supportRawDataCallbackFilterFunction(this::rawDataCallbackFilterFunction)
				.supportRawDataCallbackFilterFunctionV2(this::rawDataCallbackFilterFunction)
				.supportCommandCallbackFunction(this::handleCommand)
		;
		this.connectorFunctions = connectorFunctions;
	}
	private CommandResult handleCommand(TapConnectionContext tapConnectionContext, CommandInfo commandInfo){
		return this.handleCommandV2(tapConnectionContext, commandInfo);
	}
	private CommandResult handleCommandV1(TapConnectionContext tapConnectionContext, CommandInfo commandInfo) {
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
			TapLogger.warn(TAG,"token must be not null or not empty.");
			throw new CoreException("token must be not null or not empty.");
		}
		if (Checker.isEmpty(teamName)){
			TapLogger.warn(TAG,"teamName must be not null or not empty.");
			throw new CoreException("teamName must be not null or not empty.");
		}

		String upToken = token.toUpperCase();
		token = (upToken.startsWith("TOKEN ") ? token : "token " + token);
		HttpEntity<String,String> header = HttpEntity.create().builder("Authorization",token);
		HttpEntity<String,Object> body = IterationsLoader.create(tapConnectionContext,argMap)
				.commandSetter(command,HttpEntity.create());
		if ("DescribeIterationList".equals(command) && Checker.isNotEmpty(projectName)) {
			body.builder("ProjectName", projectName);
		}

		String url = String.format(CodingStarter.OPEN_API_URL, teamName );
		CodingHttp http = CodingHttp.create(header.getEntity(),body.getEntity(), url);
		Map<String, Object> postResult = http.post();

		Object response = postResult.get("Response");
		Map<String,Object> responseMap = (Map<String, Object>) response;
		if (Checker.isEmpty(response)){
			//TapLogger.info(TAG, "HTTP request exception, list acquisition failed: {} ", CodingStarter.OPEN_API_URL+"?Action="+command);
			throw new RuntimeException("Get list failed: " + url +"?Action="+command);
		}

		Map<String,Object> pageResult = new HashMap<>();
		Object dataObj = responseMap.get("Data");
		if(Checker.isEmpty(dataObj)){
			return new CommandResult();
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
	private CommandResult handleCommandV2(TapConnectionContext tapConnectionContext, CommandInfo commandInfo) {
		return Command.command(tapConnectionContext,commandInfo);
	}
	private List<TapEvent> rawDataCallbackFilterFunction(TapConnectorContext connectorContext, List<String> tables, Map<String, Object> issueEventData){
		return rawDataCallbackFilterFunctionV2(connectorContext,tables, issueEventData);
	}

	private List<TapEvent> rawDataCallbackFilterFunctionV2(TapConnectorContext connectorContext,List<String> tableList, Map<String, Object> issueEventData){
		//CodingLoader<Param> loader = CodingLoader.loader(connectorContext, "");
		//return Checker.isNotEmpty(loader) ? loader.rawDataCallbackFilterFunction(issueEventData) : null;
		List<CodingLoader<Param>> loaders = CodingLoader.loader(connectorContext, tableList);
		if (Checker.isNotEmpty(loaders) && !loaders.isEmpty()){
			List<TapEvent> events = new ArrayList<TapEvent>(){{
				for (CodingLoader<Param> loader : loaders) {
					List<TapEvent> tapEvents = loader.rawDataCallbackFilterFunction(issueEventData);
					if (Checker.isNotEmpty(tapEvents) && !tapEvents.isEmpty()) {
						addAll(tapEvents);
					}
				}
			}};
			return !events.isEmpty()?events:null;
		}
		return null;
	}

	private List<TapEvent> rawDataCallbackFilterFunctionV1(TapConnectorContext connectorContext, Map<String, Object> issueEventData) {
		if (Checker.isEmpty(issueEventData)) {
			TapLogger.debug(TAG, "An event with Event Data is null or empty,this callBack is stop.The data has been discarded. Data detial is:" + issueEventData);
			return null;
		}
		Object issueObj = issueEventData.get("issue");
		if (Checker.isEmpty(issueObj)) {
			TapLogger.debug(TAG, "An event with Issue Data is null or empty,this callBack is stop.The data has been discarded. Data detial is:" + issueEventData);
			return null;
		}
		String webHookEventType = String.valueOf(issueEventData.get("event"));
		Map<String, Object> issueMap = (Map<String, Object>) issueObj;
		Object codeObj = issueMap.get("code");
		if (Checker.isEmpty(codeObj)) {
			TapLogger.debug(TAG, "An event with Issue Code is be null or be empty,this callBack is stop.The data has been discarded. Data detial is:" + issueEventData);
			return null;
		}
		IssuesLoader loader = IssuesLoader.create(connectorContext);
		ContextConfig contextConfig = loader.veryContextConfigAndNodeConfig();

		IssueType issueType = contextConfig.getIssueType();
		if (Checker.isNotEmpty(issueType)) {
			String issueTypeName = issueType.getName();
			Object o = issueMap.get("type");
			if (Checker.isNotEmpty(o) && !"ALL".equals(issueTypeName) && !issueTypeName.equals(o)) {
				return null;
			}
		}
		String iterationCodes = contextConfig.getIterationCodes();
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



		//TapLogger.debug(TAG, "Start {} stream read [WebHook]", "Issues");
		TapEvent event = null;
		Object referenceTimeObj = issueMap.get("updated_at");
		Long referenceTime = null;
		if (Checker.isNotEmpty(referenceTimeObj)) {
			referenceTime = (Long) referenceTimeObj;
		}

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
		return Collections.singletonList(event);
	}

	private void streamRead(
			TapConnectorContext nodeContext,
			List<String> tableList,
			Object offsetState,
			int recordSize,
			StreamReadConsumer consumer ) {
		this.streamReadV2(nodeContext, tableList, offsetState, recordSize, consumer);
	}

	public void streamReadV1(
			TapConnectorContext nodeContext,
			List<String> tableList,
			Object offsetState,
			int recordSize,
			StreamReadConsumer consumer ){
		if (null == tableList || tableList.size()==0){
			throw new RuntimeException("tableList not Exist.");
		}

		CodingOffset codingOffset =
				null != offsetState && offsetState instanceof CodingOffset
						? (CodingOffset)offsetState : new CodingOffset();
		Map<String, Long> tableUpdateTimeMap = codingOffset.getTableUpdateTimeMap();
		if (null == tableUpdateTimeMap || tableUpdateTimeMap.isEmpty()){
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

			this.read(nodeContext, current, last, currentTable, recordSize, codingOffset, consumer,tableList.get(0));

			synchronized (this) {
				try {
					this.wait(streamExecutionGap);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
	public void streamReadV2(
			TapConnectorContext nodeContext,
			List<String> tableList,
			Object offsetState,
			int recordSize,
			StreamReadConsumer consumer ){
		//String currentTable = tableList.get(0);
		//if (null == currentTable){
		//	throw new RuntimeException("TableList is Empty or not Exist!");
		//}
		//CodingLoader<Param> loader = CodingLoader.loader(nodeContext, currentTable);
		List<CodingLoader<Param>> loaders = CodingLoader.loader(nodeContext, tableList);
		if (Checker.isEmpty(loaders) || loaders.isEmpty()) return;
		consumer.streamReadStarted();
		while (isAlive()) {
			synchronized (this) {
				try {
					this.wait(loaders.get(0).streamReadTime());
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			//if (Checker.isNotEmpty(loader)){
			//	loader.streamRead(tableList,offsetState, recordSize, consumer);
			//}
			for (CodingLoader<Param> loader : loaders) {
				loader.streamRead(tableList,offsetState, recordSize, consumer);
			}
		}
		for (CodingLoader<Param> loader : loaders) {
			loader.stopRead();
		}
	}


	private Object timestampToStreamOffset(TapConnectorContext tapConnectorContext, Long time) {
		Long date = time != null ? time: System.currentTimeMillis();
		List<SchemaStart> allSchemas = SchemaStart.getAllSchemas();
		return CodingOffset.create(allSchemas.stream().collect(Collectors.toMap(
				schema -> ((SchemaStart)schema).tableName(),
				schema -> date
		)));
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
		//this.batchReadV1(connectorContext, table, offset, batchCount, consumer);
		this.batchReadV2(connectorContext, table, offset, batchCount, consumer);
	}

	private void batchReadV1(
			TapConnectorContext connectorContext,
			TapTable table,
			Object offset,
			int batchCount,
			BiConsumer<List<TapEvent>, Object> consumer) {
		//TapLogger.debug(TAG, "start {} batch read", table.getName());
		Long readEnd = System.currentTimeMillis();
		CodingOffset codingOffset =  new CodingOffset();
		//current read end as next read begin
		codingOffset.setTableUpdateTimeMap(new HashMap<String,Long>(){{ put(table.getId(),readEnd);}});
		IssuesLoader.create(connectorContext).verifyConnectionConfig();
		this.read(connectorContext,null,readEnd,table.getId(),batchCount,codingOffset,consumer,table.getId());
		//TapLogger.debug(TAG, "compile {} batch read", table.getName());
	}
	public void batchReadV2(TapConnectorContext connectorContext,
						  TapTable table,
						  Object offset,
						  int batchCount,
						  BiConsumer<List<TapEvent>, Object> consumer){
		CodingLoader<Param> loader = CodingLoader.loader(connectorContext, table.getId());
		if (Checker.isNotEmpty(loader)) {
			loader.batchRead(offset, batchCount, consumer);
		}
	}

	private long batchCount(TapConnectorContext tapConnectorContext, TapTable tapTable) throws Throwable {
		return this.batchCountV2(tapConnectorContext, tapTable);
	}
	private long batchCountV2(TapConnectorContext tapConnectorContext, TapTable tapTable) throws Throwable {
		CodingLoader<Param> loader = CodingLoader.loader(tapConnectorContext, tapTable.getId());
		if (Checker.isNotEmpty(loader)) {
			int count = loader.batchCount();
			return Long.parseLong(String.valueOf(count));
		}
		TapLogger.debug(TAG, "batchCountV2 = 0",tapTable.getId());
		return 0L;
	}
	private long batchCountV1(TapConnectorContext tapConnectorContext, TapTable tapTable) throws Throwable {
		long count = 0;
		IssuesLoader issuesLoader = IssuesLoader.create(tapConnectorContext);
		issuesLoader.verifyConnectionConfig();
		try {
			DataMap connectionConfig = tapConnectorContext.getConnectionConfig();
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
			Map<String,Object> dataMap = issuesLoader.getIssuePage(
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
		return 3;
//		List<SchemaStart> allSchemas = SchemaStart.getAllSchemas();
//		return allSchemas.size();
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
		IssuesLoader issuesLoader = IssuesLoader.create(nodeContext);
		ContextConfig contextConfig = issuesLoader.veryContextConfigAndNodeConfig();

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
				entry("Value", issuesLoader.longToDateStr(readStartTime)+"_"+ issuesLoader.longToDateStr(readEndTime)))
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
			Map<String,Object> dataMap = issuesLoader.getIssuePage(header.getEntity(),pageBody.getEntity(),String.format(CodingStarter.OPEN_API_URL,teamName));
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
					events[0].add(insertRecordEvent(issueDetail, readTable).referenceTime(System.currentTimeMillis()));

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
	public void readV2(
			TapConnectorContext nodeContext,
			Long readStartTime,
			Long readEndTime,
			String readTable,
			int readSize,
			Object offsetState,
			BiConsumer<List<TapEvent>, Object> consumer,
			String table ){

	}
}
