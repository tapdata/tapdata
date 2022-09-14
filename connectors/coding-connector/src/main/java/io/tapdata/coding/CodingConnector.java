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

//	private RecordStream recordStream;
//	private boolean loadBatchFirstRecord;
//	private int size;
//	public CodingConnector(){
//		this.recordStream = new RecordStream(Constants.CACHE_BUFFER_SIZE, Constants.CACHE_BUFFER_COUNT);
//		loadBatchFirstRecord = Boolean.TRUE;
//		this.size = 0;
//	}
	@Override
	public void onStart(TapConnectionContext connectionContext) throws Throwable {

	}

	@Override
	public void onStop(TapConnectionContext connectionContext) throws Throwable {
		synchronized (this) {
			this.notify();
		}
		TapLogger.info(TAG, "Stop connector");
	}

	@Override
	public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
		connectorFunctions.supportBatchRead(this::batchRead)
				.supportBatchCount(this::batchCount)
				.supportTimestampToStreamOffset(this::timestampToStreamOffset)
				.supportStreamRead(this::streamRead)
				.supportRawDataCallbackFilterFunction(this::rawDataCallbackFilterFunction)
				.supportCommandCallbackFunction(this::handleCommand)
		;

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
			TapLogger.warn(TAG,"token must be not null or not empty.");
			throw new CoreException("token must be not null or not empty.");
		}
		if (Checker.isEmpty(teamName)){
			TapLogger.warn(TAG,"teamName must be not null or not empty.");
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
			TapLogger.info(TAG, "HTTP request exception, list acquisition failed: {} ", CodingStarter.OPEN_API_URL+"?Action="+command);
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
				resultList.add(map(entry("label",map.get("Name")),entry("value",map.get("Name"))));
			});
			pageResult.put("items",resultList);
		}else {
			throw new CoreException("Command only support [DescribeIterationList] or [DescribeCodingProjects].");
		}
		return new CommandResult().result(pageResult);
	}

	private TapEvent rawDataCallbackFilterFunction(TapConnectorContext connectorContext, Map<String, Object> issueEventData) {
		if (Checker.isEmpty(issueEventData)){
			TapLogger.warn(TAG,"An event with Event Data is null or empty,this callBack is stop.The data has been discarded. Data detial is:"+issueEventData);
			return null;
		}
		Object issueObj = issueEventData.get("issue");
		if (Checker.isEmpty(issueObj)){
			TapLogger.warn(TAG,"An event with Issue Data is null or empty,this callBack is stop.The data has been discarded. Data detial is:"+issueEventData);
			return null;
		}
		String webHookEventType = String.valueOf(issueEventData.get("event"));
		Object codeObj = ((Map<String, Object>)issueObj).get("code");
		if (Checker.isEmpty(codeObj)){
			TapLogger.warn(TAG,"An event with Issue Code is be null or be empty,this callBack is stop.The data has been discarded. Data detial is:"+issueEventData);
			return null;
		}
		IssueLoader loader = IssueLoader.create(connectorContext);
		ContextConfig contextConfig = loader.veryContextConfigAndNodeConfig();


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
			ConnectionMode instance = ConnectionMode.getInstanceByName(modeName);
			if (null == instance){
				throw new CoreException("Connection Mode is not empty or not null.");
			}
			if (instance instanceof CSVMode) {
				issueDetail = instance.attributeAssignment(connectorContext, issueDetail);
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
		}else {
			TapLogger.warn(TAG,"An event type with unknown origin was found and cannot be processed - ["+event+"]. The data has been discarded. Data to be processed:"+issueDetail);
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
			TapLogger.info(TAG, "start {} stream read", currentTable);
			this.read(nodeContext, current, last, currentTable, recordSize, codingOffset, consumer,tableList.get(0));
			TapLogger.info(TAG, "compile {} once stream read", currentTable);
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
		TapLogger.info(TAG, "start {} batch read", table.getName());
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
		TapLogger.info(TAG, "compile {} batch read", table.getName());
	}

	private long batchCount(TapConnectorContext tapConnectorContext, TapTable tapTable) throws Throwable {
		long count = 0;
		IssueLoader issueLoader = IssueLoader.create(tapConnectorContext);
		issueLoader.verifyConnectionConfig();
		try {
			DataMap connectionConfig = tapConnectorContext.getConnectionConfig();
			DataMap nodeConfigMap = tapConnectorContext.getNodeConfig();

			String iterationCodes = nodeConfigMap.getString("DescribeIterationList");//iterationCodes
			if (null != iterationCodes) iterationCodes = iterationCodes.trim();
			String issueType = nodeConfigMap.getString("issueType");
			if (null != issueType ) issueType = issueType.trim();

			HttpEntity<String,String> header = HttpEntity.create()
				.builder("Authorization",connectionConfig.getString("token"));
			HttpEntity<String,Object> body = HttpEntity.create()
				.builder("Action",       "DescribeIssueListWithPage")
				.builder("ProjectName",  connectionConfig.getString("projectName"))
				.builder("PageSize",     1)
				.builder("IssueType",    IssueType.verifyType(issueType))
				.builder("PageNumber",   1);
			if (null != iterationCodes && !"".equals(iterationCodes)){
				//String[] iterationCodeArr = iterationCodes.split(",");
				//@TODO 输入的迭代编号需要验证，否则，查询事项列表时作为查询条件的迭代不存在时，查询会报错
				//选择的迭代编号不需要验证
				body.builder("Conditions",list(map(entry("Key","ITERATION"),entry("Value",iterationCodes))));
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
			throw new RuntimeException("Count table " + tapTable.getName() + " error: " + e.getMessage(), e);
		}
		return count;
	}

	@Override
	public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
		//return TapTable for each project. Issue
		//IssueLoader.create(connectionContext).setTableSize(tableSize).discoverIssue(tables,consumer);

		String modeName = connectionContext.getConnectionConfig().getString("connectionMode");
		ConnectionMode connectionMode = ConnectionMode.getInstanceByName(modeName);
		if (null == connectionMode){
			throw new CoreException("Connection Mode is not empty or not null.");
		}
		List<TapTable> tapTables = connectionMode.discoverSchema(connectionContext, tables, tableSize);

		if (null != tapTables){
			consumer.accept(tapTables);
		}

//		if(tables == null || tables.isEmpty()) {
//			consumer.accept(list(
//					table("Issues")
//							.add(field("Code", JAVA_Integer).isPrimaryKey(true).primaryKeyPos(3))        //事项 Code
//							.add(field("ProjectName", "StringMinor").isPrimaryKey(true).primaryKeyPos(2))   //项目名称
//							.add(field("TeamName", "StringMinor").isPrimaryKey(true).primaryKeyPos(1))      //团队名称
//							.add(field("ParentType", "StringMinor"))                                       //父事项类型
//							.add(field("Type", "StringMinor"))                                         //事项类型：DEFECT - 缺陷;REQUIREMENT - 需求;MISSION - 任务;EPIC - 史诗;SUB_TASK - 子工作项
//
//
//							.add(field("IssueTypeDetailId", JAVA_Integer))                               //事项类型ID
//							.add(field("IssueTypeDetail", JAVA_Map))                                         //事项类型具体信息
//							.add(field("Name", "StringMinor"))                                              //名称
//							.add(field("Description", "StringLonger"))                                       //描述
//							.add(field("IterationId", JAVA_Integer))                                     //迭代 Id
//							.add(field("IssueStatusId", JAVA_Integer))                                   //事项状态 Id
//							.add(field("IssueStatusName", "StringMinor"))                                   //事项状态名称
//							.add(field("IssueStatusType", "StringMinor"))                                   //事项状态类型
//							.add(field("Priority", "StringBit"))                                          //优先级:"0" - 低;"1" - 中;"2" - 高;"3" - 紧急;"" - 未指定
//
//							.add(field("AssigneeId", JAVA_Integer))                                      //Assignee.Id 等于 0 时表示未指定
//							.add(field("Assignee", JAVA_Map))                                                //处理人
//							.add(field("StartDate", JAVA_Long))                                             //开始日期时间戳
//							.add(field("DueDate", JAVA_Long))                                               //截止日期时间戳
//							.add(field("WorkingHours", "WorkingHours"))                                      //工时（小时）
//
//							.add(field("CreatorId", JAVA_Integer))                                       //创建人Id
//							.add(field("Creator", JAVA_Map))                                                 //创建人
//							.add(field("StoryPoint", "StringMinor"))                                        //故事点
//							.add(field("CreatedAt", JAVA_Long))                                             //创建时间
//							.add(field("UpdatedAt", JAVA_Long))                                             //修改时间
//							.add(field("CompletedAt", JAVA_Long))                                           //完成时间
//
//							.add(field("ProjectModuleId", JAVA_Integer))                                 //ProjectModule.Id 等于 0 时表示未指定
//							.add(field("ProjectModule", JAVA_Map))                                           //项目模块
//
//							.add(field("WatcherIdArr", JAVA_Array))                                        //关注人Id列表
//							.add(field("Watchers", JAVA_Array))                                            //关注人
//
//							.add(field("LabelIdArr", JAVA_Array))                                          //标签Id列表
//							.add(field("Labels", JAVA_Array))                                              //标签列表
//
//							.add(field("FileIdArr", JAVA_Array))                                           //附件Id列表
//							.add(field("Files", JAVA_Array))                                               //附件列表
//							.add(field("RequirementType", "StringSmaller"))                                   //需求类型
//
//							.add(field("DefectType", JAVA_Map))                                              //缺陷类型
//							.add(field("CustomFields", JAVA_Array))                                        //自定义字段列表
//							.add(field("ThirdLinks", JAVA_Array))                                          //第三方链接列表
//
//							.add(field("SubTaskCodeArr", JAVA_Array))                                      //子工作项Code列表
//							.add(field("SubTasks", JAVA_Array))                                            //子工作项列表
//
//							.add(field("ParentCode", JAVA_Integer))                                      //父事项Code
//							.add(field("Parent", JAVA_Map))                                                  //父事项
//
//							.add(field("EpicCode", JAVA_Integer))                                        //所属史诗Code
//							.add(field("Epic", JAVA_Map))                                                    //所属史诗
//
//							.add(field("IterationCode", JAVA_Integer))                                   //所属迭代Code
//							.add(field("Iteration", JAVA_Map))                                               //所属迭代
//			));
//		}
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
				.builder("IssueType",IssueType.verifyType(contextConfig.getIssueType().getName()))
				.builder("PageSize",readSize)
				.builder("SortValue","ASC");
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

//		HttpEntity<String,Object> issueDetialBody = HttpEntity.create()
//				.builder("Action","DescribeIssue")
//				.builder("ProjectName",projectName);

		String teamName = contextConfig.getTeamName();
//		CodingHttp authorization = CodingHttp.create(header.getEntity(), String.format(CodingStarter.OPEN_API_URL, teamName ));
//		HttpRequest requestDetail = authorization.createHttpRequest();
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
//				Object code = stringObjectMap.get("Code");
//				Map<String,Object> issueDetail = issueLoader.readIssueDetail(
//						issueDetialBody,
//						authorization,
//						requestDetail,
//						(code instanceof Integer)?(Integer)code:Integer.parseInt(code.toString()),
//						projectName,
//						teamName);
				String modeName = nodeContext.getConnectionConfig().getString("connectionMode");
				ConnectionMode instance = ConnectionMode.getInstanceByName(modeName);
				if (null == instance){
					throw new CoreException("Connection Mode is not empty or not null.");
				}
				Map<String,Object> issueDetail = instance.attributeAssignment(nodeContext,stringObjectMap);

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


	private Map<String,Object> createFiledMap(){
		String fileds = "{\n" +
				"  \"Issue\": {\n" +
				"    \"ParentType\": \"MISSION\",\n" +
				"    \"Code\": \"ID\",\n" +
				"    \"Type\": \"事项类型\",\n" +
				"    \"Name\": \"标题\",\n" +
				"    \"Description\": \"描述\",\n" +
				"    \"IterationId\": 0,\n" +
				"    \"IssueStatusId\": 1587660,\n" +
				"    \"IssueStatusName\": \"未开始\",\n" +
				"    \"IssueStatusType\": \"TODO\",\n" +
				"    \"CreatedAt\": \"创建时间\",\n" +
				"    \"UpdatedAt\": \"更新时间\",\n" +
				"    \"Priority\": \"2\",\n" +
				"    \"Epic\": {\n" +
				"      \"Code\": 0,\n" +
				"      \"Type\": \"\",\n" +
				"      \"Name\": \"\",\n" +
				"      \"IssueStatusId\": 0,\n" +
				"      \"IssueStatusName\": \"\",\n" +
				"      \"Priority\": \"\",\n" +
				"      \"Assignee\": {\n" +
				"        \"Id\": 0,\n" +
				"        \"Status\": 0,\n" +
				"        \"Avatar\": \"\",\n" +
				"        \"Name\": \"\",\n" +
				"        \"Email\": \"\",\n" +
				"        \"TeamId\": 0,\n" +
				"        \"Phone\": \"\",\n" +
				"        \"GlobalKey\": \"\",\n" +
				"        \"TeamGlobalKey\": \"\"\n" +
				"      }\n" +
				"    },\n" +
				"    \"Assignee\": {\n" +
				"      \"Id\": 0,\n" +
				"      \"Status\": 0,\n" +
				"      \"Avatar\": \"\",\n" +
				"      \"Name\": \"\",\n" +
				"      \"Email\": \"\",\n" +
				"      \"TeamId\": 0,\n" +
				"      \"Phone\": \"\",\n" +
				"      \"GlobalKey\": \"\",\n" +
				"      \"TeamGlobalKey\": \"\"\n" +
				"    },\n" +
				"    \"StartDate\": 0,\n" +
				"    \"DueDate\": 0,\n" +
				"    \"WorkingHours\": 0,\n" +
				"    \"Creator\": {\n" +
				"      \"Id\": 8054404,\n" +
				"      \"Status\": 1,\n" +
				"      \"Avatar\": \"https://coding-net-production-static-ci.codehub.cn/WM-TEXT-AVATAR-eIKPrrFIbZvWEBGUurtc.jpg\",\n" +
				"      \"Name\": \"Berry\",\n" +
				"      \"Email\": \"\",\n" +
				"      \"TeamId\": 0,\n" +
				"      \"Phone\": \"\",\n" +
				"      \"GlobalKey\": \"\",\n" +
				"      \"TeamGlobalKey\": \"\"\n" +
				"    },\n" +
				"    \"StoryPoint\": \"\",\n" +
				"    \"CompletedAt\": 0,\n" +
				"    \"ProjectModule\": {\n" +
				"      \"Id\": 0,\n" +
				"      \"Name\": \"\"\n" +
				"    },\n" +
				"    \"Watchers\": [\n" +
				"      {\n" +
				"        \"Id\": 8054404,\n" +
				"        \"Status\": 1,\n" +
				"        \"Avatar\": \"https://coding-net-production-static-ci.codehub.cn/WM-TEXT-AVATAR-eIKPrrFIbZvWEBGUurtc.jpg\",\n" +
				"        \"Name\": \"Berry\",\n" +
				"        \"Email\": \"\",\n" +
				"        \"TeamId\": 0,\n" +
				"        \"Phone\": \"\",\n" +
				"        \"GlobalKey\": \"\",\n" +
				"        \"TeamGlobalKey\": \"\"\n" +
				"      }\n" +
				"    ],\n" +
				"    \"Labels\": [\n" +
				"      \n" +
				"    ],\n" +
				"    \"Files\": [\n" +
				"      \n" +
				"    ],\n" +
				"    \"RequirementType\": {\n" +
				"      \"Id\": 0,\n" +
				"      \"Name\": \"\"\n" +
				"    },\n" +
				"    \"DefectType\": {\n" +
				"      \"Id\": 0,\n" +
				"      \"Name\": \"\",\n" +
				"      \"IconUrl\": \"\"\n" +
				"    },\n" +
				"    \"CustomFields\": [\n" +
				"      \n" +
				"    ],\n" +
				"    \"ThirdLinks\": [\n" +
				"      \n" +
				"    ],\n" +
				"    \"SubTasks\": [\n" +
				"      \n" +
				"    ],\n" +
				"    \"Parent\": {\n" +
				"      \"Code\": 2,\n" +
				"      \"Type\": \"MISSION\",\n" +
				"      \"Name\": \"云版首页设计\",\n" +
				"      \"IssueStatusId\": 1587684,\n" +
				"      \"IssueStatusName\": \"已完成\",\n" +
				"      \"Priority\": \"2\",\n" +
				"      \"Assignee\": {\n" +
				"        \"Id\": 0,\n" +
				"        \"Status\": 0,\n" +
				"        \"Avatar\": \"\",\n" +
				"        \"Name\": \"\",\n" +
				"        \"Email\": \"\",\n" +
				"        \"TeamId\": 0,\n" +
				"        \"Phone\": \"\",\n" +
				"        \"GlobalKey\": \"\",\n" +
				"        \"TeamGlobalKey\": \"\"\n" +
				"      },\n" +
				"      \"IssueStatusType\": \"COMPLETED\",\n" +
				"      \"IssueTypeDetail\": {\n" +
				"        \"Id\": 0,\n" +
				"        \"Name\": \"\",\n" +
				"        \"IssueType\": \"\",\n" +
				"        \"Description\": \"\",\n" +
				"        \"IsSystem\": false\n" +
				"      }\n" +
				"    },\n" +
				"    \"Iteration\": {\n" +
				"      \"Code\": 0,\n" +
				"      \"Name\": \"\",\n" +
				"      \"Status\": \"\",\n" +
				"      \"Id\": 0\n" +
				"    },\n" +
				"    \"IssueTypeDetail\": {\n" +
				"      \"Id\": 104985,\n" +
				"      \"Name\": \"子工作项\",\n" +
				"      \"IssueType\": \"SUB_TASK\",\n" +
				"      \"Description\": \"在敏捷模式下，将一个事项拆分成更小的块。\",\n" +
				"      \"IsSystem\": true\n" +
				"    },\n" +
				"    \"IssueTypeId\": 104985\n" +
				"  }\n" +
				"}";
		return JSONUtil.parseObj(fileds,false,true);
	}
}
