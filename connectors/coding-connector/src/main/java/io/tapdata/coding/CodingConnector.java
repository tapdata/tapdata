package io.tapdata.coding;

import cn.hutool.core.date.DateUtil;
import cn.hutool.http.HttpRequest;
import io.tapdata.base.ConnectorBase;
import io.tapdata.coding.entity.CodingOffset;
import io.tapdata.coding.enums.IssueType;
import io.tapdata.coding.service.CodingStarter;
import io.tapdata.coding.service.IssueLoader;
import io.tapdata.coding.service.TestCoding;
import io.tapdata.coding.utils.collection.MapUtil;
import io.tapdata.coding.utils.http.CodingHttp;
import io.tapdata.coding.utils.http.HttpEntity;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.Entry;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

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
//	private Long currentTimePoint;
//	private List<Integer> currentTimeSplitIssueCode = new ArrayList<>();//hash code list

	@Override
	public void onStart(TapConnectionContext connectionContext) throws Throwable {

	}

	@Override
	public void onStop(TapConnectionContext connectionContext) throws Throwable {
		synchronized (this) {
			this.notify();
		}
		TapLogger.info(TAG, "Stop connector");
//		streamReadThread.notify();
	}

	@Override
	public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
		connectorFunctions.supportBatchRead(this::batchRead)
				.supportBatchCount(this::batchCount)
				.supportTimestampToStreamOffset(this::timestampToStreamOffset)
				.supportStreamRead(this::streamRead);

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
			//throw new RuntimeException("offsetState is Empty or not Exist!");
		}

		String currentTable = tableList.get(0);
		if (null == currentTable){
			throw new RuntimeException("TableList is Empty or not Exist!");
		}

		consumer.streamReadStarted();
		while (isAlive()) {
			AtomicReference<Long> lastBreakpointTime = new AtomicReference<>(tableUpdateTimeMap.get(tableList.get(0)));
			long current = lastBreakpointTime.get();
			//if (null == current ){
			//	TapLogger.info(TAG, "continue:stream read lack offsetState");
			//	continue;
			//}

			Long last = Long.MAX_VALUE;
			//断点起始时间下一秒比当前时间大时，不再继续读取
			if (current > last){
				TapLogger.info(TAG, "There is no data to read:start:{}-end:{}",IssueLoader.create(nodeContext).longToDateStr(current),IssueLoader.create(nodeContext).longToDateStr(last));
				continue;
			}

			TapLogger.info(TAG, "start {} stream read", currentTable);
			lastBreakpointTime.set(last);

			//@TODO lastTimePoint ----> current
			this.read(nodeContext, lastTimePoint, last, currentTable, recordSize, codingOffset, consumer);
			TapLogger.info(TAG, "compile {} once stream read", currentTable);

			tableUpdateTimeMap.put(tableList.get(0), lastBreakpointTime.get());
			synchronized (this) {
				try {
					this.wait(streamExecutionGap);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

		//consumer.asyncMethodAndNoRetry();
		//new Thread(() -> {
		//
		//},"StreamReadThread").start();
		//consumer.accept(null, offsetState);
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
		IssueLoader.create(connectorContext).verifyConnectionConfig(connectorContext);
		DataMap connectionConfig = connectorContext.getConnectionConfig();
		String projectName = connectionConfig.getString("projectName");
		String token = connectionConfig.getString("token");
		String teamName = connectionConfig.getString("teamName");
		this.read(connectorContext,null,readEnd,table.getId(),batchCount,codingOffset,consumer);
		TapLogger.info(TAG, "compile {} batch read", table.getName());
	}

	private long batchCount(TapConnectorContext tapConnectorContext, TapTable tapTable) throws Throwable {
		long count = 0;
		IssueLoader issueLoader = IssueLoader.create(tapConnectorContext);
		issueLoader.verifyConnectionConfig(tapConnectorContext);
		try {
			DataMap connectionConfig = tapConnectorContext.getConnectionConfig();
			DataMap nodeConfigMap = tapConnectorContext.getNodeConfig();

			String iterationCodes = nodeConfigMap.getString("iterationCodes");
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
		if(tables == null || tables.isEmpty()) {
			consumer.accept(list(
					table("Issues")
							.add(field("Code", JAVA_Integer).isPrimaryKey(true).primaryKeyPos(3))        //事项 Code
							.add(field("ProjectName", JAVA_String).isPrimaryKey(true).primaryKeyPos(2))   //项目名称
							.add(field("TeamName", JAVA_String).isPrimaryKey(true).primaryKeyPos(1))      //团队名称
							.add(field("ParentType", JAVA_String))                                       //父事项类型
							.add(field("Type", JAVA_String))                                         //事项类型：DEFECT - 缺陷;REQUIREMENT - 需求;MISSION - 任务;EPIC - 史诗;SUB_TASK - 子工作项


							.add(field("IssueTypeDetailId", JAVA_Integer))                               //事项类型ID
							.add(field("IssueTypeDetail", JAVA_Map))                                         //事项类型具体信息
							.add(field("Name", JAVA_String))                                              //名称
							.add(field("Description", JAVA_String))                                       //描述
							.add(field("IterationId", JAVA_Integer))                                     //迭代 Id
							.add(field("IssueStatusId", JAVA_Integer))                                   //事项状态 Id
							.add(field("IssueStatusName", JAVA_String))                                   //事项状态名称
							.add(field("IssueStatusType", JAVA_String))                                   //事项状态类型
							.add(field("Priority", JAVA_String))                                          //优先级:"0" - 低;"1" - 中;"2" - 高;"3" - 紧急;"" - 未指定

							.add(field("AssigneeId", JAVA_Integer))                                      //Assignee.Id 等于 0 时表示未指定
							.add(field("Assignee", JAVA_Map))                                                //处理人
							.add(field("StartDate", JAVA_Long))                                             //开始日期时间戳
							.add(field("DueDate", JAVA_Long))                                               //截止日期时间戳
							.add(field("WorkingHours", JAVA_Double))                                      //工时（小时）

							.add(field("CreatorId", JAVA_Integer))                                       //创建人Id
							.add(field("Creator", JAVA_Map))                                                 //创建人
							.add(field("StoryPoint", JAVA_String))                                        //故事点
							.add(field("CreatedAt", JAVA_Long))                                             //创建时间
							.add(field("UpdatedAt", JAVA_Long))                                             //修改时间
							.add(field("CompletedAt", JAVA_Long))                                           //完成时间

							.add(field("ProjectModuleId", JAVA_Integer))                                 //ProjectModule.Id 等于 0 时表示未指定
							.add(field("ProjectModule", JAVA_Map))                                           //项目模块

							.add(field("WatcherIdArr", JAVA_Array))                                        //关注人Id列表
							.add(field("Watchers", JAVA_Array))                                            //关注人

							.add(field("LabelIdArr", JAVA_Array))                                          //标签Id列表
							.add(field("Labels", JAVA_Array))                                              //标签列表

							.add(field("FileIdArr", JAVA_Array))                                           //附件Id列表
							.add(field("Files", JAVA_Array))                                               //附件列表
							.add(field("RequirementType", JAVA_String))                                   //需求类型

							.add(field("DefectType", JAVA_Map))                                              //缺陷类型
							.add(field("CustomFields", JAVA_Array))                                        //自定义字段列表
							.add(field("ThirdLinks", JAVA_Array))                                          //第三方链接列表

							.add(field("SubTaskCodeArr", JAVA_Array))                                      //子工作项Code列表
							.add(field("SubTasks", JAVA_Array))                                            //子工作项列表

							.add(field("ParentCode", JAVA_Integer))                                      //父事项Code
							.add(field("Parent", JAVA_Map))                                                  //父事项

							.add(field("EpicCode", JAVA_Integer))                                        //所属史诗Code
							.add(field("Epic", JAVA_Map))                                                    //所属史诗

							.add(field("IterationCode", JAVA_Integer))                                   //所属迭代Code
							.add(field("Iteration", JAVA_Map))                                               //所属迭代
			));
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
			BiConsumer<List<TapEvent>, Object> consumer ){
		if (null == nodeContext){
			throw new IllegalArgumentException("TapConnectorContext cannot be null");
		}
		DataMap connectionConfigConfigMap = nodeContext.getConnectionConfig();
		if (null == connectionConfigConfigMap){
			throw new IllegalArgumentException("TapTable' ConnectionConfigConfig cannot be null");
		}
		String projectName = connectionConfigConfigMap.getString("projectName");
		String token = connectionConfigConfigMap.getString("token");
		String teamName = connectionConfigConfigMap.getString("teamName");
		if ( null == projectName || "".equals(projectName)){
			TapLogger.error(TAG, "Connection parameter exception: {} ", projectName);
		}
		if ( null == token || "".equals(token) ){
			TapLogger.error(TAG, "Connection parameter exception: {} ", token);
		}
		if ( null == teamName || "".equals(teamName) ){
			TapLogger.error(TAG, "Connection parameter exception: {} ", teamName);
		}

		DataMap nodeConfigMap = nodeContext.getNodeConfig();
		if (null == nodeConfigMap){
			throw new IllegalArgumentException("TapTable' NodeConfig cannot be null");
		}
		//iterationName is Multiple selection values separated by commas
		String iterationCodeArr = nodeConfigMap.getString("iterationCodes");
		if (null!=iterationCodeArr) iterationCodeArr = iterationCodeArr.trim();
		String issueType = nodeConfigMap.getString("issueType");
		if (null != issueType ) issueType = issueType.trim();

		if ( null == iterationCodeArr || "".equals(iterationCodeArr)){
			TapLogger.info(TAG, "Connection node config iterationName exception: {} ", projectName);
		}
		if ( null == issueType || "".equals(issueType) ){
			TapLogger.info(TAG, "Connection node config issueType exception: {} ", token);
		}

		int currentQueryCount = 0,queryIndex = 0 ;
		IssueLoader issueLoader = IssueLoader.create(nodeContext);
		final List<TapEvent>[] events = new List[]{new ArrayList<>()};
		HttpEntity<String,String> header = HttpEntity.create().builder("Authorization",token);
		HttpEntity<String,Object> pageBody = HttpEntity.create()
				.builder("Action","DescribeIssueListWithPage")
				.builder("ProjectName",projectName)
				.builder("SortKey","UPDATED_AT")
				.builder("IssueType",IssueType.verifyType(issueType))
				.builder("PageSize",readSize)
				.builder("SortValue","ASC");
		List<Map<String,Object>> coditions = list(map(
				entry("Key","UPDATED_AT"),
				entry("Value",issueLoader.longToDateStr(readStartTime)+"_"+issueLoader.longToDateStr(readEndTime)))
		);
		if (null != iterationCodeArr && !"".equals(iterationCodeArr) && !",".equals(iterationCodeArr)){
			//String[] iterationCodeArr = iterationCodes.split(",");
			//@TODO 输入的迭代编号需要验证，否则，查询事项列表时作为查询条件的迭代不存在时，查询会报错
			//选择的迭代编号不需要验证
			coditions.add(map(entry("Key","ITERATION"),entry("Value",iterationCodeArr)));
		}
		pageBody.builder("Conditions",coditions);

		HttpEntity<String,Object> issueDetialBody = HttpEntity.create()
				.builder("Action","DescribeIssue")
				.builder("ProjectName",projectName);

		CodingHttp authorization = CodingHttp.create(header.getEntity(), String.format(CodingStarter.OPEN_API_URL, teamName));
		HttpRequest requestDetail = authorization.createHttpRequest();
		do{
			pageBody.builder("PageNumber",queryIndex++);
			Map<String,Object> dataMap = issueLoader.getIssuePage(header.getEntity(),pageBody.getEntity(),String.format(CodingStarter.OPEN_API_URL,teamName));
			if (null == dataMap || null == dataMap.get("List")) {
				TapLogger.info(TAG, "Paging result request failed, the Issue list is empty: page index = {}",queryIndex);
				throw new RuntimeException("Paging result request failed, the Issue list is empty: "+CodingStarter.OPEN_API_URL+"?Action=DescribeIssueListWithPage");
			}
			List<Map<String,Object>> resultList = (List<Map<String,Object>>) dataMap.get("List");
			currentQueryCount = resultList.size();
			batchReadPageSize = null != dataMap.get("PageSize") ? (int)(dataMap.get("PageSize")) : batchReadPageSize;
			for (Map<String, Object> stringObjectMap : resultList) {
				Integer code = Integer.parseInt(String.valueOf(stringObjectMap.get("Code")));
				//查询事项详情
				issueDetialBody.builder("IssueCode", code);
				Map<String,Object> issueDetailResponse = authorization.body(issueDetialBody.getEntity()).post(requestDetail);
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
					throw new RuntimeException("Issue Detail acquisition failed: IssueCode "+code);
				}

				issueLoader.composeIssue(projectName, teamName, issueDetail);
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

				((CodingOffset)offsetState).getTableUpdateTimeMap().put("Issues",referenceTime);
				if (events[0].size() == readSize) {
					consumer.accept(events[0], offsetState);
					events[0] = new ArrayList<>();
				}
			}
		}while (currentQueryCount >= batchReadPageSize);
		if (events[0].size() > 0)  consumer.accept(events[0], offsetState);
	}
}
