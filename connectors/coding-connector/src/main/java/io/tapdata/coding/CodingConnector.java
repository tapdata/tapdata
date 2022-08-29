package io.tapdata.coding;

import cn.hutool.core.date.DateUtil;
import cn.hutool.http.HttpRequest;
import io.tapdata.base.ConnectorBase;
import io.tapdata.coding.entity.CodingOffset;
import io.tapdata.coding.enums.IssueType;
import io.tapdata.coding.service.CodingStarter;
import io.tapdata.coding.service.streamRead.StreamReader;
import io.tapdata.coding.service.TestCoding;
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

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static io.tapdata.entity.utils.JavaTypesToTapTypes.*;

@TapConnectorClass("spec.json")
public class CodingConnector extends ConnectorBase {
	private static final String TAG = CodingConnector.class.getSimpleName();


	@Override
	public void onStart(TapConnectionContext connectionContext) throws Throwable {

	}

	@Override
	public void onStop(TapConnectionContext connectionContext) throws Throwable {
		TapLogger.info(TAG, "Stop connector");
	}

	@Override
	public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
		connectorFunctions.supportBatchRead(this::batchRead);
		connectorFunctions.supportBatchCount(this::batchCount);
		connectorFunctions.supportTimestampToStreamOffset(this::timestampToStreamOffset);
		connectorFunctions.supportStreamRead(this::streamRead);
	}

	private void streamRead(
			TapConnectorContext nodeContext,
			List<String> tableList,
			Object offsetState,
			int recordSize,
			StreamReadConsumer consumer) {
		CodingOffset codingOffset =
				null != offsetState && offsetState instanceof CodingOffset
						? (CodingOffset)offsetState : new CodingOffset();
		Map<String, String> tableUpdateTimeMap = codingOffset.getTableUpdateTimeMap();
		Map<String,String> tableCurrentMap = new HashMap<>();
		if (null != tableUpdateTimeMap ){
			tableList.forEach(table->tableCurrentMap.put(table, tableUpdateTimeMap.get(table)));
		}

		String current = tableCurrentMap.get(tableList.get(0));

		if (null == nodeContext){
			throw new IllegalArgumentException("TapConnectorContext cannot be null");
		}
		DataMap nodeConfig = nodeContext.getNodeConfig();
		if (null == nodeConfig ){
			throw new IllegalArgumentException("TapTable' DataMap cannot be null");
		}
		String projectName = nodeConfig.getString("projectName");
		String token = nodeConfig.getString("token");
		String teamName = nodeConfig.getString("teamName");
		if ( null == projectName || "".equals(projectName)){
			TapLogger.info(TAG, "Connection parameter exception: {} ", projectName);
		}
		if ( null == token || "".equals(token) ){
			TapLogger.info(TAG, "Connection parameter exception: {} ", token);
		}
		if ( null == teamName || "".equals(teamName) ){
			TapLogger.info(TAG, "Connection parameter exception: {} ", teamName);
		}


		StreamReader.create();


		consumer.accept(null, offsetState);
	}

	private Object timestampToStreamOffset(TapConnectorContext tapConnectorContext, Long time) {
		if(time == null)
			time = System.currentTimeMillis();
		return map(entry("time", time));
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

		String currentStr = DateUtil.format(new Date(),"YYYY-MM-DD HH:mm:ss");

		CodingOffset codingOffset =  new CodingOffset();
		codingOffset.setTableUpdateTimeMap(new HashMap<String,String>(){{ put(table.getId(),currentStr);}});

		this.verifyParam(connectorContext);
		DataMap connectionConfig = connectorContext.getConnectionConfig();
		String projectName = connectionConfig.getString("projectName");
		String token = connectionConfig.getString("token");
		String teamName = connectionConfig.getString("teamName");

		int currentQueryCount = 0,queryIndex = 0,pageSize= 1000;

		final List<TapEvent>[] events = new List[]{new ArrayList<>()};
		CodingHttp authorization = CodingHttp.create(map(new Entry("Authorization", token)), String.format(CodingStarter.OPEN_API_URL, teamName));
		HttpRequest requestDetail = authorization.createHttpRequest();
		Map<String, Object> bodyMap = map(
				entry("Action", "DescribeIssue"),
				entry("ProjectName", projectName),
				entry("Conditions",list(map(entry("Key","UPDATED_AT"),entry("Value","1000-01-01 00:00:00_"+currentStr)))),
				entry("SortKey","UPDATED_AT"),
				entry("SortValue","DESC")
		);
		do{
			Map<String,Object> dataMap = this.getIssuePage(pageSize,++queryIndex,token,projectName,String.format(CodingStarter.OPEN_API_URL,teamName));
			if (null == dataMap || null == dataMap.get("List")) {
				TapLogger.info(TAG, "Paging result request failed, the Issue list is empty: page index = {}",queryIndex);
				throw new RuntimeException("Paging result request failed, the Issue list is empty: "+CodingStarter.OPEN_API_URL+"?Action=DescribeIssueListWithPage");
			}
			List<Map<String,Object>> resultList = (List<Map<String,Object>>) dataMap.get("List");
			currentQueryCount = resultList.size();
			pageSize = null != dataMap.get("PageSize") ? (int)(dataMap.get("PageSize")) : pageSize;
			for (Map<String, Object> stringObjectMap : resultList) {

				try {

					Thread.sleep(Math.round(100)+100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

				String code = String.valueOf(stringObjectMap.get("Code"));
				bodyMap.put("IssueCode", Integer.parseInt(code));
				//查询事项详情
				Map<String,Object> issueDetailResponse = authorization.body(bodyMap).post(requestDetail);
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

				this.composeIssue(projectName, teamName, issueDetail);
				events[0].add(insertRecordEvent(issueDetail, table.getId()));
				if (events[0].size() == batchCount) {
					consumer.accept(events[0], null);
					events[0] = new ArrayList<>();
				}
			}
		}while (currentQueryCount >= pageSize);
		if (events[0].size() > 0)  consumer.accept(events[0], null);

		TapLogger.info(TAG, "compile {} batch read", table.getName());
	}

	private long batchCount(TapConnectorContext tapConnectorContext, TapTable tapTable) throws Throwable {
		long count = 0;
		this.verifyParam(tapConnectorContext);
		try {
			DataMap connectionConfig = tapConnectorContext.getConnectionConfig();
			String projectName = connectionConfig.getString("projectName");
			String token = connectionConfig.getString("token");
			String teamName = connectionConfig.getString("teamName");
			Map<String,Object> dataMap = this.getIssuePage(1,1,token,projectName,String.format(CodingStarter.OPEN_API_URL,teamName));
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
							.add(field("Code", JAVA_Integer).tapType(toTapType(JAVA_Integer)).isPrimaryKey(true).primaryKeyPos(3))        //事项 Code
							.add(field("ProjectName", JAVA_String).tapType(toTapType(JAVA_String)).isPrimaryKey(true).primaryKeyPos(2))   //项目名称
							.add(field("TeamName", JAVA_String).tapType(toTapType(JAVA_String)).isPrimaryKey(true).primaryKeyPos(1))      //团队名称
							.add(field("ParentType", JAVA_String).tapType(toTapType(JAVA_String)))                                        //父事项类型
							.add(field("Type", JAVA_String).tapType(toTapType(JAVA_String)))                                              //事项类型：DEFECT - 缺陷;REQUIREMENT - 需求;MISSION - 任务;EPIC - 史诗;SUB_TASK - 子工作项

							.add(field("IssueTypeDetailId", JAVA_Integer).tapType(toTapType(JAVA_Integer)))                               //事项类型ID
							.add(field("IssueTypeDetail", JAVA_Map).tapType(toTapType(JAVA_Map)))                                         //事项类型具体信息
							.add(field("Name", JAVA_String).tapType(toTapType(JAVA_String)))                                              //名称
							.add(field("Description", JAVA_String).tapType(toTapType(JAVA_String)))                                       //描述
							.add(field("IterationId", JAVA_Integer).tapType(toTapType(JAVA_Integer)))                                     //迭代 Id
							.add(field("IssueStatusId", JAVA_Integer).tapType(toTapType(JAVA_Integer)))                                   //事项状态 Id
							.add(field("IssueStatusName", JAVA_String).tapType(toTapType(JAVA_String)))                                   //事项状态名称
							.add(field("IssueStatusType", JAVA_String).tapType(toTapType(JAVA_String)))                                   //事项状态类型
							.add(field("Priority", JAVA_String).tapType(toTapType(JAVA_String)))                                          //优先级:"0" - 低;"1" - 中;"2" - 高;"3" - 紧急;"" - 未指定

							.add(field("AssigneeId", JAVA_Integer).tapType(toTapType(JAVA_Integer)))                                      //Assignee.Id 等于 0 时表示未指定
							.add(field("Assignee", JAVA_Map).tapType(toTapType(JAVA_Map)))                                                //处理人
							.add(field("StartDate", JAVA_Long).tapType(toTapType(JAVA_Long)))                                             //开始日期时间戳
							.add(field("DueDate", JAVA_Long).tapType(toTapType(JAVA_Long)))                                               //截止日期时间戳
							.add(field("WorkingHours", JAVA_Double).tapType(toTapType(JAVA_Double)))                                      //工时（小时）

							.add(field("CreatorId", JAVA_Integer).tapType(toTapType(JAVA_Integer)))                                       //创建人Id
							.add(field("Creator", JAVA_Map).tapType(toTapType(JAVA_Map)))                                                 //创建人
							.add(field("StoryPoint", JAVA_String).tapType(toTapType(JAVA_String)))                                        //故事点
							.add(field("CreatedAt", JAVA_Long).tapType(toTapType(JAVA_Long)))                                             //创建时间
							.add(field("UpdatedAt", JAVA_Long).tapType(toTapType(JAVA_Long)))                                             //修改时间
							.add(field("CompletedAt", JAVA_Long).tapType(toTapType(JAVA_Long)))                                           //完成时间

							.add(field("ProjectModuleId", JAVA_Integer).tapType(toTapType(JAVA_Integer)))                                 //ProjectModule.Id 等于 0 时表示未指定
							.add(field("ProjectModule", JAVA_Map).tapType(toTapType(JAVA_Map)))                                           //项目模块

							.add(field("WatcherIdArr", JAVA_Array).tapType(toTapType(JAVA_Array)))                                        //关注人Id列表
							.add(field("Watchers", JAVA_Array).tapType(toTapType(JAVA_Array)))                                            //关注人

							.add(field("LabelIdArr", JAVA_Array).tapType(toTapType(JAVA_Array)))                                          //标签Id列表
							.add(field("Labels", JAVA_Array).tapType(toTapType(JAVA_Array)))                                              //标签列表

							.add(field("FileIdArr", JAVA_Array).tapType(toTapType(JAVA_Array)))                                           //附件Id列表
							.add(field("Files", JAVA_Array).tapType(toTapType(JAVA_Array)))                                               //附件列表
							.add(field("RequirementType", JAVA_String).tapType(toTapType(JAVA_String)))                                   //需求类型

							.add(field("DefectType", JAVA_Map).tapType(toTapType(JAVA_Map)))                                              //缺陷类型
							.add(field("CustomFields", JAVA_Array).tapType(toTapType(JAVA_Array)))                                        //自定义字段列表
							.add(field("ThirdLinks", JAVA_Array).tapType(toTapType(JAVA_Array)))                                          //第三方链接列表

							.add(field("SubTaskCodeArr", JAVA_Array).tapType(toTapType(JAVA_Array)))                                      //子工作项Code列表
							.add(field("SubTasks", JAVA_Array).tapType(toTapType(JAVA_Array)))                                            //子工作项列表

							.add(field("ParentCode", JAVA_Integer).tapType(toTapType(JAVA_Integer)))                                      //父事项Code
							.add(field("Parent", JAVA_Map).tapType(toTapType(JAVA_Map)))                                                  //父事项

							.add(field("EpicCode", JAVA_Integer).tapType(toTapType(JAVA_Integer)))                                        //所属史诗Code
							.add(field("Epic", JAVA_Map).tapType(toTapType(JAVA_Map)))                                                    //所属史诗

							.add(field("IterationCode", JAVA_Integer).tapType(toTapType(JAVA_Integer)))                                   //所属迭代Code
							.add(field("Iteration", JAVA_Map).tapType(toTapType(JAVA_Map)))                                               //所属迭代
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
	 * 校验connectionConfig配置字段
	 */
	private void verifyParam(TapConnectionContext connectionContext){
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
			TapLogger.info(TAG, "Connection parameter exception: {} ", projectName);
		}
		if ( null == token || "".equals(token) ){
			TapLogger.info(TAG, "Connection parameter exception: {} ", token);
		}
		if ( null == teamName || "".equals(teamName) ){
			TapLogger.info(TAG, "Connection parameter exception: {} ", teamName);
		}
	}
	/**一次获取事项分页查询并返回Map结果
	 * @auth GavinX
	 * @param pageSize
	 * @param pageNumber
	 * @param token
	 * @param projectName
	 * @param url
	 * @return
	 */
	private Map<String,Object> getIssuePage(
			int pageSize,
			int pageNumber,
			String token,
			String projectName,
			String url){
		Map<String,Object> resultMap = CodingHttp.create(
				map(new Entry("Authorization", token)),
				map(
						entry("Action",     "DescribeIssueListWithPage"),
						entry("ProjectName",projectName),
						entry("PageSize",   pageSize),
						entry("IssueType",  IssueType.ALL.getName()),
						entry("PageNumber", pageNumber)
				),
				url
		).post();
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
	private void composeIssue(String projectName, String teamName, Map<String, Object> issueDetail) {
		this.addParamToBatch(issueDetail);//给自定义字段赋值
		issueDetail.put("ProjectName",projectName);
		issueDetail.put("TeamName",   teamName);
	}

	/**
	 * @auth GavinX
	 * 向事项详细信息返回结果中添加部分指定字段值
	 * @param batchMap
	 */
	private void addParamToBatch(Map<String,Object> batchMap){
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
}
