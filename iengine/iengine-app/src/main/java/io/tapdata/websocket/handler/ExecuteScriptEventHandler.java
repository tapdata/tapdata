package io.tapdata.websocket.handler;

import com.tapdata.cache.CacheUtil;
import com.tapdata.cache.ICacheService;
import com.tapdata.cache.memory.MemoryCacheService;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.JSONUtil;
import com.tapdata.constant.MapUtil;
import com.tapdata.constant.MongodbUtil;
import com.tapdata.entity.Connections;
import com.tapdata.entity.JavaScriptFunctions;
import com.tapdata.entity.Job;
import com.tapdata.entity.dataflow.DataFlow;
import com.tapdata.entity.dataflow.JsDebugLog;
import com.tapdata.entity.dataflow.JsDebugRowResult;
import com.tapdata.entity.dataflow.Stage;
import com.tapdata.processor.ScriptConnection;
import com.tapdata.processor.ScriptUtil;
import com.tapdata.processor.constant.JSEngineEnum;
import io.tapdata.debug.DebugConstant;
import io.tapdata.websocket.EventHandlerAnnotation;
import io.tapdata.websocket.WebSocketEventHandler;
import io.tapdata.websocket.WebSocketEventResult;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Query;

import javax.script.Invocable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * js debug事件处理类
 *
 * @author jackin
 */
@EventHandlerAnnotation(type = "execute_script")
public class ExecuteScriptEventHandler extends BaseEventHandler implements WebSocketEventHandler {

	private Logger logger = LogManager.getLogger(getClass());

	/**
	 * 执行js debug事件
	 *
	 * @param eventData 事件
	 * @return
	 */
	@Override
	public WebSocketEventResult handle(Map eventData) {
		WebSocketEventResult result = null;
		ScriptConnection sourceScriptConnection = null;
		ScriptConnection targetScriptConnection = null;
		ExecuteScriptReq executeScriptReq = JSONUtil.map2POJO(eventData, ExecuteScriptReq.class);
		try {
			ValidateResult validateResult = validate(executeScriptReq);

			// 校验失败的时返回
			if (!validateResult.isPassed()) {
				return WebSocketEventResult.handleFailed(WebSocketEventResult.Type.EXECUTE_SCRIPT_RESULT, validateResult.getErrorMsg());
			}

			Connections sourceConn = validateResult.getSourceConn();
			Connections targetConn = validateResult.getTargetConn();
			String script = executeScriptReq.getScript();

			List<JavaScriptFunctions> javaScriptFunctions = validateResult.getJavaScriptFunctions();

			sourceScriptConnection = ScriptUtil.initScriptConnection(sourceConn);
			targetScriptConnection = ScriptUtil.initScriptConnection(targetConn);

			ICacheService dataFlowMemoryCacheService = validateResult.getDataFlowCacheData();
			Invocable scriptEngine = ScriptUtil.getScriptEngine(JSEngineEnum.GRAALVM_JS.getEngineName(),
					script, javaScriptFunctions, clientMongoOperator, sourceScriptConnection,
					targetScriptConnection, dataFlowMemoryCacheService, logger);

			List<Map> debugData = validateResult.getDebugData();

			// 按行执行debug
			List<JsDebugRowResult> debugRowResults = new ArrayList<>();
			for (Map debugDatum : debugData) {
				// debug摸下的自定义的日志工具
				JsDebugLogger jsDebugLogger = new JsDebugLogger();

				JsDebugRowResult debugRowResult = new JsDebugRowResult();
				long startTs = System.currentTimeMillis();
				try {
					Map dataRow = (Map) debugDatum.get(DebugConstant.DEFAULT_DATA_FIELD_NAME);
					debugRowResult.setParams(Arrays.asList(dataRow));
					Object executeResult = scriptEngine.invokeFunction(ScriptUtil.FUNCTION_NAME, dataRow);
					debugRowResult.setResult(executeResult);
					debugRowResult.setStatus(WebSocketEventResult.EVENT_HANDLE_RESULT_SUCCESS);
					debugRowResult.setSrcStageId((String) MapUtil.getValueByKey(debugDatum, "__tapd8.stageId"));

				} catch (Exception e) {
					logger.error("Execute js debug failed {}, data {}", e.getMessage(), debugDatum, e);

					jsDebugLogger.error("Execute debug failed " + e.getMessage());
					debugRowResult.setStatus(WebSocketEventResult.EVENT_HANDLE_RESULT_ERRPR);
				}
				long endTs = System.currentTimeMillis();
				long spentTs = endTs - startTs;

				debugRowResult.setOut(jsDebugLogger.getLogs());
				debugRowResult.setTime(spentTs);
				debugRowResults.add(debugRowResult);
			}

			result = WebSocketEventResult.handleSuccess(WebSocketEventResult.Type.EXECUTE_SCRIPT_RESULT, debugRowResults);
		} catch (Exception e) {
			String error = String.format("Execute js debug failed %s, data flow id %s, stage id %s", e.getMessage(), executeScriptReq.getDataFlowId(), executeScriptReq.getStageId());
			logger.error(error, e);
			return WebSocketEventResult.handleFailed(WebSocketEventResult.Type.EXECUTE_SCRIPT_RESULT, error);
		} finally {
			if (sourceScriptConnection != null) {
				sourceScriptConnection.close();
			}
			if (targetScriptConnection != null) {
				targetScriptConnection.close();
			}
		}

		return result;
	}

	/**
	 * 校验类，验证前端参数传输是否正确
	 *
	 * @param event
	 * @return
	 */
	public ValidateResult validate(ExecuteScriptReq event) {

		ValidateResult result = null;
		String dataFlowId = event.getDataFlowId();
		Query query = new Query(where("id").is(dataFlowId));
		query.fields().include("stages").include("name").include("id");
		List<DataFlow> dataFlows = clientMongoOperator.find(query, ConnectorConstant.DATA_FLOW_COLLECTION, DataFlow.class);
		if (CollectionUtils.isEmpty(dataFlows)) {
			String errorMsg = String.format("Data flow %s does not exists.", dataFlowId);
			return ValidateResult.validateFailed(errorMsg);
		}


		DataFlow dataFlow = dataFlows.get(0);
		List<Stage> stages = dataFlow.getStages();
		if (CollectionUtils.isEmpty(stages)) {
			String errorMsg = String.format("Data flow stages %s does not exists.", dataFlow.getName());
			return ValidateResult.validateFailed(errorMsg);
		}

		String stageId = event.getStageId();

		if (StringUtils.isBlank(stageId)) {
			String errorMsg = String.format("Stage id cannot be empty.");
			result = new ValidateResult();
			result.setPassed(false);
			result.setErrorMsg(errorMsg);
			return result;
		}

		Stage currStage = null;
		for (Stage stage : stages) {
			if (stageId.equals(stage.getId())) {
				currStage = stage;
				break;
			}
		}

		if (currStage == null) {
			String errorMsg = String.format("Stage id does not contain in data flow %s.", dataFlow.getName());
			return ValidateResult.validateFailed(errorMsg);
		}

		Query jobQuery = new Query(where("mappings.stages.id").is(stageId));
		jobQuery.fields().include("connections").include("name").include("id");
		List<Job> jobs = clientMongoOperator.find(jobQuery, ConnectorConstant.JOB_COLLECTION, Job.class);
		if (CollectionUtils.isEmpty(jobs)) {
			// 校验失败返回错误结果
			result = new ValidateResult();
			String errorMsg = String.format("Cannot find job by stage id %s.", stageId);
			result.setPassed(false);
			result.setErrorMsg(errorMsg);
			return result;
		}

		Job job = jobs.get(0);
		// 查询源端连接信息
		String sourceId = job.getConnections().getSource();
		Query querySrc = new Query(where("id").is(sourceId));
		querySrc.fields().exclude("schema");
		List<Connections> sourceConn = MongodbUtil.getConnections(querySrc, null, clientMongoOperator, true);
		if (CollectionUtils.isEmpty(sourceConn)) {
			String errorMsg = String.format("Cannot find source connection %s.", sourceId);
			return ValidateResult.validateFailed(errorMsg);
		}

		// 查询目标端连接信息
		String targetId = job.getConnections().getTarget();
		Query queryTgt = new Query(where("id").is(sourceId));
		queryTgt.fields().exclude("schema");
		List<Connections> targetConn = MongodbUtil.getConnections(queryTgt, null, clientMongoOperator, true);
		if (CollectionUtils.isEmpty(targetConn)) {
			String errorMsg = String.format("Cannot find target connection %s.", targetId);
			return ValidateResult.validateFailed(errorMsg);
		}

		// 获取data trace的数据，用作debug调试
		long startTs = System.currentTimeMillis();
		List<Map> debugData = new ArrayList<>();
		for (String srcStageId : currStage.getInputLanes()) {
			Query debugDataQuery = new Query();
			debugDataQuery.addCriteria(where("__tapd8.dataFlowId").is(new Document("regexp", "^" + dataFlowId + "$")));
			debugDataQuery.addCriteria(where("__tapd8.stageId").is(new Document("regexp", "^" + srcStageId + "$")));
			debugDataQuery.with(Sort.by(Sort.Order.desc("createTime")));
			debugDataQuery.limit(10);

			List<Map> stageDebugData = clientMongoOperator.find(debugDataQuery, DebugConstant.DEBUG_COLLECTION_NAME, Map.class);
			if (CollectionUtils.isNotEmpty(stageDebugData)) {
				debugData.addAll(stageDebugData);
			}
			long endTs = System.currentTimeMillis();
			if (endTs - startTs >= 1000L) {
				logger.info("Found debug data {}, spent {}ms.", debugData.size(), endTs - startTs);
			}
		}

		if (CollectionUtils.isEmpty(debugData)) {
			String errorMsg = String.format("Cannot found js stage %s's debug data, please execute the data trace.", currStage.getName());
			return ValidateResult.validateFailed(errorMsg);
		}

		// 注册缓存
		MemoryCacheService memoryCacheService = new MemoryCacheService(clientMongoOperator);
		CacheUtil.registerCache(job, clientMongoOperator, memoryCacheService);

		List<JavaScriptFunctions> javaScriptFunctions = clientMongoOperator.find(new Query(where("type").ne("system")), ConnectorConstant.JAVASCRIPT_FUNCTION_COLLECTION, JavaScriptFunctions.class);
		return new ValidateResult(true, null, dataFlow, job, sourceConn.get(0), targetConn.get(0), currStage, debugData, javaScriptFunctions, memoryCacheService);
	}

	/**
	 * debug模式下的日志工具
	 */
	public class JsDebugLogger {

		private List<JsDebugLog> logs = new ArrayList<>();

		public void error(String message) {

			logs.add(new JsDebugLog("ERROR", message, new Date(), "JsDebugLogger", "JsDebugThread"));
		}

		public void warn(String message) {
			logs.add(new JsDebugLog("WARN", message, new Date(), "JsDebugLogger", "JsDebugThread"));
		}

		public void info(String message) {
			logs.add(new JsDebugLog("INFO", message, new Date(), "JsDebugLogger", "JsDebugThread"));
		}

		public void debug(String message) {
			logs.add(new JsDebugLog("DEBUG", message, new Date(), "JsDebugLogger", "JsDebugThread"));
		}

		public void trace(String message) {
			logs.add(new JsDebugLog("TRACE", message, new Date(), "JsDebugLogger", "JsDebugThread"));
		}

		public List<JsDebugLog> getLogs() {
			return logs;
		}
	}

	static class ValidateResult {

		boolean passed;

		String errorMsg;

		DataFlow dataFlow;

		Stage stage;

		Job job;

		Connections sourceConn;

		Connections targetConn;

		List<Map> debugData;

		/**
		 * 自定义全局js函数
		 */
		List<JavaScriptFunctions> javaScriptFunctions;

		private ICacheService dataFlowCacheData;

		public ValidateResult() {
		}

		public ValidateResult(
				boolean passed,
				String errorMsg,
				DataFlow dataFlow,
				Job job,
				Connections sourceConn,
				Connections targetConn,
				Stage stage,
				List<Map> debugData,
				List<JavaScriptFunctions> javaScriptFunctions,
				ICacheService dataFlowCacheData
		) {
			this.passed = passed;
			this.errorMsg = errorMsg;
			this.dataFlow = dataFlow;
			this.job = job;
			this.sourceConn = sourceConn;
			this.targetConn = targetConn;
			this.stage = stage;
			this.debugData = debugData;
			this.javaScriptFunctions = javaScriptFunctions;
			this.dataFlowCacheData = dataFlowCacheData;
		}

		public static ValidateResult validateFailed(String errorMsg) {
			ValidateResult validateResult = new ValidateResult();
			validateResult.setErrorMsg(errorMsg);
			validateResult.setPassed(false);

			return validateResult;
		}

		public boolean isPassed() {
			return passed;
		}

		public void setPassed(boolean passed) {
			this.passed = passed;
		}

		public DataFlow getDataFlow() {
			return dataFlow;
		}

		public void setDataFlow(DataFlow dataFlow) {
			this.dataFlow = dataFlow;
		}

		public Job getJob() {
			return job;
		}

		public void setJob(Job job) {
			this.job = job;
		}

		public Connections getSourceConn() {
			return sourceConn;
		}

		public void setSourceConn(Connections sourceConn) {
			this.sourceConn = sourceConn;
		}

		public Connections getTargetConn() {
			return targetConn;
		}

		public void setTargetConn(Connections targetConn) {
			this.targetConn = targetConn;
		}

		public String getErrorMsg() {
			return errorMsg;
		}

		public void setErrorMsg(String errorMsg) {
			this.errorMsg = errorMsg;
		}

		public Stage getStage() {
			return stage;
		}

		public void setStage(Stage stage) {
			this.stage = stage;
		}

		public List<Map> getDebugData() {
			return debugData;
		}

		public void setDebugData(List<Map> debugData) {
			this.debugData = debugData;
		}

		public List<JavaScriptFunctions> getJavaScriptFunctions() {
			return javaScriptFunctions;
		}

		public void setJavaScriptFunctions(List<JavaScriptFunctions> javaScriptFunctions) {
			this.javaScriptFunctions = javaScriptFunctions;
		}

		public ICacheService getDataFlowCacheData() {
			return dataFlowCacheData;
		}
	}

	/**
	 * 接收消息实体
	 *
	 * @author jackin
	 */
	public static class ExecuteScriptReq implements Serializable {

		private static final long serialVersionUID = 7854512644652895521L;

		/**
		 * 消息类型
		 */
		private String type;

		private String script;

		private String script_type;

		private String dataFlowId;

		private String stageId;

		public ExecuteScriptReq() {
		}

		public String getType() {
			return type;
		}

		public void setType(String type) {
			this.type = type;
		}

		public String getScript() {
			return script;
		}

		public void setScript(String script) {
			this.script = script;
		}

		public String getScript_type() {
			return script_type;
		}

		public void setScript_type(String script_type) {
			this.script_type = script_type;
		}

		public String getDataFlowId() {
			return dataFlowId;
		}

		public void setDataFlowId(String dataFlowId) {
			this.dataFlowId = dataFlowId;
		}

		public String getStageId() {
			return stageId;
		}

		public void setStageId(String stageId) {
			this.stageId = stageId;
		}
	}

}
