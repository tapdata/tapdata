package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.HazelcastUtil;
import com.tapdata.entity.*;
import com.tapdata.entity.dataflow.Stage;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.processor.dataflow.*;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.process.FieldProcessorNode;
import com.tapdata.tm.commons.dag.process.FieldRenameProcessorNode;
import com.tapdata.tm.commons.dag.process.JsProcessorNode;
import com.tapdata.tm.commons.dag.process.RowFilterProcessorNode;
import com.tapdata.tm.commons.task.dto.SubTaskDto;
import io.tapdata.common.sample.sampler.AverageSampler;
import io.tapdata.common.sample.sampler.CounterSampler;
import io.tapdata.common.sample.sampler.ResetCounterSampler;
import io.tapdata.common.sample.sampler.SpeedSampler;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.flow.engine.V2.common.node.NodeTypeEnum;
import io.tapdata.flow.engine.V2.entity.TapdataEvent;
import io.tapdata.flow.engine.V2.node.hazelcast.HazelcastBaseNode;
import io.tapdata.metrics.TaskSampleRetriever;
import io.tapdata.schema.TapTableMap;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Query;

import javax.annotation.Nonnull;
import java.util.*;

import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * @author jackin
 * @date 2021/12/16 10:34 PM
 **/
public class HazelcastProcessorNode extends HazelcastBaseNode {

	private DataFlowProcessor dataFlowProcessor;

	// statistic and sample related
	private ResetCounterSampler resetInputCounter;
	private CounterSampler inputCounter;
	private ResetCounterSampler resetOutputCounter;
	private CounterSampler outputCounter;
	private SpeedSampler inputQPS;
	private SpeedSampler outputQPS;

	@Override
	public void close() throws Exception {
		if (dataFlowProcessor != null) {
			dataFlowProcessor.stop();
		}
	}

	private AverageSampler timeCostAvg;

	public HazelcastProcessorNode(DataProcessorContext dataProcessorContext) throws Exception {
		super(dataProcessorContext);
		dataFlowProcessor = new ScriptDataFlowProcessor();
		final Stage stage = HazelcastUtil.node2CommonStage(processorBaseContext.getNode());

		dataFlowProcessor = createDataFlowProcessor(processorBaseContext.getNode(), stage);

		Job job = new Job();
		SubTaskDto subTaskDto = processorBaseContext.getSubTaskDto();
		job.setDataFlowId(subTaskDto.getId().toHexString());
		job.setStatus(ConnectorConstant.RUNNING);
		job.setSubTaskId(subTaskDto.getId().toHexString());
		job.setTaskId(subTaskDto.getParentId().toHexString());
		job.setJobErrorNotifier(this::errorHandle);
		job.setUser_id(subTaskDto.getUserId());
		List<JavaScriptFunctions> javaScriptFunctions = clientMongoOperator.find(new Query(where("type").ne("system")).with(Sort.by(Sort.Order.asc("last_update"))), ConnectorConstant.JAVASCRIPT_FUNCTION_COLLECTION, JavaScriptFunctions.class);

		ProcessorContext processorContext = new ProcessorContext(
				null,
				null,
				job,
				clientMongoOperator,
				javaScriptFunctions,
				null,
				dataProcessorContext.getCacheService()
		);
		dataFlowProcessor.initialize(processorContext, stage);

	}

	@Override
	protected void initSampleCollector() {
		super.initSampleCollector();

		// TODO: init outputCounter initial value
		Map<String, Number> values = TaskSampleRetriever.getInstance().retrieve(tags, Arrays.asList(
				"inputTotal", "outputTotal"
		));
		// init statistic and sample related initialize
		resetInputCounter = statisticCollector.getResetCounterSampler("inputTotal");
		inputCounter = sampleCollector.getCounterSampler("inputTotal", values.getOrDefault("inputTotal", 0).longValue());
		resetOutputCounter = statisticCollector.getResetCounterSampler("outputTotal");
		outputCounter = sampleCollector.getCounterSampler("outputTotal", values.getOrDefault("outputTotal", 0).longValue());
		inputQPS = sampleCollector.getSpeedSampler("inputQPS");
		outputQPS = sampleCollector.getSpeedSampler("outputQPS");
		timeCostAvg = sampleCollector.getAverageSampler("timeCostAvg");

	}

	@Override
	public boolean tryProcess(int ordinal, @Nonnull Object item) {
		TapdataEvent tapdataEvent = (TapdataEvent) item;
		MessageEntity messageEntity;
		if (null != tapdataEvent.getMessageEntity()) {
			messageEntity = tapdataEvent.getMessageEntity();
		} else if (null != tapdataEvent.getTapEvent()) {
			transformFromTapValue(tapdataEvent, null);
			messageEntity = tapEvent2Message((TapRecordEvent) tapdataEvent.getTapEvent());
			messageEntity.setOffset(tapdataEvent.getOffset());
		} else {
			while (running.get()) {
				if (offer(tapdataEvent)) break;
			}
			return true;
		}

		int cnt = messageEntity.isDml() ? 1 : 0;
		resetInputCounter.inc(cnt);
		inputCounter.inc(cnt);
		inputQPS.add(cnt);
		long start = System.currentTimeMillis();
		final List<MessageEntity> processedMessages = dataFlowProcessor.process(Collections.singletonList(messageEntity));
		timeCostAvg.add(System.currentTimeMillis() - start);
		resetOutputCounter.inc(cnt);
		outputCounter.inc(cnt);
		outputQPS.add(cnt);


		if (CollectionUtils.isNotEmpty(processedMessages)) {
			for (MessageEntity processedMessage : processedMessages) {
				TapdataEvent processedEvent = new TapdataEvent();
				TapRecordEvent tapRecordEvent = message2TapEvent(processedMessage);
				if (tapRecordEvent != null) {
					processedEvent.setTapEvent(tapRecordEvent);
					TapTableMap<String, TapTable> tapTableMap = processorBaseContext.getTapTableMap();
					transformToTapValue(processedEvent, tapTableMap, processorBaseContext.getNode().getId());
					while (running.get()) {
						if (offer(processedEvent)) break;
					}
				}
			}
		}

		return true;
	}

	private DataFlowProcessor createDataFlowProcessor(Node node, Stage stage) {
		final NodeTypeEnum nodeType = NodeTypeEnum.get(node.getType());
		DataFlowProcessor dataFlowProcessor = null;
		switch (nodeType) {
			case CACHE_LOOKUP_PROCESSOR:
			case JS_PROCESSOR:
				dataFlowProcessor = new ScriptDataFlowProcessor();
				stage.setType(Stage.StageTypeEnum.SCRIPT_TYPE.getType());

				JsProcessorNode jsProcessorNode = (JsProcessorNode) node;
				stage.setScript(jsProcessorNode.getScript());
				break;
			case FIELD_PROCESSOR:
			case FIELD_RENAME_PROCESSOR:
			case FIELD_ADD_DEL_PROCESSOR:
			case FIELD_CALC_PROCESSOR:
			case FIELD_MOD_TYPE_PROCESSOR:
				List<FieldProcess> fieldProcesses = new ArrayList<>();
				FieldProcessorNode fieldProcessor = (FieldProcessorNode) node;
				final List<FieldProcessorNode.Operation> operations = fieldProcessor.getOperations();
				if (null != operations) {
					for (FieldProcessorNode.Operation operation : operations) {
						FieldProcess fieldProcess = new FieldProcess();
						fieldProcess.setField(operation.getField());
						fieldProcess.setOp(operation.getOp());
						fieldProcess.setOperand(StringUtils.isNotBlank(operation.getOperand()) ? operation.getOperand() : operation.getField());
						fieldProcess.setOriginalDataType(operation.getOriginalDataType());
						fieldProcess.setJavaType(operation.getType());
						fieldProcesses.add(fieldProcess);
					}
				}
				List<FieldProcessorNode.Script> scripts = ((FieldProcessorNode) node).getScripts();
				List<FieldScript> fieldScripts = new ArrayList<>();
				if (null != scripts) {
					for (FieldProcessorNode.Script script : scripts) {
						FieldScript fieldScript = new FieldScript();
						fieldScript.setField(script.getField());
						fieldScript.setScript(script.getScript());
						fieldScript.setScriptType(script.getScriptType());
						fieldScript.setTableName(script.getTableName());
						fieldScripts.add(fieldScript);
					}
				}
				stage.setScripts(fieldScripts);
				stage.setOperations(fieldProcesses);
				if (node instanceof FieldRenameProcessorNode) {
					dataFlowProcessor = new FieldDataFlowProcessor(((FieldRenameProcessorNode) node).getFieldsNameTransform());
				} else {
					dataFlowProcessor = new FieldDataFlowProcessor();
				}
				break;
			case ROW_FILTER_PROCESSOR:
				dataFlowProcessor = new RowFilterProcessor();
				RowFilterProcessorNode rowFilterProcessor = (RowFilterProcessorNode) node;

				stage.setAction(rowFilterProcessor.getAction());
				stage.setExpression(rowFilterProcessor.getExpression());
				break;
		}

		return dataFlowProcessor;

	}
}
