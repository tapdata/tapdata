package com.tapdata.processor;

import com.tapdata.constant.DataFlowStageUtil;
import com.tapdata.entity.dataflow.Stage;
import com.tapdata.processor.dataflow.CustomProcessor;
import com.tapdata.processor.dataflow.DataFlowProcessor;
import com.tapdata.processor.dataflow.FieldCaseProcessor;
import com.tapdata.processor.dataflow.FieldDataFlowProcessor;
import com.tapdata.processor.dataflow.ProcessorContext;
import com.tapdata.processor.dataflow.ProtobufConvertProcessor;
import com.tapdata.processor.dataflow.RowFilterProcessor;
import com.tapdata.processor.dataflow.ScriptDataFlowProcessor;
import com.tapdata.processor.dataflow.aggregation.incr.IncrementAggregationProcessor;

import java.util.ArrayList;
import java.util.List;

/**
 * @author jackin
 */
public class ProcessorUtil {

	public static void closeScriptConnection(ProcessorContext context) {
		if (context != null) {
			ScriptConnection sourceScriptConnection = null;
			ScriptConnection targetScriptConnection = null;
			try {
				sourceScriptConnection = context.getSourceScriptConnection();
				targetScriptConnection = context.getTargetScriptConnection();
			} catch (Exception e) {
				// do nothing
			} finally {
				if (sourceScriptConnection != null) {
					sourceScriptConnection.close();
				}
				if (targetScriptConnection != null) {
					targetScriptConnection.close();
				}
			}
		}
	}

	public static List<DataFlowProcessor> stagesConvertToProcessor(
			List<Stage> stages,
			ProcessorContext processorContext
	) {
		List<DataFlowProcessor> dataFlowProcessors = new ArrayList<>();
		for (Stage stage : stages) {
			String type = stage.getType();
			Stage.StageTypeEnum stageTypeEnum = Stage.StageTypeEnum.fromString(type);
			if (DataFlowStageUtil.isProcessorStage(type)) {
				DataFlowProcessor dataFlowProcessor;
				try {
					switch (stageTypeEnum) {
						case JS_PROCESSOR:
							dataFlowProcessor = new ScriptDataFlowProcessor();
							dataFlowProcessor.initialize(processorContext, stage);
							dataFlowProcessors.add(dataFlowProcessor);
						case CUSTOM_PROCESSOR:
						case CACHE_LOOKUP_PROCESSOR:
							dataFlowProcessor = new CustomProcessor();
							dataFlowProcessor.initialize(processorContext, stage);
							dataFlowProcessors.add(dataFlowProcessor);
							break;
						case FIELD_PROCESSOR:
							dataFlowProcessor = new FieldDataFlowProcessor();
							dataFlowProcessor.initialize(processorContext, stage);
							dataFlowProcessors.add(dataFlowProcessor);
							break;
						case AGGREGATION_PROCESSOR:
							dataFlowProcessor = new IncrementAggregationProcessor();
							dataFlowProcessor.initialize(processorContext, stage);
							dataFlowProcessors.add(dataFlowProcessor);
							break;
						case ROW_FILTER_PROCESSOR:
							dataFlowProcessor = new RowFilterProcessor();
							dataFlowProcessor.initialize(processorContext, stage);
							dataFlowProcessors.add(dataFlowProcessor);
							break;
						case DATA_RULES_PROCESSOR:
							dataFlowProcessor = new DataRulesProcessor();
							dataFlowProcessor.initialize(processorContext, stage);
							dataFlowProcessors.add(dataFlowProcessor);
							break;
						case PROTOBUF_CONVERT_PROCESSOR:
							dataFlowProcessor = new ProtobufConvertProcessor();
							dataFlowProcessor.initialize(processorContext, stage);
							dataFlowProcessors.add(dataFlowProcessor);
							break;
						case FIELD_NAME_TRANSFORM_PROCESSOR:
							dataFlowProcessor = new FieldCaseProcessor();
							dataFlowProcessor.initialize(processorContext, stage);
							dataFlowProcessors.add(dataFlowProcessor);
							break;
						default:
							throw new ProcessorException("Unknown processor type " + type, true);
					}
				} catch (Exception e) {
					throw new ProcessorException("Init data flow processor failed " + e.getMessage(), e, true);
				}
			}
		}

		return dataFlowProcessors;
	}
}
