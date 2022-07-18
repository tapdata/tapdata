package com.tapdata.processor.dataflow;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.entity.MessageEntity;
import com.tapdata.entity.dataflow.Capitalized;
import com.tapdata.entity.dataflow.Stage;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2021-10-12 21:23
 **/
public class FieldCaseProcessor implements DataFlowProcessor {

	private ProcessorContext processorContext;
	private Stage stage;

	@Override
	public List<MessageEntity> process(List<MessageEntity> batch) {
		if (CollectionUtils.isEmpty(batch)) {
			return batch;
		}
		for (MessageEntity messageEntity : batch) {
			if (StringUtils.isNotBlank(messageEntity.getOp()) &&
					StringUtils.equalsAny(messageEntity.getOp(), ConnectorConstant.MESSAGE_OPERATION_INSERT, ConnectorConstant.MESSAGE_OPERATION_UPDATE, ConnectorConstant.MESSAGE_OPERATION_DELETE)) {
				try {
					process(messageEntity);
				} catch (Exception e) {
					String msg = "Field case convert failed, message: " + messageEntity + ", cause: " + e.getMessage();
					throw new RuntimeException(msg, e);
				}
			}
		}

		return batch;
	}

	private void process(MessageEntity messageEntity) {
		if (messageEntity == null || stage == null) {
			return;
		}
		String fieldsNameTransform = stage.getFieldsNameTransform();
		Map<String, Object> after = messageEntity.getAfter();
		Map<String, Object> before = messageEntity.getBefore();

		Object afterMongoId = null;
		Object beforeMongoId = null;
		boolean needHandleMongoId = false;
		if (processorContext.getSourceConn() != null && processorContext.getTargetConn() != null) {
			// mongo-mongo，_id不做大小写转换
			if (StringUtils.isNoneBlank(processorContext.getSourceConn().getDatabase_type(), processorContext.getTargetConn().getDatabase_type())
					&& StringUtils.equalsAnyIgnoreCase(processorContext.getSourceConn().getDatabase_type(), DatabaseTypeEnum.MONGODB.getType(), DatabaseTypeEnum.ALIYUN_MONGODB.getType())
					&& StringUtils.equalsAnyIgnoreCase(processorContext.getTargetConn().getDatabase_type(), DatabaseTypeEnum.MONGODB.getType(), DatabaseTypeEnum.ALIYUN_MONGODB.getType())) {
				needHandleMongoId = true;
				if (MapUtils.isNotEmpty(after) && after.containsKey("_id")) {
					afterMongoId = after.get("_id");
					after.remove("_id");
				}
				if (MapUtils.isNotEmpty(before) && before.containsKey("_id")) {
					beforeMongoId = before.get("_id");
					before.remove("_id");
				}
			}
		}

		// 字段名大小写转换
		Map<String, Object> newAfter = Capitalized.convert(after, fieldsNameTransform);
		Map<String, Object> newBefore = Capitalized.convert(before, fieldsNameTransform);

		if (needHandleMongoId) {
			if (afterMongoId != null) {
				newAfter.put("_id", afterMongoId);
			}
			if (beforeMongoId != null) {
				newBefore.put("_id", beforeMongoId);
			}
		}

		messageEntity.setAfter(newAfter);
		messageEntity.setBefore(newBefore);
	}

	@Override
	public void stop() {

	}

	@Override
	public void initialize(ProcessorContext context, Stage stage) throws Exception {
		this.processorContext = context;
		this.stage = stage;
	}

	@Override
	public Stage getStage() {
		return stage;
	}
}
