package com.tapdata.processor.dataflow;

import com.alibaba.fastjson.JSON;
import com.tapdata.entity.MessageEntity;
import com.tapdata.entity.dataflow.Stage;
import com.tapdata.processor.dataflow.pb.DynamicProtoUtil;
import com.tapdata.processor.dataflow.pb.PbConfiguration;
import com.tapdata.processor.dataflow.pb.PbModel;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ProtobufConvertProcessor implements DataFlowProcessor {

	private Logger logger = LogManager.getLogger(getClass());

	private Stage stage;

	private ProcessorContext context;

	private PbConfiguration pbConfiguration;

	/**
	 * {
	 * "Unit.login.encryptionRules": "a.b.c",
	 * "Unit.login.loginTime": "rewq",
	 * "Unit.login.platformName": "strfdsafing",
	 * "Unit.login.loginSerialNumber": "vfd",
	 * "Unit.login.platformPassword": "ferfre"
	 * }
	 */
	private Map<String, String> dataFieldMappingMap;

	private boolean running = true;

	@Override
	public List<MessageEntity> process(List<MessageEntity> batch) {
		if (CollectionUtils.isNotEmpty(batch)) {
			for (MessageEntity messageEntity : batch) {
				if (!running) {
					break;
				}
				Map<String, Object> dataMap = messageEntity.getAfter();
				if (MapUtils.isNotEmpty(dataMap)) {
					//将数据报文转换为pb格式并添加到数据map中
					byte[] pbByteArray = DynamicProtoUtil.getPbMsgByteArray(dataMap, pbConfiguration, dataFieldMappingMap);
					dataMap.put("unit", pbByteArray);
				}
			}
		}
		return batch;
	}

	@Override
	public void stop() {
		running = false;
	}

	@Override
	public void initialize(ProcessorContext context, Stage stage) throws Exception {
		this.stage = stage;
		this.context = context;

		Map<String, Object> pbProcessorConfig = stage.getPbProcessorConfig();
		Map<String, String> mappingParamMap = (Map<String, String>) pbProcessorConfig.get("mapping");
		Map<String, String> mappingMap = mappingParamMap.entrySet().stream().collect(Collectors.toMap(e -> StringUtils.replaceChars(e.getKey(), "#", "."),
				e -> StringUtils.replaceChars(e.getValue(), "#", ".")));
		this.dataFieldMappingMap = JSON.parseObject(JSON.toJSONString(mappingMap), Map.class);
		PbModel pbModel = JSON.parseObject(JSON.toJSONString(pbProcessorConfig.get("schema")), PbModel.class);
		this.pbConfiguration = PbConfiguration.builder().schema(DynamicProtoUtil.generateSchema(pbModel))
				.filedMsgDefNameMappingMap(DynamicProtoUtil.getFieldTypeMappingMap(pbModel)).build();

	}

	@Override
	public Stage getStage() {
		return stage;
	}
}
