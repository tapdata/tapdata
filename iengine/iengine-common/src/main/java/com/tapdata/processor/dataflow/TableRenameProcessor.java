package com.tapdata.processor.dataflow;

import com.google.common.collect.Maps;
import com.tapdata.entity.MessageEntity;
import com.tapdata.entity.dataflow.Stage;
import com.tapdata.tm.commons.dag.process.TableRenameProcessNode;
import com.tapdata.tm.commons.dag.vo.TableRenameTableInfo;
import org.apache.commons.lang3.StringUtils;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

public class TableRenameProcessor implements DataFlowProcessor{

	private ProcessorContext processorContext;
	private Stage stage;

	/**
	 * key: 源表名
	 */
	private Map<String, TableRenameTableInfo> tableNameMappingMap;


	public TableRenameProcessor(TableRenameProcessNode tableRenameProcessNode) {

		LinkedHashSet<TableRenameTableInfo> tableNames = tableRenameProcessNode.getTableNames();

		if (Objects.isNull(tableNames) || tableNames.isEmpty()) {
			this.tableNameMappingMap =  Maps.newLinkedHashMap();
		}

		this.tableNameMappingMap = tableNames.stream()
			.collect(Collectors.toMap(TableRenameTableInfo::getPreviousTableName, Function.identity()));


	}

	@Override
	public List<MessageEntity> process(List<MessageEntity> batch) {
		if (batch == null) {
			return batch;
		}

		for (MessageEntity messageEntity : batch) {
			String tableName = messageEntity.getTableName();
			TableRenameTableInfo tableNameMapping = tableNameMappingMap.get(tableName);
			if (tableNameMapping == null || StringUtils.isEmpty(tableNameMapping.getCurrentTableName())) {
				throw new IllegalArgumentException(String.format("no suitable rename configuration for [%s] ", tableName));
			}
			messageEntity.setTableName(tableNameMapping.getCurrentTableName());
		}
		return batch;
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
		return this.stage;
	}

}
