package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import com.google.common.collect.Maps;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.tm.commons.dag.process.TableRenameProcessNode;
import com.tapdata.tm.commons.dag.vo.TableRenameTableInfo;
import io.tapdata.entity.event.TapBaseEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class HazelcastRenameTableProcessorNode extends HazelcastProcessorBaseNode {

	/**
	 * key: 源表名
	 */
	private Map<String, TableRenameTableInfo> tableNameMappingMap;

	public HazelcastRenameTableProcessorNode(ProcessorBaseContext processorBaseContext) {
		super(processorBaseContext);
		initTableNameMapping();
	}

	private void initTableNameMapping() {
		TableRenameProcessNode tableRenameProcessNode = (TableRenameProcessNode) getNode();
		LinkedHashSet<TableRenameTableInfo> tableNames = tableRenameProcessNode.getTableNames();

		if (Objects.isNull(tableNames) || tableNames.isEmpty()) {
			this.tableNameMappingMap = Maps.newLinkedHashMap();
		}

		this.tableNameMappingMap = tableNames.stream()
				.collect(Collectors.toMap(TableRenameTableInfo::getPreviousTableName, Function.identity()));
	}

	@Override
	protected void tryProcess(TapdataEvent tapdataEvent, BiConsumer<TapdataEvent, ProcessResult> consumer) {
		TapEvent tapEvent = tapdataEvent.getTapEvent();
		String tableId = TapEventUtil.getTableId(tapEvent);

		if (tapEvent instanceof TapBaseEvent) {
			TableRenameTableInfo tableRenameTableInfo = tableNameMappingMap.get(tableId);
			if (tableRenameTableInfo != null && StringUtils.isNotEmpty(tableRenameTableInfo.getCurrentTableName())) {
				((TapBaseEvent) tapEvent).setTableId(tableRenameTableInfo.getCurrentTableName());
			}
		}
		consumer.accept(tapdataEvent, getProcessResult(TapEventUtil.getTableId(tapEvent)));
	}

	@Override
	protected String getTgtTableNameFromTapEvent(TapEvent tapEvent) {
		String tableId = TapEventUtil.getTableId(tapEvent);
		TableRenameTableInfo tableRenameTableInfo = tableNameMappingMap.get(tableId);
		if (tableRenameTableInfo != null && StringUtils.isNotEmpty(tableRenameTableInfo.getCurrentTableName())) {
			return tableRenameTableInfo.getCurrentTableName();
		}
		return super.getTgtTableNameFromTapEvent(tapEvent);
	}

	protected void updateNodeConfig(TapdataEvent tapdataEvent) {
		super.updateNodeConfig(tapdataEvent);
		TapEvent tapEvent = tapdataEvent.getTapEvent();
		if (tapEvent instanceof TapCreateTableEvent || tapEvent instanceof TapDropTableEvent) {
			initTableNameMapping();
		}
	}
}
