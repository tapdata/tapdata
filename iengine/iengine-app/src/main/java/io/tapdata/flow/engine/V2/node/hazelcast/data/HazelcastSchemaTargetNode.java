package io.tapdata.flow.engine.V2.node.hazelcast.data;


import com.hazelcast.jet.core.Inbox;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.DataProcessorContext;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.TapValue;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import org.apache.commons.collections.map.LRUMap;
import org.apache.commons.collections4.MapUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;

public class HazelcastSchemaTargetNode extends HazelcastVirtualTargetNode {

	private final static Logger logger = LogManager.getLogger(HazelcastSchemaTargetNode.class);

	/**
	 * key: subTaskId+jsNodeId
	 */
	private static final Map<String, TapTable> tabTableCacheMap = new LRUMap(100);

	private final String schemaKey;

	public static TapTable getTapTable(String schemaKey) {
		TapTable tapTable = tabTableCacheMap.get(schemaKey);
		tabTableCacheMap.remove(schemaKey);
		return tapTable;
	}


	public HazelcastSchemaTargetNode(DataProcessorContext dataProcessorContext) {
		super(dataProcessorContext);
		this.schemaKey = dataProcessorContext.getSubTaskDto().getId().toHexString() + "-" + dataProcessorContext.getNode().getId();

	}

	@Override
	public void process(int ordinal, @NotNull Inbox inbox) {
		try {
			if (!inbox.isEmpty()) {
				while (isRunning()) {
					List<TapdataEvent> tapdataEvents = new ArrayList<>();
					final int count = inbox.drainTo(tapdataEvents, 1000);
					if (count > 0) {

						TapRecordEvent tapEvent;
						for (TapdataEvent tapdataEvent : tapdataEvents) {
							if (logger.isDebugEnabled()) {
								logger.debug("tapdata event [{}]", tapdataEvent.toString());
							}
							if (null != tapdataEvent.getMessageEntity()) {
								tapEvent = message2TapEvent(tapdataEvent.getMessageEntity());
							} else if (null != tapdataEvent.getTapEvent()) {
								tapEvent = (TapRecordEvent) tapdataEvent.getTapEvent();
							} else {
								continue;
							}
							// 解析模型
							Map<String, Object> after = TapEventUtil.getAfter(tapEvent);
							if (logger.isDebugEnabled()) {
								logger.info("after map is [{}]", after);
							}
							TapTable tapTable = new TapTable();
							if (MapUtils.isNotEmpty(after)) {
								for (Map.Entry<String, Object> entry : after.entrySet()) {
									if (logger.isDebugEnabled()) {
										logger.debug("entry type: {} - {}", entry.getKey(), entry.getValue().getClass());
									}
									if (entry.getValue() instanceof TapValue) {
										TapValue<?, ?> tapValue = (TapValue<?, ?>) entry.getValue();
										TapField tapField = new TapField(entry.getKey(), tapValue.getOriginType());
										tapField.setTapType(tapValue.getTapType());
										tapTable.add(tapField);
									}
								}
							}
							tabTableCacheMap.put(schemaKey, tapTable);
						}

					} else {
						break;
					}
				}
			}
		} catch (Exception e) {
			logger.error("Target process failed {}", e.getMessage(), e);
			throw sneakyThrow(e);
		}
	}
}
