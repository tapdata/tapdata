package io.tapdata.customsql;

import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.TapdataHeartbeatEvent;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.aspect.BatchReadFuncAspect;
import io.tapdata.aspect.StreamReadFuncAspect;
import io.tapdata.aspect.task.AbstractAspectTask;
import io.tapdata.aspect.task.AspectTaskSession;
import io.tapdata.customsql.util.DateTimeUtil;
import io.tapdata.customsql.util.NumberUtil;
import io.tapdata.customsql.util.StringUtil;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.codec.filter.ToTapValueCheck;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import io.tapdata.node.pdk.ConnectorNodeService;
import io.tapdata.pdk.apis.entity.QueryOperator;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.schema.TapTableMap;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections4.MapUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

@AspectTaskSession(
        includeTypes = {TaskDto.SYNC_TYPE_MIGRATE, TaskDto.SYNC_TYPE_SYNC},
        order = Integer.MIN_VALUE)
public class CustomSqlAspectTask extends AbstractAspectTask {


    public CustomSqlAspectTask() {
        observerHandlers.register(StreamReadFuncAspect.class, this::handleStreamReadFunc);
    }

    public Void handleStreamReadFunc(StreamReadFuncAspect aspect) {
        switch (aspect.getState()) {
            case BatchReadFuncAspect.STATE_START:
                if(!(aspect.getDataProcessorContext().getNode() instanceof TableNode)) return null;
                TableNode tableNode = (TableNode) aspect.getDataProcessorContext().getNode();
                if (!tableNode.getIsFilter() || CollectionUtils.isEmpty(tableNode.getConditions())) {
                    return null;
                }
                aspect.streamingProcessCompleteConsumers(events -> {
                    TapTableMap<String, TapTable> tapTableMap = aspect.getDataProcessorContext().getTapTableMap();
                    // 遍历所有events
                    ConnectorNode connectorNode = ConnectorNodeService.getInstance().getConnectorNode(aspect.getDataProcessorContext().getPdkAssociateId());
                    TapCodecsFilterManager tapCodecsFilterManager = connectorNode.getCodecsFilterManager();
                    boolean heartbeat = false;
                    List<TapdataEvent> deletedEvents = new ArrayList<>();
                    for (int index = 0; index < events.size(); index++) {
                        TapEvent tapEvent = events.get(index).getTapEvent();
                        if (tapEvent instanceof TapRecordEvent) {
                            boolean result = checkAndFilter(events.get(index), tapTableMap, tableNode, tapCodecsFilterManager);
                            TapEvent tapRecordEvent = events.get(index).getTapEvent();
                            if(tapRecordEvent instanceof TapUpdateRecordEvent){
                                if(!result) {
                                    Map<String, Object> before = ((TapUpdateRecordEvent) tapRecordEvent).getBefore();
                                    if (MapUtils.isEmpty(before)) {
                                        before = ((TapUpdateRecordEvent) tapRecordEvent).getAfter();
                                    }
                                    TapDeleteRecordEvent tapDeleteRecordEvent =
                                            TapSimplify.deleteDMLEvent(before, ((TapUpdateRecordEvent) tapRecordEvent).getTableId());
                                    tapRecordEvent.clone(tapDeleteRecordEvent);
                                    events.get(index).setTapEvent(tapDeleteRecordEvent);
                                }else {
                                    Map<String, Object> after = ((TapUpdateRecordEvent) tapRecordEvent).getAfter();
                                    TapInsertRecordEvent insertRecordEvent =
                                            TapSimplify.insertRecordEvent(after, ((TapUpdateRecordEvent) tapRecordEvent).getTableId());
                                    tapRecordEvent.clone(insertRecordEvent);
                                    events.get(index).setTapEvent(insertRecordEvent);
                                }
                                result =true;
                            }
                            if (index == events.size() - 1 && !result ) {
                                heartbeat = true;
                            }
                            if (!result) {
                                deletedEvents.add(events.get(index));
                            }
                        }
                    }

                    // 删除过滤的事件，最后一个事件如果过滤则转化为心跳事件
                    if (CollectionUtils.isNotEmpty(deletedEvents)) {
                        events.removeAll(deletedEvents);
                        if (heartbeat) {
                            TapdataEvent tapdataEvent = deletedEvents.get(deletedEvents.size() - 1);
                            events.add(TapdataHeartbeatEvent.create(TapEventUtil.getTimestamp(tapdataEvent.getTapEvent()),
                                    tapdataEvent.getStreamOffset(), tapdataEvent.getNodeIds()));
                        }
                    }
                });
            default:
                break;

        }
        return null;
    }

    public boolean checkAndFilter(TapdataEvent tapdataEvent, TapTableMap<String, TapTable> tapTableMap,
                                  TableNode tableNode,TapCodecsFilterManager tapCodecsFilterManager) {
        TapEvent tapEvent = tapdataEvent.getTapEvent();
        Map nameFieldMap = tapTableMap.get(((TapRecordEvent) tapEvent).getTableId()).getNameFieldMap();
        // 遍历所有条件
        Map<String, Object> mapRecordValue = null;
        if (tapEvent instanceof TapDeleteRecordEvent) {
            mapRecordValue = ((TapDeleteRecordEvent) tapEvent).getBefore();
        } else if (tapEvent instanceof TapUpdateRecordEvent) {
            mapRecordValue = ((TapUpdateRecordEvent) tapEvent).getAfter();
        } else if (tapEvent instanceof TapInsertRecordEvent) {
            mapRecordValue = ((TapInsertRecordEvent) tapEvent).getAfter();
        }
        return compareValue(mapRecordValue, nameFieldMap, tableNode,tapCodecsFilterManager);
    }


    public boolean compareValue(Map<String, Object> mapRecordValue, Map<String, TapField> nameFieldMap,
                                TableNode tableNode, TapCodecsFilterManager tapCodecsFilterManager) {
        List<QueryOperator> operators = tableNode.getConditions();

        AtomicBoolean filterValue = new AtomicBoolean(true);
        tapCodecsFilterManager.transformToTapValueMap(mapRecordValue, nameFieldMap, (ToTapValueCheck) (key, value) -> {
            for (QueryOperator queryOperator : operators) {
                if (key.equals(queryOperator.getKey())) {
                    if (value == null) {
                        filterValue.set(false);
                        return false;
                    }
                    if (value instanceof Number) {
                        if (!NumberUtil.compare(queryOperator, value)) {
                            filterValue.set(false);
                            return false;
                        }
                    } else if (value instanceof DateTime) {
                        if (!DateTimeUtil.compare(queryOperator, value)) {
                            filterValue.set(false);
                            return false;
                        }
                    } else if (value instanceof String) {
                        if (!StringUtil.compare(queryOperator, value)) {
                            filterValue.set(false);
                            return false;
                        }
                    }
                }
            }
            return true;
        });
        return filterValue.get();
    }


}
