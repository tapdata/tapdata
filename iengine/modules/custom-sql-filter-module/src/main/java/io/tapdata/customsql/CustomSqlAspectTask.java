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
import io.tapdata.entity.codec.TapCodecsRegistry;
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
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import io.tapdata.pdk.apis.entity.QueryOperator;
import io.tapdata.schema.TapTableMap;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;
import java.util.Map;

@AspectTaskSession(
        includeTypes = {TaskDto.SYNC_TYPE_MIGRATE, TaskDto.SYNC_TYPE_SYNC},
        order = Integer.MIN_VALUE)
public class CustomSqlAspectTask extends AbstractAspectTask {

  private StreamReadFuncAspect aspect;

  public CustomSqlAspectTask() {
    observerHandlers.register(StreamReadFuncAspect.class, this::handleStreamReadFunc);
  }

  public Void handleStreamReadFunc(StreamReadFuncAspect aspect) {
    switch (aspect.getState()) {
      case BatchReadFuncAspect.STATE_START:
        aspect.streamingProcessCompleteConsumers(events -> {
          // 不存在则直接返回
          TableNode tableNode  = (TableNode) aspect.getDataProcessorContext().getNode();
         if(!tableNode.getIsFilter() && CollectionUtils.isEmpty(tableNode.getConditions())){
            return;
          }
          TapTableMap<String, TapTable> tapTableMap = aspect.getDataProcessorContext().getTapTableMap();
          // 遍历所有events
          for (TapdataEvent tapdataEvent : events) {
            TapEvent tapEvent = tapdataEvent.getTapEvent();
            if (tapEvent instanceof TapRecordEvent) {
              checkAndFilter(tapdataEvent,tapTableMap, tableNode);
            }
          }
        });
      default:
        break;

    }
    return null;
  }

  public void checkAndFilter(TapdataEvent tapdataEvent,  TapTableMap<String, TapTable> tapTableMap, TableNode tableNode) {
    TapEvent tapEvent  = tapdataEvent.getTapEvent();
    Map<String, TapField> nameFieldMap = (Map<String, TapField>) tapTableMap.get(((TapRecordEvent) tapEvent).getTableId());
    // 遍历所有条件
    Map<String, Object> map = null;
    if (tapEvent instanceof TapDeleteRecordEvent) {
      map = ((TapDeleteRecordEvent) tapEvent).getBefore();
    } else if (tapEvent instanceof TapUpdateRecordEvent) {
      map = ((TapUpdateRecordEvent) tapEvent).getAfter();
    } else if (tapEvent instanceof TapInsertRecordEvent) {
      map = ((TapInsertRecordEvent) tapEvent).getAfter();
    }
    boolean flag =  compareValue(map, nameFieldMap, tableNode);
    if (!flag) {
      // 如果不符合则转换为心跳事件
      tapdataEvent = TapdataHeartbeatEvent.create(TapEventUtil.getTimestamp(tapdataEvent.getTapEvent()), tapdataEvent.getStreamOffset(), tapdataEvent.getNodeIds());
      return;
    }

  }


  public boolean compareValue(Map<String, Object> map, Map<String, TapField> nameFieldMap, TableNode tableNode) {
    List<QueryOperator> operators = tableNode.getConditions();
    TapCodecsFilterManager codecsFilterManager = TapCodecsFilterManager.create(TapCodecsRegistry.create());
    codecsFilterManager.transformToTapValueMap(map, nameFieldMap, (ToTapValueCheck) (key, value) -> {
      for (QueryOperator queryOperator : operators) {
        if (key.equals(queryOperator.getKey())) {
          if (value instanceof Number) {
            if (!NumberUtil.compare(queryOperator.getValue(), value, queryOperator.getOperator())) {
              return false;
            }
          } else if (value instanceof DateTime) {
            if (!DateTimeUtil.compare(queryOperator.getValue(), value, queryOperator.getOperator())) {
              return false;
            }
          } else if (value instanceof String) {
            if (!StringUtil.compare(queryOperator.getValue(), value, queryOperator.getOperator())) {
              return false;
            }
          }
        }
      }
      return true;
    });
    return true;
  }
}
