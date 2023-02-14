package io.tapdata.flow.engine.V2.node.hazelcast.processor.aggregation;

import com.google.common.collect.Lists;
import com.hazelcast.core.HazelcastInstance;
import com.tapdata.constant.MapUtil;
import com.tapdata.entity.MessageEntity;
import com.tapdata.entity.OperationType;
import com.tapdata.entity.SyncStage;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.process.AggregationProcessorNode;
import com.tapdata.tm.commons.task.dto.Aggregation;
import io.tapdata.construct.constructImpl.ConstructIMap;
import io.tapdata.construct.constructImpl.DocumentIMap;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.TapUtils;
import io.tapdata.flow.engine.V2.node.hazelcast.HazelcastBaseNode;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import io.tapdata.schema.TapTableMap;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.collections4.MapUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;
import java.util.*;

/**
 * DAG上的聚合节点的实现类（内置多个聚合器Aggregator）
 *
 * @author alexouyang
 * @date 2022/4/19
 */
public class HazelcastMultiAggregatorProcessor extends HazelcastBaseNode {

    private final Logger logger = LogManager.getLogger(HazelcastMultiAggregatorProcessor.class);

    private final List<Aggregator> aggregators = new ArrayList<>();

    private final Queue<Object> eventQueue = new LinkedList<>();

    private final String nodeId;

    List<Aggregation> rules;

    private volatile ConstructIMap<BigDecimal> cacheNumbers;

    private volatile ConstructIMap<List<BigDecimal>> cacheList;

    private final List<String> targetFieldsName = new ArrayList<>();

    private TapRecordEvent originalTapRecordEvent;

    private TapValueTransform tapValueTransform;

    public HazelcastMultiAggregatorProcessor(ProcessorBaseContext processorBaseContext) {
        super(processorBaseContext);
        Node<?> node = processorBaseContext.getNode();
        TapTableMap<String, TapTable> tapTableMap = processorBaseContext.getTapTableMap();
        String tableName = tapTableMap.keySet().iterator().next();
        TapTable tapTable = tapTableMap.get(tableName);
        targetFieldsName.addAll(tapTable.getNameFieldMap().keySet());
        AggregationProcessorNode aggNode = (AggregationProcessorNode) node;
        nodeId = aggNode.getId();
        rules = aggNode.getAggregations();
    }

    private void initCache(String nodeId, HazelcastInstance hazelcastInstance) {
        cacheNumbers = new DocumentIMap<>(hazelcastInstance,
                nodeId + "-" + "AggregatorCache",
                externalStorageDto);
        cacheList = new DocumentIMap<>(hazelcastInstance,
                nodeId + "-" + "AggregatorCacheList",
                externalStorageDto);
    }

    public static void clearCache(String nodeId, HazelcastInstance hazelcastInstance) {
        ConstructIMap<BigDecimal> cacheNumbers = new ConstructIMap<>(hazelcastInstance, nodeId + "-" + "AggregatorCache");
        ConstructIMap<ArrayList<BigDecimal>> cacheList = new ConstructIMap<>(hazelcastInstance, nodeId + "-" + "AggregatorCacheList");

        try {
            cacheNumbers.clear();
        } catch (Exception e) {
            throw new RuntimeException("Clear aggregate cache failed, name: " + cacheNumbers.getName() + "(" + cacheNumbers.getType() + "), error: " + e.getMessage(), e);
        }
        try {
            cacheList.clear();
        } catch (Exception e) {
            throw new RuntimeException("Clear aggregate cache list failed, name: " + cacheList.getName() + "(" + cacheList.getType() + "), error: " + e.getMessage(), e);
        }
    }

    /***
     * 根据用户页面配置Aggregator聚合器
     *
     */
    private void initPipeline(List<Aggregation> rules) {
        final Node<?> curNode = processorBaseContext.getNode();
        final String nodeId = curNode.getId();

        for (int i = 0; i < rules.size(); i++) {
            Aggregation rule = rules.get(i);
            aggregators.add(new Aggregator(rule, nodeId, i + 1));
        }
        if (aggregators.size() == 1) {
            Aggregator aggregator = aggregators.get(0);
            aggregator.wrapItem().deleteUnRelatedField().groupBy().rollingAggregate().cdcUpdate().finish();
            return;
        }
        for (int i = 0; i < aggregators.size(); i++) {
            Aggregator current = aggregators.get(i);
            if (i == aggregators.size() - 1) {
                current.deleteUnRelatedField().groupBy().rollingAggregate().cdcUpdate().finish();
            } else {
                Aggregator next = aggregators.get(i + 1);
                if (i == 0) {
                    current.wrapItem().deleteUnRelatedField().groupBy().rollingAggregate().cdcUpdate().next(next);
                } else {
                    current.deleteUnRelatedField().groupBy().rollingAggregate().cdcUpdate().next(next);
                }
            }
        }
    }

    @Override
    public void doInit(@NotNull Context context) throws Exception {
        // 判空
        logger.info("init aggregator processor, nodeId:{}", nodeId);
        if (rules != null && !rules.isEmpty()) {
            initCache(nodeId, context.hazelcastInstance());

            initPipeline(rules);
            for (Aggregator aggregator : aggregators) {
                aggregator.init();
            }
        } else {
            logger.warn("Aggregation DAG config error.");
        }
    }

    @Override
    public void doClose() throws Exception {
        for (Aggregator aggregator : aggregators) {
            aggregator.close();
        }
        logger.info("close aggregator, nodeId: {}", nodeId);
    }

    @Override
    protected boolean tryProcess(int ordinal, @NotNull Object item) {
        logger.info("try process aggregator, nodeId: {}", nodeId);
        Node<?> node = processorBaseContext.getNode();
        if (logger.isDebugEnabled()) {
            logger.debug(
                    "aggregator node [id: {}, name: {}] receive event {}",
                    node.getId(),
                    node.getName(),
                    item
            );
        }

        TapdataEvent tapdataEvent;
        if (item instanceof TapdataEvent) {
            tapdataEvent = (TapdataEvent) item;
        } else {
            return true;
        }
        if (null == tapdataEvent.getMessageEntity() && !(tapdataEvent.getTapEvent() instanceof TapRecordEvent)) {
            while (isRunning()) {
                if (offer(tapdataEvent)) break;
            }
            return true;
        }
        tapValueTransform = transformFromTapValue(tapdataEvent);
        if (tapdataEvent.getTapEvent() instanceof TapRecordEvent) {
            originalTapRecordEvent = (TapRecordEvent) tapdataEvent.getTapEvent();
        }

        // initialize input
        Object initialInput = tapdataEvent;
        eventQueue.offer(initialInput);
        for (Aggregator aggregator : aggregators) {
            final List<AggregatorProcessorBase> processors = aggregator.getProcessors();
            for (AggregatorProcessorBase processor : processors) {
                int queueSize = eventQueue.size();
                for (int i = 0; i < queueSize; i++) {
                    Object inputItem = eventQueue.poll();
                    List<Object> outputItems = processor.tryProcess(inputItem);
                    for (Object outputItem : outputItems) {
                        eventQueue.offer(outputItem);
                    }
                }
            }
        }
        return true;
    }

    public abstract static class AggregatorProcessorBase {

        protected AggregatorProcessorBase next;

        /**
         * 逻辑处理器类
         * @param item
         * @return
         */
        public abstract List<Object> tryProcess(final Object item);
    }

    /**
     * 聚合器类Aggregator
     * 聚合器Aggregator可以串联，且每个聚合器都是独立的
     */
    @Getter
    @Setter
    public final class Aggregator {

        private final String name;

        private final Aggregation rule;

        private final List<String> groupbyList;

        private boolean running = false;

        private final int pollTimeout = 1000;

        private final List<AggregatorProcessorBase> processors = new ArrayList<>();

        public Aggregator(Aggregation rule, String nodeId, int index) {
            this.rule = rule;
            this.groupbyList = this.rule.getGroupByExpression();
            this.name = nodeId + "-" + rule.getAggFunction() + "-" + index;
        }

        public void init() {
            // inner chain
            for (int i = 0; i < processors.size() - 1; i++) {
                processors.get(i).next = processors.get(i + 1);
            }
        }

        public void close() {
            for (AggregatorProcessorBase processor : processors) {
                if (processor instanceof RollingAggregateP) {
                    ((RollingAggregateP) processor).closeBucketThreads();
                }
            }
        }


        private Aggregator deleteUnRelatedField() {
            processors.add(new DeleteUnRelatedField(rule));
            return this;
        }

        private Aggregator wrapItem() {
            processors.add(new WrappedItem());
            return this;
        }

        private Aggregator crossGrouping() {
            processors.add(new CrossGroupP(groupbyList));
            return this;
        }

        private Aggregator groupBy() {
            processors.add(new GroupByP(groupbyList));
            return this;
        }

        private Aggregator rollingAggregate() {
            processors.add(new RollingAggregateP());
            return this;
        }

        private Aggregator cdcUpdate() {
            processors.add(new CdcUpdateP());
            return this;
        }

        private void next(Aggregator next) {
            processors.add(new NextP(next));
        }

        private void finish() {
            processors.add(new FinishP());
        }

        /***
         * 删除不输出的字段，可以提速10%
         */
        public class DeleteUnRelatedField extends AggregatorProcessorBase {
            private final Aggregation rule;

            public DeleteUnRelatedField(Aggregation rule) {
                this.rule = rule;
            }

            @Override
            public List<Object> tryProcess(Object item) {
                if (item == null) {
                    return Collections.emptyList();
                }

                WrapItem wrappedItem = (WrapItem) item;
                Object event = wrappedItem.getMessage();
                if (event instanceof MessageEntity) {
                    deleteUnRelatedFieldPOld((MessageEntity) event);
                } else if (event instanceof TapRecordEvent) {
                    deleteUnRelatedFieldP((TapRecordEvent) event);
                } else {
                    throw new RuntimeException("Unimplemented message type");
                }
                return Lists.newArrayList(item);
            }


            private void deleteUnRelatedFieldPOld(MessageEntity messageEntity) {
                List<String> reserveFieldList = getReverseFieldList();

                if (messageEntity.getBefore() != null) {
                    try {
                        Map<String, Object> copy = new HashMap<>(messageEntity.getBefore().size());
                        MapUtil.deepCloneMap(messageEntity.getBefore(), copy);
                        for (String key : messageEntity.getBefore().keySet()) {
                            if (!reserveFieldList.contains(key)) {
                                copy.remove(key);
                            }
                        }
                        messageEntity.setBefore(copy);
                    } catch (Exception e) {
                        logger.error("deep copy map error ", e);
                    }

                }
                if (messageEntity.getAfter() != null) {
                    try {
                        Map<String, Object> copy = new HashMap<>(messageEntity.getAfter().size());
                        MapUtil.deepCloneMap(messageEntity.getAfter(), copy);
                        for (String key : messageEntity.getAfter().keySet()) {
                            if (!reserveFieldList.contains(key)) {
                                copy.remove(key);
                            }
                        }
                        messageEntity.setAfter(copy);
                    } catch (Exception e) {
                        logger.error("deep copy map error ", e);
                    }
                }
            }

            private void deleteUnRelatedFieldP(TapRecordEvent event) {
                List<String> reserveFieldList = getReverseFieldList();
                Map<String, Object> before = TapEventUtil.getBefore(event);
                deleteUnRelatedFields(before, reserveFieldList, event, true);
                Map<String, Object> after = TapEventUtil.getAfter(event);
                deleteUnRelatedFields(after, reserveFieldList, event, false);
            }

            private List<String> getReverseFieldList() {
                List<String> reserveFieldList = new ArrayList<>();
                if (rule.getAggExpression() != null) {
                    reserveFieldList.add(rule.getAggExpression());
                }
                if (rule.getFilterPredicate() != null) {
                    reserveFieldList.add(rule.getFilterPredicate());
                }
                if (rule.getGroupByExpression() != null) {
                    reserveFieldList.addAll(rule.getGroupByExpression());
                }
                return reserveFieldList;
            }

            private void deleteUnRelatedFields(Map<String, Object> map, List<String> reserveFieldList, TapRecordEvent event, boolean isBefore) {
                if (map != null) {
                    try {
                        Map<String, Object> copy = new HashMap<>(map.size());
                        MapUtil.deepCloneMap(map, copy);
                        for (String key : map.keySet()) {
                            if (!reserveFieldList.contains(key)) {
                                copy.remove(key);
                            }
                        }
                        if (isBefore) {
                            TapEventUtil.setBefore(event, copy);
                        } else {
                            TapEventUtil.setAfter(event, copy);
                        }
                    } catch (Exception e) {
                        logger.error("Deep copy map error ", e);
                    }
                }
            }
        }

        /***
         * 包装消息对象
         */
        public class WrappedItem extends AggregatorProcessorBase {

            public List<Object> tryProcess(Object item) {
                final List<Object> result = Lists.newArrayList();
                if (item == null) {
                    return Collections.emptyList();
                }

                TapdataEvent event = (TapdataEvent) item;
                if (event.getMessageEntity() != null) {
                    MessageEntity messageEntity = ((TapdataEvent) item).getMessageEntity();
                    final OperationType operationType = OperationType.fromOp(messageEntity.getOp());
                    if (operationType != OperationType.INSERT &&
                            operationType != OperationType.UPDATE &&
                            operationType != OperationType.DELETE) {
                        logger.info("no need to process for aggregator:" + operationType);
                        return Collections.emptyList();
                    }
                    WrapItem wrappedItem = new WrapItem();
                    event.setMessageEntity(null);
                    wrappedItem.setEvent(event);
                    wrappedItem.setMessage(messageEntity);
                    result.add(wrappedItem);
                } else if (event.getTapEvent() != null) {
                    TapRecordEvent tapRecordEvent = (TapRecordEvent) ((TapdataEvent) item).getTapEvent();
                    WrapItem wrappedItem = new WrapItem();
                    event.setTapEvent(null);
                    wrappedItem.setEvent(event);
                    wrappedItem.setMessage(tapRecordEvent);
                    result.add(wrappedItem);
                } else {
                    throw new RuntimeException("Unimplemented message type");
                }
                return result;
            }
        }

        /**
         * 如果客户的消息是update，而且针对的是group by 内的字段，需要特殊处理一下
         */
        public class CrossGroupP extends AggregatorProcessorBase {

            private final List<String> groupbyList;

            public CrossGroupP(List<String> groupbyList) {
                this.groupbyList = groupbyList;
            }

            @Override
            public List<Object> tryProcess(Object item) {
                if (item == null) {
                    return Collections.emptyList();
                }
                WrapItem wrappedItem = (WrapItem) item;
                if (groupbyList == null || groupbyList.isEmpty()) {
                    return Lists.newArrayList(item);
                } else {
                    List<Object> result;
                    Object event = wrappedItem.getMessage();
                    if (event instanceof MessageEntity) {
                        result = crossGroucrpByOldP((MessageEntity) event, wrappedItem);
                    } else if (event instanceof TapRecordEvent) {
                        result = crossGroupByP((TapRecordEvent) event, wrappedItem);
                    } else {
                        throw new RuntimeException("Unimplemented message type");
                    }
                    return result;
                }
            }

            private List<Object> crossGroucrpByOldP(MessageEntity messageEntity, WrapItem wrappedItem) {
                final OperationType operationType = OperationType.fromOp(messageEntity.getOp());
                final List<Object> result = Lists.newArrayList();
                // 从一个group到另外一个group不能同时设置多个group by keys
                if (groupbyList.size() > 1) {
                    result.add(wrappedItem);
                    return result;
                }
                String groupKey = groupbyList.get(0);
                if (operationType == OperationType.UPDATE) {
                    Object beforeGroup = messageEntity.getBefore().get(groupKey);
                    Object afterGroup = messageEntity.getAfter().get(groupKey);
                    if (beforeGroup != null && afterGroup != null && !beforeGroup.equals(afterGroup)) {
                        // split into two events
                        MessageEntity delete = (MessageEntity) messageEntity.clone();
                        delete.setOp("d");
                        delete.setAfter(null);
                        WrapItem deleteWrap = wrappedItem.clone();
                        deleteWrap.setMessage(delete);
                        result.add(deleteWrap);

                        MessageEntity insert = (MessageEntity) messageEntity.clone();
                        insert.setOp("i");
                        insert.setBefore(null);
                        WrapItem insertWrap = wrappedItem.clone();
                        insertWrap.setMessage(insert);
                        result.add(insertWrap);
                    } else {
                        throw new RuntimeException("Unimplemented code!");
                    }
                } else if (operationType == OperationType.DELETE
                        || operationType == OperationType.INSERT) {
                    WrapItem otherWrap = wrappedItem.clone();
                    otherWrap.setMessage(messageEntity);
                    result.add(otherWrap);
                } else {
                    logger.debug("Event has unsupported op for aggregators:" + messageEntity.getOp());
                }
                return result;
            }

            private List<Object> crossGroupByP(TapRecordEvent tapRecordEvent, WrapItem wrappedItem) {
                final List<Object> result = Lists.newArrayList();
                // 从一个group到另外一个group不能同时设置多个group by keys
                if (groupbyList.size() > 1) {
                    result.add(wrappedItem);
                    return result;
                }
                String groupKey = groupbyList.get(0);
                if (tapRecordEvent instanceof TapUpdateRecordEvent) {
                    Object beforeUid = ((TapUpdateRecordEvent) tapRecordEvent).getBefore().get(groupKey);
                    Object afterUid = ((TapUpdateRecordEvent) tapRecordEvent).getAfter().get(groupKey);
                    if (beforeUid != null && afterUid != null && !beforeUid.equals(afterUid)) {
                        // split into two events
                        TapDeleteRecordEvent cloneDelete = new TapDeleteRecordEvent();
                        tapRecordEvent.clone(cloneDelete);
                        cloneDelete.setBefore(InstanceFactory.instance(TapUtils.class).cloneMap(((TapUpdateRecordEvent) tapRecordEvent).getBefore()));
                        WrapItem deleteWrap = wrappedItem.clone();
                        deleteWrap.setMessage(cloneDelete);
                        result.add(deleteWrap);

                        TapInsertRecordEvent cloneInsert = new TapInsertRecordEvent();
                        tapRecordEvent.clone(cloneInsert);
                        cloneInsert.setAfter(InstanceFactory.instance(TapUtils.class).cloneMap(((TapUpdateRecordEvent) tapRecordEvent).getAfter()));
                        WrapItem insertWrap = wrappedItem.clone();
                        insertWrap.setMessage(cloneInsert);
                        result.add(insertWrap);
                    } else {
                        throw new RuntimeException("Unimplemented code!");
                    }
                } else if (tapRecordEvent instanceof TapDeleteRecordEvent
                        || tapRecordEvent instanceof TapInsertRecordEvent) {
                    WrapItem otherWrap = wrappedItem.clone();
                    otherWrap.setMessage(tapRecordEvent);
                    result.add(otherWrap);
                } else {
                    logger.debug("Unsupported op for aggregator:" + tapRecordEvent.getClass().getSimpleName());
                }
                return result;
            }
        }

        /**
         * 分组处理器
         */
        public class GroupByP extends AggregatorProcessorBase {
            private final List<String> groupbyList;

            private String aggregatorOp;

            public GroupByP(List<String> groupbyList) {
                this.groupbyList = groupbyList;

                if (rule != null && rule.getAggFunction() != null) {
                    aggregatorOp = rule.getAggFunction();
                } else {
                    throw new RuntimeException("Aggregator rule is null!");
                }
            }

            @Override
            public List<Object> tryProcess(Object item) {
                if (item == null) {
                    return Collections.emptyList();
                }
                if (groupbyList == null || groupbyList.isEmpty()) {
                    return Lists.newArrayList(item);
                }

                WrapItem wrappedItem = (WrapItem) item;
                Object event = wrappedItem.getMessage();
                if (event == null) {
                    logger.error("event is null");
                    return Collections.emptyList();
                }
                final List<Object> result;
                try {
                    if (event instanceof MessageEntity) {
                        result = groupByPOld((MessageEntity) event, wrappedItem);
                    } else if (event instanceof TapRecordEvent) {
                        result = groupByP((TapRecordEvent) event, wrappedItem);
                    } else {
                        throw new RuntimeException("Unimplemented message type");
                    }
                } catch (Exception e) {
                    logger.error("GroupP error:", e);
                    throw new RuntimeException("Unimplemented case");
                }
                return result;
            }

            private List<Object> groupByPOld(MessageEntity event, WrapItem wrappedItem) throws Exception {
                final List<Object> result = Lists.newArrayList();
                final OperationType operationType = OperationType.fromOp(event.getOp());
                    if (operationType == OperationType.UPDATE) {
                        //split into two events
                        MessageEntity delete = (MessageEntity) event.clone();
                        delete.setOp("d");
                        delete.setAfter(null);
                        WrapItem deleteWrap = wrappedItem.clone();
                        deleteWrap.setMessage(delete);
                        String cachedGroupByKey1 = concatGroupByKeys(event.getBefore(), groupbyList);
                        deleteWrap.setCachedGroupByKey(cachedGroupByKey1);
                        result.add(deleteWrap);

                        MessageEntity insert = (MessageEntity) event.clone();
                        insert.setOp("i");
                        insert.setBefore(null);
                        WrapItem insertWrap = wrappedItem.clone();
                        insertWrap.setMessage(insert);
                        String cachedGroupByKey2 = concatGroupByKeys(event.getAfter(), groupbyList);
                        insertWrap.setCachedGroupByKey(cachedGroupByKey2);
                        result.add(insertWrap);
                    } else if (operationType == OperationType.INSERT) {
                        String cachedGroupByKey = concatGroupByKeys(event.getAfter(), groupbyList);
                        // 第一次insert事件传到target仍为insert，后续insert传到target转为update
                        if (checkCacheKeyIfExist(cachedGroupByKey, aggregatorOp)) {
                            MessageEntity update = (MessageEntity) event.clone();
                            update.setOp("u");
                            update.setBefore(null);
                            WrapItem updateWrap = wrappedItem.clone();
                            updateWrap.setMessage(update);
                            updateWrap.setCachedGroupByKey(cachedGroupByKey);
                            result.add(updateWrap);
                        } else {
                            wrappedItem.setCachedGroupByKey(cachedGroupByKey);
                            result.add(wrappedItem);
                        }
                    } else {
                        String cachedGroupByKey = concatGroupByKeys(event.getBefore(), groupbyList);
                        wrappedItem.setCachedGroupByKey(cachedGroupByKey);
                        result.add(wrappedItem);
                    }
                    return result;
            }

            private List<Object> groupByP(TapRecordEvent event, WrapItem wrappedItem) throws Exception {
                final List<Object> result = Lists.newArrayList();
                Map<String, Object> before = TapEventUtil.getBefore(event);
                Map<String, Object> after = TapEventUtil.getAfter(event);
                if (event instanceof TapUpdateRecordEvent) {
                    final List<Object> splitEvents = handleUpdateTapEvent(event, before, after, wrappedItem);
                    result.addAll(splitEvents);
                } else if (event instanceof TapInsertRecordEvent) {
                    String cachedGroupByKey = concatGroupByKeys(after, groupbyList);
                    // insertEvent转化为能造成counter变化的updateEvent
                    if (checkCacheKeyIfExist(cachedGroupByKey, aggregatorOp)) {
                        TapUpdateRecordEvent cloneUpdate = new TapUpdateRecordEvent();
                        event.clone(cloneUpdate);
                        cloneUpdate.setAfter(InstanceFactory.instance(TapUtils.class).cloneMap(after));
                        WrapItem updateWrap = wrappedItem.clone();
                        updateWrap.setChangedCount(BigDecimal.ONE);
                        updateWrap.setMessage(cloneUpdate);
                        updateWrap.setCachedGroupByKey(cachedGroupByKey);
                        result.add(updateWrap);
                    } else {
                        wrappedItem.setCachedGroupByKey(cachedGroupByKey);
                        wrappedItem.setChangedCount(BigDecimal.ONE);
                        result.add(wrappedItem);
                    }
                } else {
                    String cachedGroupByKey = concatGroupByKeys(before, groupbyList);
                    if (checkCacheKeyIfExist(cachedGroupByKey, aggregatorOp)) {
                        TapUpdateRecordEvent cloneUpdate = new TapUpdateRecordEvent();
                        event.clone(cloneUpdate);
                        // todo by dayun 因为源端传过来的deleteEvent没有after，所以这里转成update事件只需要从before获取groupKey就好，让target知道删除哪一行
                        cloneUpdate.setAfter(InstanceFactory.instance(TapUtils.class).cloneMap(before));
                        cloneUpdate.setBefore(InstanceFactory.instance(TapUtils.class).cloneMap(before));
                        WrapItem updateWrap = wrappedItem.clone();
                        updateWrap.setChangedCount(BigDecimal.ONE.negate());
                        updateWrap.setMessage(cloneUpdate);
                        updateWrap.setCachedGroupByKey(cachedGroupByKey);
                        result.add(updateWrap);
                    } else {
                        wrappedItem.setCachedGroupByKey(cachedGroupByKey);
                        wrappedItem.setChangedCount(BigDecimal.ONE.negate());
                        result.add(wrappedItem);
                    }
                }
                return result;
            }

            private List<Object> handleUpdateTapEvent(final TapRecordEvent event, final Map<String, Object> before, final Map<String, Object> after, final WrapItem wrappedItem) {
                final List<Object> result = Lists.newArrayList();
                if (MapUtils.isEmpty(before) || MapUtils.isEmpty(after)) {
                    // before 有可能是空的，例如目前mongo source connector没有提供before，所以暂时无法彻底解决groupByKey的值有变化的情况
                    logger.error("data source does not provide enough info, tableId:{}", event.getTableId());
                    throw new RuntimeException("Unimplemented case");
                }
                if (checkGroupByKeyChanged(before, after)) {
                    result.addAll(splitTapUpdateEventAsFourEvents(event, before, after, wrappedItem));
                } else {
                    //split into two events
                    result.addAll(splitTapUpdateEventAsTwoEvents(event, before, after, wrappedItem));
                }
                return result;
            }

            private List<Object> splitTapUpdateEventAsFourEvents(final TapRecordEvent event, final Map<String, Object> before, final Map<String, Object> after, final WrapItem wrappedItem) {
                final List<Object> result = Lists.newArrayList();
                String cachedGroupByKeyBefore = concatGroupByKeys(before, groupbyList);
                TapDeleteRecordEvent cloneDeleteBefore = new TapDeleteRecordEvent();
                event.clone(cloneDeleteBefore);
                cloneDeleteBefore.setBefore(InstanceFactory.instance(TapUtils.class).cloneMap(before));
                WrapItem deleteWrapBefore = wrappedItem.clone();
                deleteWrapBefore.setChangedCount(BigDecimal.ONE.negate());
                deleteWrapBefore.setMessage(cloneDeleteBefore);
                deleteWrapBefore.setCachedGroupByKey(cachedGroupByKeyBefore);
                result.add(deleteWrapBefore);

                TapInsertRecordEvent cloneInsertBefore = new TapInsertRecordEvent();
                event.clone(cloneInsertBefore);
                cloneInsertBefore.setAfter(InstanceFactory.instance(TapUtils.class).cloneMap(before));
                WrapItem insertWrapBefore = wrappedItem.clone();
                insertWrapBefore.setMessage(cloneInsertBefore);
                insertWrapBefore.setCachedGroupByKey(cachedGroupByKeyBefore);
                result.add(insertWrapBefore);

                String cachedGroupByKeyAfter = concatGroupByKeys(after, groupbyList);
                TapDeleteRecordEvent cloneDeleteAfter = new TapDeleteRecordEvent();
                event.clone(cloneDeleteAfter);
                cloneDeleteAfter.setBefore(InstanceFactory.instance(TapUtils.class).cloneMap(after));
                WrapItem deleteWrapAfter = wrappedItem.clone();
                deleteWrapAfter.setMessage(cloneDeleteAfter);
                deleteWrapAfter.setCachedGroupByKey(cachedGroupByKeyAfter);
                result.add(deleteWrapAfter);

                TapInsertRecordEvent cloneInsertAfter = new TapInsertRecordEvent();
                event.clone(cloneInsertAfter);
                cloneInsertAfter.setAfter(InstanceFactory.instance(TapUtils.class).cloneMap(after));
                WrapItem insertWrapAfter = wrappedItem.clone();
                insertWrapAfter.setChangedCount(BigDecimal.ONE);
                insertWrapAfter.setMessage(cloneInsertAfter);
                insertWrapAfter.setCachedGroupByKey(cachedGroupByKeyAfter);
                result.add(insertWrapAfter);
                return result;
            }

            private List<Object> splitTapUpdateEventAsTwoEvents(final TapRecordEvent event, final Map<String, Object> before, final Map<String, Object> after, final WrapItem wrappedItem) {
                final List<Object> result = Lists.newArrayList();
                TapDeleteRecordEvent cloneDelete = new TapDeleteRecordEvent();
                event.clone(cloneDelete);
                cloneDelete.setBefore(InstanceFactory.instance(TapUtils.class).cloneMap(before));
                WrapItem deleteWrap = wrappedItem.clone();
                deleteWrap.setMessage(cloneDelete);
                deleteWrap.setChangedCount(BigDecimal.ZERO);
                String cachedGroupByKey1 = concatGroupByKeys(before, groupbyList);
                deleteWrap.setCachedGroupByKey(cachedGroupByKey1);
                result.add(deleteWrap);

                TapInsertRecordEvent cloneInsert = new TapInsertRecordEvent();
                event.clone(cloneInsert);
                cloneInsert.setAfter(InstanceFactory.instance(TapUtils.class).cloneMap(after));
                WrapItem insertWrap = wrappedItem.clone();
                insertWrap.setMessage(cloneInsert);
                insertWrap.setChangedCount(BigDecimal.ZERO);
                String cachedGroupByKey2 = concatGroupByKeys(after, groupbyList);
                insertWrap.setCachedGroupByKey(cachedGroupByKey2);
                result.add(insertWrap);
                return result;
            }

            /**
             * 检查更新前后是否有修改了groupKey的值
             * @param before
             * @param after
             * @return
             */
            private boolean checkGroupByKeyChanged(final Map<String, Object> before, final Map<String, Object> after) {
                for (final String columnName : groupbyList) {
                    Object columnValBefore = before.get(columnName);
                    Object columnValAfter = after.get(columnName);
                    if (!columnValBefore.equals(columnValAfter)) {
                        return true;
                    }
                }
                return false;
            }

        } // end class GroupByP

        /**
         * 真正的聚合逻辑
         */
        public class RollingAggregateP extends AggregatorProcessorBase {

            private final String aggregatorField;

            private final String aggregatorOp;

            public RollingAggregateP() {
                if (rule != null && rule.getAggFunction() != null) {
                    aggregatorOp = rule.getAggFunction();
                    // COUNT 比较特别，它的目标字段为空，特殊处理一下
                    if (!"COUNT".equalsIgnoreCase(aggregatorOp)) {
                        aggregatorField = rule.getAggExpression();
                    } else {
                        aggregatorField = null;
                    }
                } else {
                    throw new RuntimeException("Aggregator rule is null!");
                }
            }

            public void closeBucketThreads() {
                try {
                    logger.debug("close BucketWorkers...");
                } catch (Exception ignore) {
                }
            }

            @Override
            public List<Object> tryProcess(Object item) {
                if (item == null) {
                    return Collections.emptyList();
                }
                try {
                    WrapItem wrappedItem = (WrapItem) item;
                    Object event = wrappedItem.getMessage();
                    if (event == null) {
                        logger.error("event is null");
                        return Collections.emptyList();
                    } else {
                        String cacheKey = wrappedItem.getCachedGroupByKey();
                        // 用户选择不分组
                        if (cacheKey == null || cacheKey.isEmpty()) {
                            cacheKey = "global_aggregator_" + name;
                            wrappedItem.setCachedGroupByKey(cacheKey);
                        }
                        switch (aggregatorOp) {
                            case "COUNT":
                                if (wrappedItem.isEvent()) {
                                    boolean rs = AggregateOps.count(name, cacheNumbers, wrappedItem, wrappedItem.isMessageEntity());

                                    return rs ? Lists.newArrayList(wrappedItem) : Collections.emptyList();
                                } else {
                                    throw new RuntimeException("Unimplemented type: " + event.getClass().getSimpleName());
                                }
                            case "SUM":
                                if (wrappedItem.isEvent()) {
                                    boolean rs = AggregateOps.sum(name, aggregatorField, cacheNumbers, wrappedItem, wrappedItem.isMessageEntity());
                                    return rs ? Lists.newArrayList(wrappedItem) : Collections.emptyList();
                                } else {
                                    throw new RuntimeException("Unimplemented type: " + event.getClass().getSimpleName());
                                }
                            case "AVG":
                                if (wrappedItem.isEvent()) {
                                    boolean rs = AggregateOps.avg(name, aggregatorField, cacheNumbers, wrappedItem, wrappedItem.isMessageEntity());
                                    return rs ? Lists.newArrayList(wrappedItem) : Collections.emptyList();
                                } else {
                                    throw new RuntimeException("Unimplemented type: " + event.getClass().getSimpleName());
                                }
                            case "MAX":
                                if (wrappedItem.isEvent()) {
                                    boolean rs = AggregateOps.max(aggregatorField, cacheNumbers, cacheList, wrappedItem, wrappedItem.isMessageEntity());
                                    return rs ? Lists.newArrayList(wrappedItem) : Collections.emptyList();
                                } else {
                                    throw new RuntimeException("Unimplemented type: " + event.getClass().getSimpleName());
                                }
                            case "MIN":
                                if (wrappedItem.isEvent()) {
                                    boolean rs = AggregateOps.min(aggregatorField, cacheNumbers, cacheList, wrappedItem, wrappedItem.isMessageEntity());
                                    return rs ? Lists.newArrayList(wrappedItem) : Collections.emptyList();
                                } else {
                                    throw new RuntimeException("Unimplemented type");
                                }
                            default:
                                throw new RuntimeException("Unimplemented aggregatorOp type");
                        } // end switch

                    }
                } catch (Exception e) {
                    logger.error("RollingAggregateP error:", e);
                }
                return Collections.emptyList();
            }
        }

        /***
         * 串联Aggregator
         */
        public class NextP extends AggregatorProcessorBase {

            private final Aggregator nextAggregator;

            public NextP(Aggregator nextAggregator) {
                this.nextAggregator = nextAggregator;
            }

            @Override
            public List<Object> tryProcess(Object item) {
                return nextAggregator.processors.get(0).tryProcess(item);
            }
        }

        /**
         * 后处理cdc过程中update事件转化而来的delete+insert事件，delete事件直接丢弃，insert事件转换回update事件
         * 初次同步跳过
         */
        public class CdcUpdateP extends AggregatorProcessorBase {
            public CdcUpdateP() {}
            @Override
            public List<Object> tryProcess(Object item) {

                WrapItem wrappedItem = (WrapItem) item;
                if (wrappedItem.getEvent() == null || wrappedItem.getEvent().getSyncStage() != SyncStage.CDC) {
                    return Lists.newArrayList(item);
                }
                Object event = wrappedItem.getMessage();
                BigDecimal changedCounter = wrappedItem.getChangedCount();
                if (event == null) {
                    logger.error("event is null");
                    return Collections.emptyList();
                }
                try {
                    if (event instanceof MessageEntity) {
                        return Lists.newArrayList(item);
                    } else if (event instanceof TapRecordEvent) {
                        if (changedCounter.compareTo(BigDecimal.ZERO) != 0) {
                            return Lists.newArrayList(item);
                        }
                        TapRecordEvent tapRecordEvent = (TapRecordEvent) event;
                        if (tapRecordEvent instanceof TapInsertRecordEvent) {
                            TapInsertRecordEvent insertRecordEvent = (TapInsertRecordEvent) tapRecordEvent;
                            Map<String, Object> after = insertRecordEvent.getAfter();
                            TapUpdateRecordEvent cloneUpdate = new TapUpdateRecordEvent();
                            tapRecordEvent.clone(cloneUpdate);
                            cloneUpdate.setAfter(InstanceFactory.instance(TapUtils.class).cloneMap(after));
                            WrapItem updateWrap = wrappedItem.clone();
                            updateWrap.setMessage(cloneUpdate);
                            updateWrap.setChangedCount(BigDecimal.ZERO);
                            return Lists.newArrayList(updateWrap);
                        } else if (tapRecordEvent instanceof TapDeleteRecordEvent) {
                            return Collections.emptyList();
                        } else {
                            throw new RuntimeException("Unimplemented message type");
                        }
                    }
                } catch (Exception e) {
                    logger.error("GroupP error:", e);
                    throw new RuntimeException("Unimplemented case");
                }
                return Collections.emptyList();
            }
        }

        /***
         * 所有Aggregator都处理完了
         * 对外输出
         */
        public class FinishP extends AggregatorProcessorBase {

            @Override
            public List<Object> tryProcess(Object item) {
                if (item == null) {
                    return Collections.emptyList();
                }
                if (item instanceof WrapItem) {
                    WrapItem wrappedItem = (WrapItem) item;
                    TapdataEvent event = wrappedItem.getEvent();
                    if (wrappedItem.getCachedRollingAggregateCounter() == null) {
                        logger.error("Unexpected null");
                    }
                    if (wrappedItem.getMessage() instanceof MessageEntity) {
                        MessageEntity messageEntity = (MessageEntity) wrappedItem.getMessage();
                        schemaFilter(messageEntity.getBefore(), messageEntity.getAfter());
                        // delete 事件特殊处理,否则delete事件把目标结果直接删了
                        if (wrappedItem.getCachedRollingAggregateCounter().compareTo(BigDecimal.ZERO) != 0) {
                            final OperationType operationType = OperationType.fromOp(messageEntity.getOp());
                            if (operationType == OperationType.INSERT || operationType == OperationType.DELETE) {
                                messageEntity.setOp("i");
                            } else {
                                messageEntity.setOp("u");
                            }
                        }
                        event.setMessageEntity(messageEntity);
                    } else if (((WrapItem) item).getMessage() instanceof TapRecordEvent) {
                        TapRecordEvent tapRecordEvent = (TapRecordEvent) wrappedItem.getMessage();
                        schemaFilter(TapEventUtil.getBefore(tapRecordEvent), TapEventUtil.getAfter(tapRecordEvent));
                        if (wrappedItem.getCachedRollingAggregateCounter().compareTo(BigDecimal.ZERO) != 0) {
                            if (tapRecordEvent instanceof TapDeleteRecordEvent) {
                                TapDeleteRecordEvent cloneDelete = new TapDeleteRecordEvent();
                                tapRecordEvent.clone(cloneDelete);
                                cloneDelete.setBefore(((TapDeleteRecordEvent) tapRecordEvent).getBefore());
                                cloneDelete.setReferenceTime(originalTapRecordEvent.getReferenceTime());
                                event.setTapEvent(cloneDelete);
                            } else if (tapRecordEvent instanceof TapInsertRecordEvent) {
                                TapInsertRecordEvent cloneInsert = new TapInsertRecordEvent();
                                tapRecordEvent.clone(cloneInsert);
                                cloneInsert.setAfter(((TapInsertRecordEvent) tapRecordEvent).getAfter());
                                cloneInsert.setReferenceTime(originalTapRecordEvent.getReferenceTime());
                                event.setTapEvent(cloneInsert);
                            } else if (tapRecordEvent instanceof TapUpdateRecordEvent) {
                                TapUpdateRecordEvent cloneUpdate = new TapUpdateRecordEvent();
                                tapRecordEvent.clone(cloneUpdate);
                                cloneUpdate.setAfter(((TapUpdateRecordEvent) tapRecordEvent).getAfter());
                                cloneUpdate.setReferenceTime(originalTapRecordEvent.getReferenceTime());
                                event.setTapEvent(cloneUpdate);
                            }
                        } else {
                            if (tapRecordEvent instanceof TapInsertRecordEvent) {
                                logger.warn("no need to insert this record");
                                return Collections.emptyList();
                            } else if (tapRecordEvent instanceof TapUpdateRecordEvent) {
                                logger.warn("no need to keep this record");
                                TapDeleteRecordEvent cloneDelete = new TapDeleteRecordEvent();
                                tapRecordEvent.clone(cloneDelete);
                                cloneDelete.setBefore(((TapUpdateRecordEvent) tapRecordEvent).getBefore());
                                cloneDelete.setReferenceTime(originalTapRecordEvent.getReferenceTime());
                                event.setTapEvent(cloneDelete);
                            }
                        }
                    }
                    transformToTapValue(event, processorBaseContext.getTapTableMap(), processorBaseContext.getNode().getId(), tapValueTransform);
                    offer(event);
                } else {
                    throw new RuntimeException("not implement");
                }
                return Collections.emptyList();
            }

            private void schemaFilter(Map<String, Object> before, Map<String, Object> after) {
                if (before != null) {
                    Set<String> keySet = new HashSet<>(before.keySet());
                    for (String key : keySet) {
                        if (!targetFieldsName.contains(key)) {
                            before.remove(key);
                        }
                    }
                }
                if (after != null) {
                    Set<String> keySet = new HashSet<>(after.keySet());
                    for (String key : keySet) {
                        if (!targetFieldsName.contains(key)) {
                            after.remove(key);
                        }
                    }
                }
            }
        }

        private boolean checkCacheKeyIfExist(final String cacheKey, final String aggregatorOp) throws Exception {
            if ("COUNT".equalsIgnoreCase(aggregatorOp) || "SUM".equalsIgnoreCase(aggregatorOp) || "AVG".equalsIgnoreCase(aggregatorOp)) {
                return cacheNumbers.exists(cacheKey);
            } else {
                return cacheList.exists(cacheKey);
            }
        }

        private String concatGroupByKeys(Map<String, Object> data, List<String> groupbyList) {
            return concatGroupByKeysWithName(name, data, groupbyList);
        }

        private String concatGroupByKeysWithName(String name, Map<String, Object> data, List<String> groupbyList) {
            StringBuilder sb = new StringBuilder();
            sb.append(name).append("#");
            for (String single : groupbyList) {
                if (single != null) {
                    Object column = data.get(single);
                    if (column == null) {
                        throw new RuntimeException("field:" + single + " value is null.");
                    } else {
                        sb.append(column).append('#');
                    }
                } else {
                    sb.append('#');
                }
            }
            return sb.toString();
        }

        private BigDecimal getCachedValue(final String cacheKey, final String ops) throws Exception {
            if (!checkCacheKeyIfExist(cacheKey, ops)) {
                return null;
            }
            if ("MAX".equalsIgnoreCase(ops) || "MIN".equalsIgnoreCase(ops)) {
                return cacheList.find(cacheKey).get(0);
            } else {
                return cacheNumbers.find(cacheKey);
            }
        }
    }// end class Aggregator

}
