package io.tapdata.flow.engine.V2.node.hazelcast.processor.aggregation;

import cn.hutool.core.collection.ConcurrentHashSet;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.hazelcast.core.HazelcastInstance;
import com.tapdata.constant.CollectionUtil;
import com.tapdata.constant.ExecutorUtil;
import com.tapdata.constant.MapUtil;
import com.tapdata.entity.MessageEntity;
import com.tapdata.entity.OperationType;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.process.AggregationProcessorNode;
import com.tapdata.tm.commons.task.dto.Aggregation;
import io.tapdata.constructImpl.ConstructIMap;
import io.tapdata.constructImpl.DocumentIMap;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


/**
 * DAG上的聚合节点的实现类（内置多个聚合器Aggregator）
 *
 * @author alexouyang
 * @date 2022/4/19
 */
public class HazelcastMultiAggregatorProcessor extends HazelcastBaseNode {

    private final Logger logger = LogManager.getLogger(HazelcastMultiAggregatorProcessor.class);

    private final List<Aggregator> aggregators = new ArrayList<>();

    private final String nodeId;

    List<Aggregation> rules;

    private ConstructIMap<BigDecimal> cacheNumbers;

    private ConstructIMap<ArrayList<BigDecimal>> cacheList;

    private ConstructIMap<TapdataEvent> cacheEvents;

    private final List<String> targetFieldsName = new ArrayList<>();

    private int count = 0;

    private int count2 = 0;

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
//    pks = aggNode.getPrimaryKeys();

    }

    private void initCache(String nodeId, HazelcastInstance hazelcastInstance) {
        cacheNumbers = new DocumentIMap<>(hazelcastInstance,
                nodeId + "-" + "AggregatorCache");
        cacheList = new DocumentIMap<>(hazelcastInstance,
                nodeId + "-" + "AggregatorCacheList");
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
        for (int i = 0; i < rules.size(); i++) {
            Aggregation rule = rules.get(i);
            aggregators.add(new Aggregator(rule, processorBaseContext.getNode().getId(), i + 1));
        }
        if (aggregators.size() == 1) {
            Aggregator aggregator = aggregators.get(0);
            aggregator.wrapItem().deleteUnRelatedField().groupBy().rollingAggregate().finish();
            return;
        }
        for (int i = 0; i < aggregators.size(); i++) {
            Aggregator current = aggregators.get(i);
            if (i == aggregators.size() - 1) {
                current.deleteUnRelatedField().groupBy().rollingAggregate().finish();
            } else {
                Aggregator next = aggregators.get(i + 1);
                if (i == 0) {
                    current.wrapItem().deleteUnRelatedField().groupBy().rollingAggregate().next(next);
                } else {
                    current.deleteUnRelatedField().groupBy().rollingAggregate().next(next);
                }
            }
        }
    }

    @Override
    protected void init(@NotNull Context context) throws Exception {
        // 判空
        if (rules != null && !rules.isEmpty()) {
            initCache(nodeId, context.hazelcastInstance());

            initPipeline(rules);
            for (Aggregator aggregator : aggregators) {
                aggregator.init();
            }
        } else {
            logger.warn("Aggregation DAG config error.");
        }

        super.init(context);
    }

    @Override
    public void close() throws Exception {
        for (Aggregator aggregator : aggregators) {
            aggregator.close();
        }
        super.close();
    }

    @Override
    protected boolean tryProcess(int ordinal, @NotNull Object item) {
        Node<?> node = processorBaseContext.getNode();
        if (logger.isDebugEnabled()) {
            logger.debug(
                    "aggregator node [id: {}, name: {}] receive event {}",
                    node.getId(),
                    node.getName(),
                    item
            );
        }

//      // begin debug
//      TapdataEvent event = (TapdataEvent) item;
//        if( event.getMessageEntity() != null ){
//          MessageEntity messageEntity = ((TapdataEvent) item).getMessageEntity();
//        if(messageEntity.getAfter()!=null){
//          Object c_time = messageEntity.getAfter().get("c_time");
//          if(c_time != null){
//            try{
//              Instant instant = (Instant) c_time;
//              String timestamp = new Timestamp(instant.getEpochSecond()*1000L).toString();
//              if("2022-05-20 11:41:50.0".equals(timestamp)){
//                logger.debug("GROUP=(2022-05-20 11:41:50) in aggregator, COUNT=" + (++count));
//              }
//            }catch (Exception ignore){
//              logger.error("in aggregator:" + ignore);
//            }
//          }
//        }
//      }
//      // end debug
//      // begin debug
//      if( count2 % 100000 == 0 ){
//        logger.info("data sampling, count2=" + count2 + " queue size:" + aggregators.get(0).processors.get(0).inbox.size());
//      }
//      count2++;
//      // end debug
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
        transformFromTapValue(tapdataEvent, null);

        // initialize input
        Object initialInput = tapdataEvent;
        List<Object> inputItems = Lists.newArrayList(initialInput);
        for (Aggregator aggregator : aggregators) {
            final List<AggregatorProcessorBase> processors = aggregator.getProcessors();
            for (AggregatorProcessorBase processor : processors) {
                if (inputItems.size() == 0) {
                    if (!(processor instanceof Aggregator.FinishP)) {
                        logger.warn("empty input");
                    }
                    break;
                }
                if (inputItems.size() == 1) {
                    Object input = inputItems.get(0);
                    inputItems = processor.tryProcess(input);
                } else {
                    List<Object> result = null;
                    for (Object input : inputItems) {
                        result = processor.tryProcess(input);
                    }
                    inputItems = result;
                }
            }
        }
        return true;
    }

    public abstract static class AggregatorProcessorBase {

//        public final LinkedBlockingQueue<Object> inbox = new LinkedBlockingQueue<>(100);

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

        private List<Object> initialInput;

        private ExecutorService threadService;

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

//            final ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("Aggregator-processors-%d").build();
//            threadService = new ThreadPoolExecutor(processors.size(), processors.size(), 0L,
//                    TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(4), namedThreadFactory,
//                    new ThreadPoolExecutor.AbortPolicy());
            // 多线程启动聚合器内部的pipeline
//            running = true;
//            for (Runnable p : processors) {
//                threadService.execute(p);
//            }
        }

        public void close() {
            for (AggregatorProcessorBase processor : processors) {
                if (processor instanceof RollingAggregateP) {
                    ((RollingAggregateP) processor).closeBucketThreads();
                }
            }
            try {
                running = false;
                ExecutorUtil.shutdown(threadService, 10L, TimeUnit.SECONDS);
            } catch (Exception ignore) {
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

                Map<String, Object> before = TapEventUtil.getBefore(event);
                if (before != null) {
                    try {
                        Map<String, Object> copy = new HashMap<>(before.size());
                        MapUtil.deepCloneMap(before, copy);
                        for (String key : before.keySet()) {
                            if (!reserveFieldList.contains(key)) {
                                copy.remove(key);
                            }
                        }
                        TapEventUtil.setBefore(event, copy);
                    } catch (Exception e) {
                        logger.error("Deep copy map error ", e);
                    }

                }
                Map<String, Object> after = TapEventUtil.getAfter(event);
                if (after != null) {
                    try {
                        Map<String, Object> copy = new HashMap<>(after.size());
                        MapUtil.deepCloneMap(after, copy);
                        for (String key : after.keySet()) {
                            if (!reserveFieldList.contains(key)) {
                                copy.remove(key);
                            }
                        }
                        TapEventUtil.setAfter(event, copy);
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

            public GroupByP(List<String> groupbyList) {
                this.groupbyList = groupbyList;
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
                if (event instanceof MessageEntity) {
                    result = groupByPOld((MessageEntity) event, wrappedItem);
                } else if (event instanceof TapRecordEvent) {
                    result = groupByP((TapRecordEvent) event, wrappedItem);
                } else {
                    throw new RuntimeException("Unimplemented message type");
                }
                return result;
            }

            private String concatGroupByKeys(Map<String, Object> data, List<String> groupbyList) {
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

            private List<Object> groupByPOld(MessageEntity event, WrapItem wrappedItem) {
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
                    } else {
                        if (event.getAfter() != null) {
                            String cachedGroupByKey = concatGroupByKeys(event.getAfter(), groupbyList);
                            wrappedItem.setCachedGroupByKey(cachedGroupByKey);
                        } else if (event.getBefore() != null) {
                            String cachedGroupByKey = concatGroupByKeys(event.getBefore(), groupbyList);
                            wrappedItem.setCachedGroupByKey(cachedGroupByKey);
                        }
                        result.add(wrappedItem);
                    }
                    return result;
            }

            private List<Object> groupByP(TapRecordEvent event, WrapItem wrappedItem) {
                final List<Object> result = Lists.newArrayList();
                Map<String, Object> before = TapEventUtil.getBefore(event);
                Map<String, Object> after = TapEventUtil.getAfter(event);
                if (event instanceof TapUpdateRecordEvent) {
                    //split into two events
                    TapDeleteRecordEvent cloneDelete = new TapDeleteRecordEvent();
                    event.clone(cloneDelete);
                    cloneDelete.setBefore(InstanceFactory.instance(TapUtils.class).cloneMap(before));
                    WrapItem deleteWrap = wrappedItem.clone();
                    deleteWrap.setMessage(cloneDelete);
                    String cachedGroupByKey1 = concatGroupByKeys(before, groupbyList);
                    deleteWrap.setCachedGroupByKey(cachedGroupByKey1);
                    result.add(deleteWrap);

                    TapInsertRecordEvent cloneInsert = new TapInsertRecordEvent();
                    event.clone(cloneInsert);
                    cloneInsert.setAfter(InstanceFactory.instance(TapUtils.class).cloneMap(after));
                    WrapItem insertWrap = wrappedItem.clone();
                    insertWrap.setMessage(cloneInsert);
                    String cachedGroupByKey2 = concatGroupByKeys(after, groupbyList);
                    insertWrap.setCachedGroupByKey(cachedGroupByKey2);
                    result.add(insertWrap);
                } else {
                    if (after != null) {
                        String cachedGroupByKey = concatGroupByKeys(after, groupbyList);
                        wrappedItem.setCachedGroupByKey(cachedGroupByKey);
                    } else if (before != null) {
                        String cachedGroupByKey = concatGroupByKeys(before, groupbyList);
                        wrappedItem.setCachedGroupByKey(cachedGroupByKey);
                    }
                    result.add(wrappedItem);
                }
                return result;
            }

        } // end class GroupByP

        /**
         * 真正的聚合逻辑
         */
        public class RollingAggregateP extends AggregatorProcessorBase {

            private final String aggregatorField;

            private final String aggregatorOp;

            private AggregatorBucket opBucket = new AggregatorBucket();

            private boolean workerRunning = false;

            private ExecutorService opThreadService;

            private List<Future<Object>> opBucketFutures = Lists.newArrayList();

            private int count = 0;

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

//                initBucketThreads();
            }

            /**
             * 用分桶算法，加速计算
             */
            private void initBucketThreads() {
                logger.debug("initBucketWorkers...");
                final ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("RollingAggregateP-" + aggregatorOp + "-%d").build();
                opThreadService = new ThreadPoolExecutor(opBucket.bucketCount, opBucket.bucketCount, 0L,
                        TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(4), namedThreadFactory,
                        new ThreadPoolExecutor.AbortPolicy());
                // 多线程启动聚合器内部的pipeline
                workerRunning = true;
                for (int i = 0; i < opBucket.bucketCount; i++) {
                    BucketWorker worker = new BucketWorker(i);
                    final Future<Object> objectFuture = opThreadService.submit(worker);
                    opBucketFutures.add(objectFuture);
                }
            }

            public void closeBucketThreads() {
                try {
                    logger.debug("close BucketWorkers...");
                    workerRunning = false;
                    Thread.sleep(10000);
                    opThreadService.shutdownNow();
                } catch (Exception ignore) {
                }
            }

            @Override
            public List<Object> tryProcess(Object item) {
                if (item == null) {
                    return Collections.emptyList();
                }
                try {
                    final List<Object> result = Lists.newArrayList();
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
//                                final List<Object> sumResult = handleFutureList(opBucketFutures);
//                                result.addAll(sumResult);
                            case "AVG":
//                                if (count % 100000 == 0) {
//                                    logger.info("Rolling sample count=" + count + " input queue=" + items.size());
//                                }
//                                count++;
                                if (wrappedItem.isEvent()) {
                                    boolean rs = AggregateOps.avg(name, aggregatorField, cacheNumbers, wrappedItem, wrappedItem.isMessageEntity());
                                    return rs ? Lists.newArrayList(wrappedItem) : Collections.emptyList();
//                                    while (!opBucket.lockExist(cacheKey)) {
//                                        if (opBucket.setLock(cacheKey)) {
//                                            int bucketId = Math.abs(cacheKey.hashCode()) % opBucket.bucketCount;
//                                            List<WrapItem> values = opBucket.getData(bucketId).get(cacheKey);
//                                            if (values == null) {
//                                                values = new ArrayList<>(1000);
//                                                opBucket.getData(bucketId).put(cacheKey, values);
//                                            }
//                                            try {
//                                                values.add(wrappedItem);
//                                            } catch (Throwable e) {
//                                                logger.error(e);
//                                            }
//                                            opBucket.delLock(cacheKey);
//                                            break;
//                                        }
//                                    }
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

            private List<Object> handleFutureList(final List<Future<Object>> futures) {
                return opBucketFutures.stream().map(x -> {
                    try {
                        return x.get();
                    } catch (final Exception ignore) {
                        return null;
                    }
                }).collect(Collectors.toList());
            }


            /**
             * 分桶类
             */
            public class AggregatorBucket {
                private final Integer bucketCount = 8;

                // lock
                public Set<String> locks = new ConcurrentHashSet<>();

                private Map<Integer, Map<String, List<WrapItem>>> buckets;

                public AggregatorBucket() {
                    // init buckets
                    buckets = new ConcurrentHashMap<>(bucketCount);
                    for (int i = 0; i < bucketCount; i++) {
                        buckets.put(i, new ConcurrentHashMap<>());
                    }
                }

                public boolean lockExist(String cacheKey) {
                    return locks.contains(cacheKey);
                }

                public boolean setLock(String cacheKey) {
                    synchronized (locks) {
                        if (!lockExist(cacheKey)) {
                            return locks.add(cacheKey);
                        } else {
                            return false;
                        }
                    }
                }

                public boolean delLock(String cacheKey) {
                    synchronized (locks) {
                        if (lockExist(cacheKey)) {
                            return locks.remove(cacheKey);
                        } else {
                            return false;
                        }
                    }
                }

                public Map<String, List<WrapItem>> getData(int bucketId) {
                    return buckets.get(bucketId);
                }
            }

            /**
             * 分桶并行计算类
             * 加速单cpu计算很慢的算子，比如SUM和AVG
             */
            public class BucketWorker implements Callable {

                private Integer workerId;

                public BucketWorker(Integer id) {
                    workerId = id;
                }

                @Override
                public Object call() {
                    while (workerRunning) {
                        Map.Entry<String, List<WrapItem>> kv = tryLock();
                        if (kv != null && kv.getValue() != null) {
                            while (!kv.getValue().isEmpty()) {
                                try {
                                    LinkedBlockingQueue<WrapItem> data = new LinkedBlockingQueue<>(kv.getValue());
                                    kv.getValue().clear();
                                    // 先把锁释放了，提升并行效率
                                    releaseLock(kv.getKey());
                                    WrapItem wrapItem = null;
                                    Integer batch = data.size();
                                    while (!data.isEmpty()) {
                                        wrapItem = data.poll(pollTimeout, TimeUnit.MILLISECONDS);
                                        logger.debug("doSum, cacheKey= " + kv.getKey());
                                        doWork(wrapItem);
                                    }
                                    // 丢弃中间结果，直接返回最后一个结果，提速
                                    return wrapItem;
                                } catch (Exception e) {
                                    logger.error("Bucket worker execute failed, error: " + e.getMessage(), e);
                                } finally {
                                    releaseLock(kv.getKey());
                                }
                            }
                            releaseLock(kv.getKey());
                        } else {
                            logger.debug("tryLock failed!");
                            return null;
                        }
                    }
                    return null;
                }

                private boolean doWork(WrapItem wrapItem) throws Exception {
                    if (wrapItem.isEvent()) {
                        if ("SUM".equalsIgnoreCase(aggregatorOp)) {
                            AggregateOps.sum(name, aggregatorField, cacheNumbers, wrapItem, wrapItem.isMessageEntity());
                        } else if ("AVG".equalsIgnoreCase(aggregatorOp)) {
                            AggregateOps.avg(name, aggregatorField, cacheNumbers, wrapItem, wrapItem.isMessageEntity());
                        } else {
                            logger.info("Unimplemented op type");
                        }
                    } else {
                        throw new RuntimeException("Unimplemented message type");
                    }
                    return false;
                }

                /***
                 * 尝试加锁
                 * @return 返回被锁的对象，可以为空
                 */
                private Map.Entry<String, List<WrapItem>> tryLock() {
                    for (Map.Entry<String, List<WrapItem>> data : opBucket.getData(workerId).entrySet()) {
                        if (opBucket.lockExist(data.getKey())) {
                            logger.debug("key " + data.getKey() + " has locked, looking other keys");
                        } else if (data.getValue() == null || data.getValue().isEmpty()) {
                            logger.debug("key " + data.getKey() + " has no data, looking other keys");
                        } else {
                            opBucket.setLock(data.getKey());
                            return data; // break
                        }
                    }
                    // 遍历完停1秒
                    try {
                        Thread.sleep(1000);
                    } catch (Exception ignore) {
                    }
                    return null;
                }

                private void releaseLock(String lock) {
                    if (opBucket.lockExist(lock)) {
                        opBucket.delLock(lock);
                    }
                }
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
                        schemaFilterOld(messageEntity);
                        // delete 事件特殊处理,否则delete事件把目标结果直接删了
                        if (wrappedItem.getCachedRollingAggregateCounter().compareTo(BigDecimal.ZERO) != 0) {
                            messageEntity.setOp("i");
                        }

                        event.setMessageEntity(messageEntity);
                    } else if (((WrapItem) item).getMessage() instanceof TapRecordEvent) {
                        TapRecordEvent tapRecordEvent = (TapRecordEvent) wrappedItem.getMessage();
                        schemaFilter(tapRecordEvent);
                        if (wrappedItem.getCachedRollingAggregateCounter().compareTo(BigDecimal.ZERO) != 0) {
                            TapInsertRecordEvent cloneInsert = new TapInsertRecordEvent();
                            tapRecordEvent.clone(cloneInsert);
                            if (tapRecordEvent instanceof TapDeleteRecordEvent) {
                                cloneInsert.setAfter(((TapDeleteRecordEvent) tapRecordEvent).getBefore());
                            } else if (tapRecordEvent instanceof TapInsertRecordEvent) {
                                cloneInsert.setAfter(((TapInsertRecordEvent) tapRecordEvent).getAfter());
                            } else if (tapRecordEvent instanceof TapUpdateRecordEvent) {
                                cloneInsert.setAfter(((TapUpdateRecordEvent) tapRecordEvent).getAfter());
                            }
                            event.setTapEvent(cloneInsert);
                        } else {
                            event.setTapEvent(tapRecordEvent);
                        }
                    }
                    // debug
                    if (event.getMessageEntity() != null) {
                        MessageEntity messageEntity = event.getMessageEntity();
                        if (messageEntity.getAfter() != null) {
                            Object c_time = messageEntity.getAfter().get("c_time");
                            Object c_time_grouped_count = messageEntity.getAfter().get("COUNT");
                            if (c_time != null) {
                                try {

                                    Instant instant = (Instant) c_time;
                                    String timestamp = new Timestamp(instant.getEpochSecond() * 1000L).toString();
                                    if ("2022-05-20 11:41:50.0".equals(timestamp)) {
                                        logger.debug("GROUP=(2022-05-20 11:41:50) emit out, COUNT=" + c_time_grouped_count);
                                    }
                                } catch (Exception ignore) {
                                    logger.error("emit out:" + ignore);
                                }
                            }
                        }
                    }
                    // end debug
                    transformToTapValue(event, processorBaseContext.getTapTableMap(), processorBaseContext.getNode().getId());
//                    while (isRunning()) {
//                        if (offer(event)) break;
//                    }
                    offer(event);
                    logger.warn("--offer event--");
                } else {
                    throw new RuntimeException("not implement");
                }
                return Collections.emptyList();
            }

            private void schemaFilterOld(MessageEntity messageEntity) {
                if (messageEntity.getBefore() != null) {
                    Set<String> keySet = new HashSet<>(messageEntity.getBefore().keySet());
                    for (String key : keySet) {
                        if (!targetFieldsName.contains(key)) {
                            messageEntity.getBefore().remove(key);
                        }
                    }
                }

                if (messageEntity.getAfter() != null) {
                    Set<String> keySet = new HashSet<>(messageEntity.getAfter().keySet());
                    for (String key : keySet) {
                        if (!targetFieldsName.contains(key)) {
                            messageEntity.getAfter().remove(key);
                        }
                    }
                }
            }

            private void schemaFilter(TapRecordEvent event) {
                Map<String, Object> before = TapEventUtil.getBefore(event);
                if (before != null) {
                    Set<String> keySet = new HashSet<>(before.keySet());
                    for (String key : keySet) {
                        if (!targetFieldsName.contains(key)) {
                            before.remove(key);
                        }
                    }
                }
                Map<String, Object> after = TapEventUtil.getAfter(event);
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

    }// end class Aggregator

}
