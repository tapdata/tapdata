package io.tapdata.flow.engine.V2.node.hazelcast.processor.aggregation;

import com.tapdata.entity.MessageEntity;
import com.tapdata.entity.OperationType;
import io.tapdata.construct.constructImpl.ConstructIMap;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * 大家一致同意先不要搞太复杂
 * 因此每个聚合节点只支持1个同类算子，比如SUM只能有1个，这样可以简化代码
 * 算子为什么没有处理update，因为前置处理器：CrossGroupP和groupByP 把update拆成了delete+insert
 *
 * 从source传过来的update事件，都通过前置处理器CrossGroupP和groupByP 把update拆成了delete+insert
 * 从source传过来的insert事件，除了第一条记录，后续都在前置处理器进行转化成update，因此走到算子里的update实际上都为insert
 *
 * @author alexouyang
 * @Date 2022/4/26
 */
public class AggregateOps {

    private final static Logger logger = LogManager.getLogger(AggregateOps.class);

    private final static String SUM = "SUM";

    private final static String AVG = "AVG";

    private final static String COUNT = "COUNT";

    private final static String MAX = "MAX";

    private final static String MIN = "MIN";

    private final static int scale = 4;

    private final static int roundingMode = BigDecimal.ROUND_HALF_UP;

    private final static int MAX_LENGTH = 1000;

    public static boolean count(String aggregatorName, ConstructIMap<BigDecimal> cache,
                                WrapItem wrappedItem, boolean isMessageEntity) throws Exception {
        if (isMessageEntity) {
            return countMessageEntity(aggregatorName, cache, wrappedItem);
        } else {
            return countTapRecordEvent(aggregatorName, cache, wrappedItem);
        }
    }

    public static boolean sum(String aggregatorName, String aggregatorField, ConstructIMap<BigDecimal> cache,
                              WrapItem wrappedItem, boolean isMessageEntity) throws Exception {
        if (isMessageEntity) {
            return sumMessageEntity(aggregatorName, aggregatorField, cache, wrappedItem);
        } else {
            return sumTapRecordEvent(aggregatorName, aggregatorField, cache, wrappedItem);
        }
    }

    public static boolean avg(String aggregatorName, String aggregatorField, ConstructIMap<BigDecimal> cache,
                              WrapItem wrappedItem, boolean isMessageEntity) throws Exception {
        if (isMessageEntity) {
            return avgMessageEntity(aggregatorName, aggregatorField, cache, wrappedItem);
        } else {
            return avgTapRecordEvent(aggregatorName, aggregatorField, cache, wrappedItem);
        }
    }

    public static void avg(TapRecordEvent tapRecordEvent, ConstructIMap<BigDecimal> cache, WrapItem wrappedItem) {

    }

    public static boolean max(String aggregatorField, ConstructIMap<BigDecimal> cache, ConstructIMap<List<BigDecimal>> cacheList,
                              WrapItem wrappedItem, boolean isMessageEntity) throws Exception {
        if (isMessageEntity) {
            return maxMessageEntity(aggregatorField, cache, cacheList, wrappedItem);
        } else {
            return maxTapRecordEvent(aggregatorField, cache, cacheList, wrappedItem);
        }
    }

    public static boolean min(String aggregatorField, ConstructIMap<BigDecimal> cache, ConstructIMap<List<BigDecimal>> cacheList,
                              WrapItem wrappedItem, boolean isMessageEntity) throws Exception {
        if (isMessageEntity) {
            return minMessageEntity(aggregatorField, cache, cacheList, wrappedItem);
        } else {
            return minTapRecordEvent(aggregatorField, cache, cacheList, wrappedItem);
        }
    }

    public static void min(TapRecordEvent tapRecordEvent, ConstructIMap<BigDecimal> cache, WrapItem wrappedItem) {

    }

    private static BigDecimal updateCounter(ConstructIMap<BigDecimal> cache, String cacheKey, MessageEntity event, BigDecimal changedCount) throws Exception {
        String counterCacheKey = cacheKey + "#counter";
        final OperationType operationType = OperationType.fromOp(event.getOp());
        if (cache.exists(counterCacheKey)) {
            BigDecimal counter = cache.find(counterCacheKey);
            if (operationType == OperationType.DELETE) {
                BigDecimal newValue = counter.subtract(BigDecimal.ONE);
                cache.update(counterCacheKey, newValue);
                return newValue;
            } else if (operationType == OperationType.INSERT) {
                BigDecimal newValue = counter.add(BigDecimal.ONE);
                cache.update(counterCacheKey, newValue);
                return newValue;
            } else {
                return counter;
            }
        } else {
            if (operationType == OperationType.DELETE) {
                logger.error("counter not exist." + event);
                return BigDecimal.ZERO;
            } else if (operationType == OperationType.INSERT) {
                cache.insert(counterCacheKey, BigDecimal.ONE);
                return BigDecimal.ONE;
            } else {
                return BigDecimal.ZERO;
            }
        }
    }

    private static BigDecimal updateCounter(ConstructIMap<BigDecimal> cache, String cacheKey, TapRecordEvent event, BigDecimal changedCount) throws Exception {
        String counterCacheKey = cacheKey + "#counter";
        OperationType operationType = OperationType.fromOp(TapEventUtil.getOp(event));
        if (cache.exists(counterCacheKey)) {
            BigDecimal counter = cache.find(counterCacheKey);
            BigDecimal addition = preProcessCount(operationType, changedCount);
            BigDecimal newValue = counter.add(addition);
            cache.update(counterCacheKey, newValue);
            return newValue;
//            if (event instanceof TapDeleteRecordEvent) {
//                BigDecimal newValue = counter.subtract(BigDecimal.ONE);
//                cache.update(counterCacheKey, newValue);
//                return newValue;
//            } else if (event instanceof TapInsertRecordEvent || event instanceof TapUpdateRecordEvent) {
//                BigDecimal newValue = counter.add(BigDecimal.ONE);
//                cache.update(counterCacheKey, newValue);
//                return newValue;
//            } else {
//                return counter;
//            }

        } else {
            if (event instanceof TapDeleteRecordEvent) {
                logger.error("counter not exist." + event);
                return BigDecimal.ZERO;
            } else if (event instanceof TapInsertRecordEvent) {
                cache.insert(counterCacheKey, BigDecimal.ONE);
                return BigDecimal.ONE;
            } else {
                return BigDecimal.ZERO;
            }
        }
    }

    private static BigDecimal preProcessSum(MessageEntity event, String aggregatorField, BigDecimal changedCount) {
        final OperationType operationType = OperationType.fromOp(event.getOp());
        if (changedCount.compareTo(BigDecimal.ZERO) != 0) {
            boolean positive = changedCount.compareTo(BigDecimal.ZERO) > 0;
            Map<String, Object> before = event.getBefore();
            Map<String, Object> after = event.getAfter();
            return positive ? getNumberForPreProcessSum(after, aggregatorField) : getNumberForPreProcessSum(before, aggregatorField).negate();
        }
        if (operationType == OperationType.DELETE) {
            return getNumberForPreProcessSum(event.getBefore(), aggregatorField).negate();
        } else if (operationType == OperationType.INSERT) {
            return getNumberForPreProcessSum(event.getAfter(), aggregatorField);
        } else {
            throw new RuntimeException("unimplemented code");
        }
    }

    private static BigDecimal preProcessSum(TapRecordEvent event, String aggregatorField, BigDecimal changedCount) {
        if (changedCount == null) {
            return BigDecimal.ZERO;
        }
        if (changedCount.compareTo(BigDecimal.ZERO) != 0) {
            boolean positive = changedCount.compareTo(BigDecimal.ZERO) > 0;
            Map<String, Object> before = TapEventUtil.getBefore(event);
            Map<String, Object> after = TapEventUtil.getAfter(event);
            return positive ? getNumberForPreProcessSum(after, aggregatorField) : getNumberForPreProcessSum(before, aggregatorField).negate();
        }
        if (event instanceof TapDeleteRecordEvent) {
            Map<String, Object> before = TapEventUtil.getBefore(event);
            return getNumberForPreProcessSum(before, aggregatorField).negate();
        } else if (event instanceof TapInsertRecordEvent) {
            Map<String, Object> after = TapEventUtil.getAfter(event);
            return getNumberForPreProcessSum(after, aggregatorField);
        } else {
            throw new RuntimeException("unimplemented code");
        }
    }

    private static BigDecimal getNumberForPreProcessSum(Map<String, Object> map, final String aggregatorField) {
        if (map == null) {
            return BigDecimal.ZERO;
        }
        Object fieldValue = map.get(aggregatorField);
        try {
            return AggregatorUtils.getBigDecimal(fieldValue);
        } catch (Exception ignore) {

        }
        return BigDecimal.ZERO;
    }

    private static BigDecimal preProcessField(MessageEntity event, String aggregatorField) {
        final OperationType operationType = OperationType.fromOp(event.getOp());
        if (operationType == OperationType.DELETE) {
            if (event.getBefore() != null) {
                Object fieldValue = event.getBefore().get(aggregatorField);
                try {
                    return AggregatorUtils.getBigDecimal(fieldValue);
                } catch (Exception ignore) {

                }
            }
        } else if (operationType == OperationType.INSERT || operationType == OperationType.UPDATE) {
            if (event.getAfter() != null) {
                Object fieldValue = event.getAfter().get(aggregatorField);
                try {
                    return AggregatorUtils.getBigDecimal(fieldValue);
                } catch (Exception ignore) {

                }
            }
        } else {
            return null;
        }
        return null;
    }

    private static BigDecimal preProcessField(TapRecordEvent event, String aggregatorField) {
        if (event instanceof TapDeleteRecordEvent) {
            Map<String, Object> before = TapEventUtil.getBefore(event);
            if (before != null) {
                Object fieldValue = before.get(aggregatorField);
                try {
                    return AggregatorUtils.getBigDecimal(fieldValue);
                } catch (Exception ignore) {

                }
            }
        } else if (event instanceof TapInsertRecordEvent || event instanceof TapUpdateRecordEvent) {
            Map<String, Object> after = TapEventUtil.getAfter(event);
            if (after != null) {
                Object fieldValue = after.get(aggregatorField);
                try {
                    return AggregatorUtils.getBigDecimal(fieldValue);
                } catch (Exception ignore) {

                }
            }
        } else {
            return null;
        }
        return null;
    }

    private static void postProcessSum(MessageEntity messageEntity, BigDecimal sum) {
        if (messageEntity != null) {
            if (messageEntity.getBefore() != null) {
                messageEntity.getBefore().put(SUM, sum);
            }
            if (messageEntity.getAfter() != null) {
                messageEntity.getAfter().put(SUM, sum);
            }
        }
    }

    private static void postProcessSum(TapRecordEvent tapRecordEvent, BigDecimal sum) {
        final String sumStr = sum.toPlainString();
        if (tapRecordEvent != null) {
            Map<String, Object> before = TapEventUtil.getBefore(tapRecordEvent);
            if (before != null) {
                before.put(SUM, sumStr);
            }
            Map<String, Object> after = TapEventUtil.getAfter(tapRecordEvent);
            if (after != null) {
                after.put(SUM, sumStr);
            }
        }
    }

    private static BigDecimal preProcessCount(OperationType operationType, BigDecimal changedCount) {
        if (changedCount == null) {
            return BigDecimal.ZERO;
        }
        if (changedCount.compareTo(BigDecimal.ZERO) != 0) {
            return changedCount;
        }

        if (operationType == OperationType.INSERT) {
            return BigDecimal.ONE;
        } else if (operationType == OperationType.DELETE) {
            return BigDecimal.ONE.negate();
        } else {
            return BigDecimal.ZERO;
        }
    }

    private static void postProcessCount(MessageEntity messageEntity, BigDecimal count) {
        if (messageEntity != null) {
            if (messageEntity.getBefore() != null) {
                messageEntity.getBefore().put(COUNT, count);
            }
            if (messageEntity.getAfter() != null) {
                messageEntity.getAfter().put(COUNT, count);
            }
        }
    }

    private static void postProcessCount(TapRecordEvent tapRecordEvent, BigDecimal count) {
        if (null == tapRecordEvent) return;
        Map<String, Object> before = TapEventUtil.getBefore(tapRecordEvent);
        if (null != before) {
            before.put(COUNT, count);
        }
        Map<String, Object> after = TapEventUtil.getAfter(tapRecordEvent);
        if (null != after) {
            after.put(COUNT, count);
        }
    }

    private static void postProcessAvg(MessageEntity messageEntity, BigDecimal sum, BigDecimal counter) {
        if (messageEntity != null) {
            if (messageEntity.getBefore() != null) {
                messageEntity.getBefore().put(AVG, sum.divide(counter, scale, roundingMode));
            }
            if (messageEntity.getAfter() != null) {
                messageEntity.getAfter().put(AVG, sum.divide(counter, scale, roundingMode));
            }
        }
    }

    private static void postProcessAvg(TapRecordEvent tapRecordEvent, BigDecimal sum, BigDecimal counter) {
        final String avgStr = sum.divide(counter, scale, roundingMode).toPlainString();
        if (tapRecordEvent != null) {
            Map<String, Object> before = TapEventUtil.getBefore(tapRecordEvent);
            if (before != null) {
                before.put(AVG, avgStr);
            }
            Map<String, Object> after = TapEventUtil.getAfter(tapRecordEvent);
            if (after != null) {
                after.put(AVG, avgStr);
            }
        }
    }

    private static boolean countMessageEntity(String aggregatorName, ConstructIMap<BigDecimal> cache,
                                              WrapItem wrappedItem) throws Exception {
        String cacheKey = wrappedItem.getCachedGroupByKey();
        MessageEntity messageEntity = (MessageEntity) wrappedItem.getMessage();
        OperationType operationType = OperationType.fromOp(messageEntity.getOp());

        BigDecimal countValue = preProcessCount(operationType, wrappedItem.getChangedCount());

        if (cache.exists(cacheKey)) {
            BigDecimal valueInCache = cache.find(cacheKey);
            BigDecimal count = valueInCache.add(countValue);
            /**
             * 把count==0的记录和缓存清空
             */
            if (count.compareTo(BigDecimal.ZERO) == 0) {
                cache.delete(cacheKey);
                postProcessCount(messageEntity, count);
            } else {
                cache.update(cacheKey, count);
                postProcessCount(messageEntity, count);
            }
            wrappedItem.setCachedRollingAggregateCounter(count);
            wrappedItem.setCachedGroupByKey(null);

        } else {
            cache.insert(cacheKey, countValue);
            postProcessCount(messageEntity, countValue);
            wrappedItem.setCachedRollingAggregateCounter(countValue);
            wrappedItem.setCachedGroupByKey(null);
        }
        return true;
    }

    private static boolean countTapRecordEvent(String aggregatorName, ConstructIMap<BigDecimal> cache,
                                               WrapItem wrappedItem) throws Exception {
        String cacheKey = wrappedItem.getCachedGroupByKey();
        TapRecordEvent tapRecordEvent = (TapRecordEvent) wrappedItem.getMessage();
        OperationType operationType = OperationType.fromOp(TapEventUtil.getOp(tapRecordEvent));

        BigDecimal countValue = preProcessCount(operationType, wrappedItem.getChangedCount());

        if (cache.exists(cacheKey)) {
            BigDecimal valueInCache = cache.find(cacheKey);
            BigDecimal count = valueInCache.add(countValue);
            /**
             * 把count==0的记录和缓存清空
             */
            if (count.compareTo(BigDecimal.ZERO) == 0) {
                cache.delete(cacheKey);
                postProcessCount(tapRecordEvent, count);
            } else {
                cache.update(cacheKey, count);
                postProcessCount(tapRecordEvent, count);
            }
            wrappedItem.setCachedRollingAggregateCounter(count);
            wrappedItem.setCachedGroupByKey(null);

        } else {
            cache.insert(cacheKey, countValue);
            postProcessCount(tapRecordEvent, countValue);
            wrappedItem.setCachedRollingAggregateCounter(countValue);
            wrappedItem.setCachedGroupByKey(null);
        }
        return true;
    }


    private static boolean sumMessageEntity(String aggregatorName, String aggregatorField, ConstructIMap<BigDecimal> cache,
                                            WrapItem wrappedItem) throws Exception {
        String cacheKey = wrappedItem.getCachedGroupByKey();
        MessageEntity messageEntity = (MessageEntity) wrappedItem.getMessage();
        BigDecimal fieldValue = preProcessSum(messageEntity, aggregatorField, wrappedItem.getChangedCount());
        if (fieldValue == null) {
            logger.error("value is null for aggregatorField name: " + aggregatorField);
            return false;
        }
        // 累计的行数
        BigDecimal groupedRecordCount = updateCounter(cache, cacheKey, messageEntity, wrappedItem.getChangedCount());

        if (cache.exists(cacheKey)) {
            // 删除
            if (groupedRecordCount.compareTo(BigDecimal.ZERO) == 0) {
                logger.info("cacheKey=" + cacheKey + " counter=" + groupedRecordCount);
                cache.delete(cacheKey);
                cache.delete(cacheKey + "#counter");
            } else {
                BigDecimal valueInCache = cache.find(cacheKey);
                BigDecimal sum = valueInCache.add(fieldValue);
                // 更新
                cache.update(cacheKey, sum);
                postProcessSum(messageEntity, sum);
            }
        } else {
            // 插入
            cache.insert(cacheKey, fieldValue);
            postProcessSum(messageEntity, fieldValue);
        }
        wrappedItem.setCachedRollingAggregateCounter(groupedRecordCount);
        wrappedItem.setCachedGroupByKey(null);
        return true;
    }

    private static boolean sumTapRecordEvent(String aggregatorName, String aggregatorField, ConstructIMap<BigDecimal> cache,
                                             WrapItem wrappedItem) throws Exception {
        String cacheKey = wrappedItem.getCachedGroupByKey();
        TapRecordEvent tapRecordEvent = (TapRecordEvent) wrappedItem.getMessage();
        BigDecimal fieldValue = preProcessSum(tapRecordEvent, aggregatorField, wrappedItem.getChangedCount());
        if (fieldValue == null) {
            logger.error("value is null for aggregatorField name: " + aggregatorField);
            return false;
        }
        // 累计的行数
        BigDecimal groupedRecordCount = updateCounter(cache, cacheKey, tapRecordEvent, wrappedItem.getChangedCount());

        if (cache.exists(cacheKey)) {
            // 删除
            if (groupedRecordCount.compareTo(BigDecimal.ZERO) == 0) {
                logger.info("cacheKey=" + cacheKey + " counter=" + groupedRecordCount);
                cache.delete(cacheKey);
                cache.delete(cacheKey + "#counter");
            } else {
                BigDecimal valueInCache = cache.find(cacheKey);
                BigDecimal sum = valueInCache.add(fieldValue);
                // 更新
                cache.update(cacheKey, sum);
                postProcessSum(tapRecordEvent, sum);
            }
        } else {
            // 插入
            cache.insert(cacheKey, fieldValue);
            postProcessSum(tapRecordEvent, fieldValue);
        }
        wrappedItem.setCachedRollingAggregateCounter(groupedRecordCount);
        wrappedItem.setCachedGroupByKey(null);
        return true;
    }

    private static boolean avgMessageEntity(String aggregatorName, String aggregatorField, ConstructIMap<BigDecimal> cache,
                                            WrapItem wrappedItem) throws Exception {
        String cacheKey = wrappedItem.getCachedGroupByKey();
        MessageEntity messageEntity = (MessageEntity) wrappedItem.getMessage();
        // 这一步跟SUM是一样的
        BigDecimal fieldValue = preProcessSum(messageEntity, aggregatorField, wrappedItem.getChangedCount());
        if (fieldValue == null) {
            logger.error("value is null for aggregatorField name: " + aggregatorField);
            return false;
        }
        // 累计的行数
        BigDecimal groupedRecordCount = updateCounter(cache, cacheKey, messageEntity, wrappedItem.getChangedCount());

        if (cache.exists(cacheKey)) {
            // 删除
            if (groupedRecordCount.compareTo(BigDecimal.ZERO) == 0) {
                logger.info("cacheKey=" + cacheKey + " counter=" + groupedRecordCount);
                cache.delete(cacheKey);
                cache.delete(cacheKey + "#counter");
            } else {
                BigDecimal valueInCache = cache.find(cacheKey);
                BigDecimal sum = valueInCache.add(fieldValue);
                // 更新
                cache.update(cacheKey, sum);
                postProcessAvg(messageEntity, sum, groupedRecordCount);
            }
        } else {
            // 插入
            cache.insert(cacheKey, fieldValue);
            postProcessAvg(messageEntity, fieldValue, groupedRecordCount);
        }
        wrappedItem.setCachedRollingAggregateCounter(groupedRecordCount);
        wrappedItem.setCachedGroupByKey(null);
        return true;
    }

    private static boolean avgTapRecordEvent(String aggregatorName, String aggregatorField, ConstructIMap<BigDecimal> cache,
                                             WrapItem wrappedItem) throws Exception {
        String cacheKey = wrappedItem.getCachedGroupByKey();
        TapRecordEvent tapRecordEvent = (TapRecordEvent) wrappedItem.getMessage();
        // 这一步跟SUM是一样的
        BigDecimal fieldValue = preProcessSum(tapRecordEvent, aggregatorField, wrappedItem.getChangedCount());
        if (fieldValue == null) {
            logger.error("value is null for aggregatorField name: " + aggregatorField);
            return false;
        }
        // 累计的行数
        BigDecimal groupedRecordCount = updateCounter(cache, cacheKey, tapRecordEvent, wrappedItem.getChangedCount());
        if (cache.exists(cacheKey)) {
            // 删除
            if (groupedRecordCount.compareTo(BigDecimal.ZERO) == 0) {
                logger.info("cacheKey=" + cacheKey + " counter=" + groupedRecordCount);
                cache.delete(cacheKey);
                cache.delete(cacheKey + "#counter");
            } else {
                BigDecimal valueInCache = cache.find(cacheKey);
                BigDecimal sum = valueInCache.add(fieldValue);
                // 更新
                cache.update(cacheKey, sum);
                postProcessAvg(tapRecordEvent, sum, groupedRecordCount);
            }
        } else {
            // 插入
            cache.insert(cacheKey, fieldValue);
            postProcessAvg(tapRecordEvent, fieldValue, groupedRecordCount);
        }
        wrappedItem.setCachedRollingAggregateCounter(groupedRecordCount);
        wrappedItem.setCachedGroupByKey(null);
        return true;
    }

    private static boolean maxMessageEntity(String aggregatorField, ConstructIMap<BigDecimal> cache, ConstructIMap<List<BigDecimal>> cacheList,
                                            WrapItem wrappedItem) throws Exception {
        String cacheKey = wrappedItem.getCachedGroupByKey();
        MessageEntity messageEntity = (MessageEntity) wrappedItem.getMessage();
        final OperationType operationType = OperationType.fromOp(messageEntity.getOp());

        BigDecimal fieldValue = preProcessField(messageEntity, aggregatorField);
        BigDecimal changedCount = wrappedItem.getChangedCount();
        // 累计的行数
        BigDecimal groupedRecordCount = updateCounter(cache, cacheKey, messageEntity, changedCount);

        if (cacheList.exists(cacheKey)) {
            List<BigDecimal> groupedMaxList = cacheList.find(cacheKey);
            if (groupedMaxList.contains(fieldValue)) {
                if (operationType == OperationType.DELETE) {
                    groupedMaxList.remove(fieldValue);
                } else if (operationType == OperationType.INSERT || operationType == OperationType.UPDATE) {
                    logger.debug("cacheList already contains this value:" + fieldValue + " do not need to insert");
                } else {
                    throw new RuntimeException("unimplement code");
                }
            } else {
                if (operationType == OperationType.DELETE) {
                    logger.debug("cacheList didn't contains this value:" + fieldValue + " can not delete");
                } else if (operationType == OperationType.INSERT) {
                    groupedMaxList = doInsertAndCut(groupedMaxList, fieldValue, new MaxComparator(), changedCount);
                } else {
                    if (changedCount == null || changedCount.compareTo(BigDecimal.ZERO) <= 0) {
                        throw new RuntimeException("unimplement code");
                    } else {
                        groupedMaxList = doInsertAndCut(groupedMaxList, fieldValue, new MaxComparator(), changedCount);
                    }
                }
            }
            if (groupedMaxList.size() > 0) {
                cacheList.update(cacheKey, groupedMaxList);
            } else {
                cacheList.delete(cacheKey);
            }
            postProcessMax(messageEntity, groupedMaxList);
        } else {
            if (operationType == OperationType.INSERT) {
                ArrayList<BigDecimal> groupedMaxList = new ArrayList<>(1);
                groupedMaxList.add(fieldValue);
                cacheList.insert(cacheKey, groupedMaxList);
                postProcessMax(messageEntity, groupedMaxList);
            } else {
                throw new RuntimeException("unexpect logic");
            }
        }

        wrappedItem.setCachedRollingAggregateCounter(groupedRecordCount);
        wrappedItem.setCachedGroupByKey(null);
        return true;

    }

    private static boolean maxTapRecordEvent(String aggregatorField, ConstructIMap<BigDecimal> cache, ConstructIMap<List<BigDecimal>> cacheList, WrapItem wrappedItem) throws Exception {
        String cacheKey = wrappedItem.getCachedGroupByKey();
        TapRecordEvent tapRecordEvent = (TapRecordEvent) wrappedItem.getMessage();

        BigDecimal fieldValue = preProcessField(tapRecordEvent, aggregatorField);

        // 累计的行数
        BigDecimal groupedRecordCount = updateCounter(cache, cacheKey, tapRecordEvent, wrappedItem.getChangedCount());
        BigDecimal changedCount = wrappedItem.getChangedCount();
        if (cacheList.exists(cacheKey)) {
            List<BigDecimal> groupedMaxList = cacheList.find(cacheKey);
            groupedMaxList.sort(new MaxComparator());
            if (groupedMaxList.contains(fieldValue)) {
                if (tapRecordEvent instanceof TapDeleteRecordEvent) {
                    groupedMaxList.remove(fieldValue);
                } else if (tapRecordEvent instanceof TapInsertRecordEvent) {
                    logger.debug("cacheList already contains this value:" + fieldValue + " do not need to insert");
                } else {
                    if (changedCount.compareTo(BigDecimal.ZERO) < 0) {
                        groupedMaxList.remove(fieldValue);
                    } else if (changedCount.compareTo(BigDecimal.ZERO) > 0) {
                        logger.debug("cacheList already contains this value:" + fieldValue + " do not need to insert");
                    } else {
                        throw new RuntimeException("unimplement code");
                    }
                }
            } else {
                if (tapRecordEvent instanceof TapDeleteRecordEvent) {
                    logger.debug("cacheList didn't contains this value:" + fieldValue + " can not delete");
                } else if (tapRecordEvent instanceof TapInsertRecordEvent) {
                    groupedMaxList = doInsertAndCut(groupedMaxList, fieldValue, new MaxComparator(), changedCount);
                } else {
                    if (changedCount == null || changedCount.compareTo(BigDecimal.ZERO) <= 0) {
                        throw new RuntimeException("unimplement code");
                    } else {
                        groupedMaxList = doInsertAndCut(groupedMaxList, fieldValue, new MaxComparator(), changedCount);
                    }
                }
            }
            if (groupedMaxList.size() > 0) {
                cacheList.update(cacheKey, groupedMaxList);
            } else {
                cacheList.delete(cacheKey);
            }
            postProcessMax(tapRecordEvent, groupedMaxList);
        } else {
            if (tapRecordEvent instanceof TapInsertRecordEvent) {
                ArrayList<BigDecimal> groupedMaxList = new ArrayList<>(1);
                groupedMaxList.add(fieldValue);
                cacheList.insert(cacheKey, groupedMaxList);
                postProcessMax(tapRecordEvent, groupedMaxList);
            } else {
                throw new RuntimeException("unexpect logic");
            }
        }

        wrappedItem.setCachedRollingAggregateCounter(groupedRecordCount);
        wrappedItem.setCachedGroupByKey(null);
        return true;
    }

    private static void postProcessMax(MessageEntity messageEntity, List<BigDecimal> groupedMaxList) {
        if (messageEntity != null) {
            if (messageEntity.getBefore() != null && groupedMaxList.size() > 0) {
                messageEntity.getBefore().put(MAX, groupedMaxList.get(0));
            }
            if (messageEntity.getAfter() != null && groupedMaxList.size() > 0) {
                messageEntity.getAfter().put(MAX, groupedMaxList.get(0));
            }
        }
    }

    private static void postProcessMax(TapRecordEvent tapRecordEvent, List<BigDecimal> groupedMaxList) {
        if (tapRecordEvent != null) {
            final String maxStr = groupedMaxList.size() > 0 ? groupedMaxList.get(0).toPlainString() : null;
            Map<String, Object> before = TapEventUtil.getBefore(tapRecordEvent);
            if (before != null && groupedMaxList.size() > 0) {
                before.put(MAX, maxStr);
            }
            Map<String, Object> after = TapEventUtil.getAfter(tapRecordEvent);
            if (after != null && groupedMaxList.size() > 0) {
                after.put(MAX, maxStr);
            }
        }
    }

    private static List<BigDecimal> doInsertAndCut(List<BigDecimal> groupedMaxList, BigDecimal fieldValue, Comparator comparator, BigDecimal changedCount) {
        if (changedCount != null) {
            groupedMaxList.add(fieldValue);
        }
        groupedMaxList.sort(comparator);
        if (groupedMaxList.size() > MAX_LENGTH) {
            return new ArrayList(groupedMaxList.subList(0, MAX_LENGTH));
        } else {
            return groupedMaxList;
        }
    }

    private static boolean minMessageEntity(String aggregatorField, ConstructIMap<BigDecimal> cache, ConstructIMap<List<BigDecimal>> cacheList,
                                            WrapItem wrappedItem) throws Exception {
        String cacheKey = wrappedItem.getCachedGroupByKey();
        MessageEntity messageEntity = (MessageEntity) wrappedItem.getMessage();
        final OperationType operationType = OperationType.fromOp(messageEntity.getOp());

        BigDecimal fieldValue = preProcessField(messageEntity, aggregatorField);

        // 累计的行数
        BigDecimal groupedRecordCount = updateCounter(cache, cacheKey, messageEntity, wrappedItem.getChangedCount());
        BigDecimal changedCount = wrappedItem.getChangedCount();
        if (cacheList.exists(cacheKey)) {
            List<BigDecimal> groupedMinList = cacheList.find(cacheKey);
            if (groupedMinList.contains(fieldValue)) {
                if (operationType == OperationType.DELETE) {
                    groupedMinList.remove(fieldValue);
                } else if (operationType == OperationType.INSERT) {
                    logger.debug("cacheList already contains this value:" + fieldValue + " do not need to insert");
                } else {
                    if (changedCount.compareTo(BigDecimal.ZERO) < 0) {
                        groupedMinList.remove(fieldValue);
                    } else {
                        throw new RuntimeException("unimplement code");
                    }
                }
            } else {
                if (operationType == OperationType.DELETE) {
                    logger.debug("cacheList didn't contains this value:" + fieldValue + " can not delete");
                } else if (operationType == OperationType.INSERT) {
                    groupedMinList = doInsertAndCut(groupedMinList, fieldValue, new MinComparator(), changedCount);
                } else {
                    if (changedCount == null || changedCount.compareTo(BigDecimal.ZERO) <= 0) {
                        throw new RuntimeException("unimplement code");
                    } else {
                        groupedMinList = doInsertAndCut(groupedMinList, fieldValue, new MinComparator(), changedCount);
                    }
                }
            }
            if (groupedMinList.size() > 0) {
                cacheList.update(cacheKey, groupedMinList);
            } else {
                cacheList.delete(cacheKey);
            }
            postProcessMin(messageEntity, groupedMinList);
        } else {
            if (operationType == OperationType.INSERT) {
                ArrayList<BigDecimal> groupedMinList = new ArrayList<>(1);
                groupedMinList.add(fieldValue);
                cacheList.insert(cacheKey, groupedMinList);
                postProcessMin(messageEntity, groupedMinList);
            } else {
                throw new RuntimeException("unexpect logic");
            }
        }

        wrappedItem.setCachedRollingAggregateCounter(groupedRecordCount);
        wrappedItem.setCachedGroupByKey(null);
        return true;
    }

    private static boolean minTapRecordEvent(String aggregatorField, ConstructIMap<BigDecimal> cache, ConstructIMap<List<BigDecimal>> cacheList, WrapItem wrappedItem) throws Exception {
        String cacheKey = wrappedItem.getCachedGroupByKey();
        TapRecordEvent tapRecordEvent = (TapRecordEvent) wrappedItem.getMessage();

        BigDecimal fieldValue = preProcessField(tapRecordEvent, aggregatorField);

        // 累计的行数
        BigDecimal groupedRecordCount = updateCounter(cache, cacheKey, tapRecordEvent, wrappedItem.getChangedCount());
        BigDecimal changedCount = wrappedItem.getChangedCount();
        if (cacheList.exists(cacheKey)) {
            List<BigDecimal> groupedMinList = cacheList.find(cacheKey);
            groupedMinList.sort(new MinComparator());
            if (groupedMinList.contains(fieldValue)) {
                if (tapRecordEvent instanceof TapDeleteRecordEvent) {
                    groupedMinList.remove(fieldValue);
                } else if (tapRecordEvent instanceof TapInsertRecordEvent) {
                    logger.debug("cacheList already contains this value:" + fieldValue + " do not need to insert");
                } else {
                    if (changedCount.compareTo(BigDecimal.ZERO) < 0) {
                        groupedMinList.remove(fieldValue);
                    } else if (changedCount.compareTo(BigDecimal.ZERO) > 0) {
                        logger.debug("cacheList already contains this value:" + fieldValue + " do not need to insert");
                    } else {
                        throw new RuntimeException("unimplement code");
                    }
                }
            } else {
                if (tapRecordEvent instanceof TapDeleteRecordEvent) {
                    logger.debug("cacheList didn't contains this value:" + fieldValue + " can not delete");
                } else if (tapRecordEvent instanceof TapInsertRecordEvent) {
                    groupedMinList = doInsertAndCut(groupedMinList, fieldValue, new MinComparator(), changedCount);
                } else {
                    if (changedCount == null || changedCount.compareTo(BigDecimal.ZERO) <= 0) {
                        throw new RuntimeException("unimplement code");
                    } else {
                        groupedMinList = doInsertAndCut(groupedMinList, fieldValue, new MinComparator(), changedCount);
                    }
                }
            }
            if (groupedMinList.size() > 0) {
                cacheList.update(cacheKey, groupedMinList);
            } else {
                cacheList.delete(cacheKey);
            }
            postProcessMin(tapRecordEvent, groupedMinList);
        } else {
            if (tapRecordEvent instanceof TapInsertRecordEvent) {
                ArrayList<BigDecimal> groupedMinList = new ArrayList<>(1);
                groupedMinList.add(fieldValue);
                cacheList.insert(cacheKey, groupedMinList);
                postProcessMin(tapRecordEvent, groupedMinList);
            } else {
                throw new RuntimeException("unexpect logic");
            }
        }

        wrappedItem.setCachedRollingAggregateCounter(groupedRecordCount);
        wrappedItem.setCachedGroupByKey(null);
        return true;
    }

    private static void postProcessMin(MessageEntity messageEntity, List<BigDecimal> groupedMinList) {
        if (messageEntity != null) {
            if (messageEntity.getBefore() != null && groupedMinList.size() > 0) {
                messageEntity.getBefore().put(MIN, groupedMinList.get(0));
            }
            if (messageEntity.getAfter() != null && groupedMinList.size() > 0) {
                messageEntity.getAfter().put(MIN, groupedMinList.get(0));
            }
        }
    }

    private static void postProcessMin(TapRecordEvent tapRecordEvent, List<BigDecimal> groupedMinList) {
        if (tapRecordEvent != null) {
            Map<String, Object> before = TapEventUtil.getBefore(tapRecordEvent);
            final String minStr = groupedMinList.size() > 0 ? groupedMinList.get(0).toPlainString() : null;
            if (before != null && groupedMinList.size() > 0) {
                before.put(MIN, minStr);
            }
            Map<String, Object> after = TapEventUtil.getAfter(tapRecordEvent);
            if (after != null && groupedMinList.size() > 0) {
                after.put(MIN, minStr);
            }
        }
    }

    public static class MinComparator implements Comparator {
        @Override
        public int compare(Object o1, Object o2) {
            if (o1 == null && o2 == null) {
                return 0;
            } else if (o1 == null) {
                return 1;
            } else if (o2 == null) {
                return -1;
            } else {
                Comparable<Object> c1 = (Comparable<Object>) o1;
                Comparable<Object> c2 = (Comparable<Object>) o2;
                return c1.compareTo(c2);
            }
        }
    }

    public static class MaxComparator implements Comparator {
        @Override
        public int compare(Object o1, Object o2) {
            if (o1 == null && o2 == null) {
                return 0;
            } else if (o1 == null) {
                return 1;
            } else if (o2 == null) {
                return -1;
            } else {
                Comparable<Object> c1 = (Comparable<Object>) o1;
                Comparable<Object> c2 = (Comparable<Object>) o2;
                return c2.compareTo(c1);
            }
        }
    }
}

