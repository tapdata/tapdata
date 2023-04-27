/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tapdata.processor.dataflow.aggregation;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.MapUtil;
import com.tapdata.entity.Mapping;
import com.tapdata.entity.MessageEntity;
import com.tapdata.entity.dataflow.Aggregation;
import com.tapdata.entity.dataflow.Stage;
import com.tapdata.processor.dataflow.DataFlowProcessor;
import com.tapdata.processor.dataflow.ProcessorContext;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.averagingDouble;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.summarizingDouble;
import static java.util.stream.Collectors.toList;

/**
 * 内存聚合计算处理器，将所有源数据都存储到内存中，然后通过java streams进行全量集合计算
 *
 * @author jackin
 */
public class MemoryAggregationProcessor implements DataFlowProcessor {

	private Logger logger = LogManager.getLogger(MemoryAggregationProcessor.class);

	private Stage stage;
	private List<AggregationConfig> configs;

	/**
	 * key: table name
	 * value:
	 * -- key: pks value
	 * -- value: record
	 */
	private Map<String, Map<String, Map<String, Object>>> data;

	private ProcessorContext context;

	private Map<String, List<String>> tablePKs;

	private AtomicInteger recordCount = new AtomicInteger();

	@Override
	public void initialize(ProcessorContext context, Stage stage) throws Exception {
		this.stage = stage;
		data = new HashMap<>();
		this.context = context;

		configs = new ArrayList<>();
		List<Aggregation> aggregations = stage.getAggregations();
		if (CollectionUtils.isEmpty(aggregations)) {
			throw new RuntimeException("Aggregation processor config cannot not be empty");
		}
		for (Aggregation aggregation : aggregations) {
			aggregation.setJsEngineName(this.stage.getJsEngineName());
			configs.add(new AggregationConfig(
					aggregation
			));
		}

		List<Mapping> mappings = context.getJob().getMappings();
		if (CollectionUtils.isNotEmpty(mappings)) {
			tablePKs = new HashMap<>();
			for (Mapping mapping : mappings) {
				String fromTable = mapping.getFrom_table();
				String primaryKeys = stage.getPrimaryKeys();
				if (StringUtils.isNotBlank(primaryKeys)) {
					tablePKs.put(fromTable, Arrays.asList(primaryKeys.split(",")));
				}
			}
		}
	}

	@Override
	public Stage getStage() {
		return stage;
	}

	public MessageEntity process(MessageEntity msg) {

		addData(msg);

		return msg;
	}

	private void addData(MessageEntity msg) {
		String op = msg.getOp();
		String tableName = msg.getTableName();
		Map<String, Object> value = msg.getAfter();
		if (ConnectorConstant.MESSAGE_OPERATION_DELETE.equals(op)) {
			value = msg.getBefore();
		}
		if (MapUtils.isNotEmpty(value)) {
			try {
				if (!data.containsKey(tableName)) {
					data.put(tableName, new HashMap<>());
				}

				List<String> pks = null;
				if (MapUtils.isNotEmpty(tablePKs) && tablePKs.containsKey(tableName)) {
					pks = tablePKs.get(tableName);
				}

				StringBuilder sb = new StringBuilder();
				if (CollectionUtils.isNotEmpty(pks)) {
					for (String pk : pks) {
						Object pkValue = MapUtil.getValueByKey(value, pk);
						if (pkValue != null) {
							// 兼容 数字、字符串、日志、ObjectId
							sb.append(String.valueOf(pkValue));
						}
					}
				}

				Map<String, Map<String, Object>> pkRecords = data.get(tableName);
				if (StringUtils.isNotBlank(sb)) {
					pkRecords.put(sb.toString(), value);
					recordCount.set(0);
				} else {
					pkRecords.put(String.valueOf(pkRecords.size()), value);
					recordCount.incrementAndGet();
				}
				if (recordCount.get() >= 1000) {
					logger.warn("AggregationProcessor primary key is not correct,stage name: {}", stage.getName());
					recordCount.set(0);
				}

			} catch (Exception e) {
				if (context.getJob().getStopOnError()) {
					throw new RuntimeException("Aggregate " + msg + " failed " + e.getMessage() + ", stop on error is true, will stop replicator.", e);
				}
				e.printStackTrace();
				logger.warn("Aggregate {} failed {}, will skip it.", msg, e.getMessage());
			}
		}
	}

	@Override
	public List<MessageEntity> process(List<MessageEntity> msgs) {

		String sourceStageId = null;
		if (CollectionUtils.isNotEmpty(msgs)) {
			for (MessageEntity msg : msgs) {
				addData(msg);
				sourceStageId = msg.getSourceStageId();
			}
		}

		List<MessageEntity> aggregateData = aggregate(sourceStageId);
		return aggregateData;
	}

	private List<MessageEntity> aggregate(String sourceStageId) {
		List<MessageEntity> msgs = null;
		if (MapUtils.isNotEmpty(data)) {
			msgs = new ArrayList<>();
			for (Map.Entry<String, Map<String, Map<String, Object>>> entry : data.entrySet()) {

				String tableName = entry.getKey();
				for (AggregationConfig config : configs) {
					FilterEval filterEval = config.getFilterEval();
					Map<String, Map<String, Object>> pkRecods = entry.getValue();
					Stream<Map<String, Object>> stream = pkRecods.values().parallelStream();
					List<Map<String, Object>> filterList = stream.filter(record -> filterEval != null ? filterEval.filter(record) : true).collect(toList());

					aggregate(
							sourceStageId,
							msgs,
							tableName,
							config.getAggregation(),
							filterList
					);
				}
			}
		}

		return msgs;
	}

	public static List<Map<String, Object>> aggregate(
			String sourceStageId,
			List<MessageEntity> msgs,
			String tableName,
			Aggregation aggregation,
			Collection<Map<String, Object>> filterList
	) {
		List<Map<String, Object>> resutls = new ArrayList<>();
		List<String> groupByExpression = aggregation.getGroupByExpression();
		if (CollectionUtils.isNotEmpty(groupByExpression)) {
			Function<Map<String, Object>, ?> mandatory = null;
			Function<Map<String, Object>, ?>[] others = null;

			if (groupByExpression.size() > 1) {
				others = new Function[groupByExpression.size() - 1];
			}
			for (int i = 0; i < groupByExpression.size(); i++) {
				String str = groupByExpression.get(i);
				if (i > 0) {
					others[i - 1] = record -> MapUtil.getValueByKey(record, str);

				} else {
					mandatory = record -> MapUtil.getValueByKey(record, str);
				}

			}

			Map<List<Object>, List<Map<String, Object>>> groupByResult = null;
			if (others != null) {
				groupByResult = groupListBy(filterList, mandatory, others);
			} else {
				groupByResult = groupListBy(filterList, mandatory);
			}

			for (Map.Entry<List<Object>, List<Map<String, Object>>> listEntry : groupByResult.entrySet()) {
				List<Object> groups = listEntry.getKey();
				List<Map<String, Object>> list = listEntry.getValue();
				Object result = aggregatorProcess(list, aggregation);

				Map<String, Object> aggId = new HashMap<>();
				aggId.put(LRUAggregationProcessor.AGG_RET_TAP_SUB_NAME_FIELD, aggregation.getName());
				Map<String, Object> value = new HashMap<>();
				value.put(aggregation.getAggFunction(), result);
				for (int i = 0; i < groupByExpression.size(); i++) {
					String key = groupByExpression.get(i);
					Object groupValue = groups.size() - 1 >= i ? groups.get(i) : null;
					value.put(key, groupValue);
					aggId.put(key, groupValue);
				}
				value.put("_id", aggId);

				resutls.add(value);
//				MessageEntity messageEntity = new MessageEntity(
//					ConnectorConstant.MESSAGE_OPERATION_INSERT,
//					value,
//					tableName
//				);
//				messageEntity.setSourceStageId(sourceStageId);
//				msgs.add(messageEntity);
			}
		} else {
			Object result = aggregatorProcess(filterList, aggregation);
			Map<String, Object> value = new HashMap<>();
			value.put(aggregation.getAggFunction(), result);
			value.put("_id", aggregation.getName());
			resutls.add(value);

//			MessageEntity messageEntity = new MessageEntity(
//				ConnectorConstant.MESSAGE_OPERATION_INSERT,
//				value,
//				tableName
//			);
//			messageEntity.setSourceStageId(sourceStageId);
//			msgs.add(messageEntity);
		}

		return resutls;
	}


	public static Object aggregatorProcess(Collection<Map<String, Object>> list, Aggregation aggregation) {
		Object result = 0D;
		String aggFunction = aggregation.getAggFunction();
		String aggExpression = aggregation.getAggExpression();
		switch (aggFunction.toUpperCase()) {
			case "AVG":
				result = list.stream().collect(averagingDouble(
						record -> getRecordDoubleValue(aggExpression, record)
				));
				break;
			case "SUM":
				result = list.stream().collect(summarizingDouble(
						record -> getRecordDoubleValue(aggExpression, record)
				)).getSum();
				break;
			case "MAX":
				final Comparator<Map<String, Object>> compMax = (r1, r2) -> {
					final Object value1 = MapUtil.getValueByKey(r1, aggExpression);
					final Object value2 = MapUtil.getValueByKey(r2, aggExpression);

					return ((Comparable) value1).compareTo(value2);
				};

				Map<String, Object> maxRecord = list.stream()
						.max(compMax)
						.get();
				result = MapUtil.getValueByKey(maxRecord, aggExpression);
				break;
			case "MIN":
				final Comparator<Map<String, Object>> compMin = (r1, r2) -> {
					final Object value1 = MapUtil.getValueByKey(r1, aggExpression);
					final Object value2 = MapUtil.getValueByKey(r2, aggExpression);

					return ((Comparable) value1).compareTo(value2);
				};

				Map<String, Object> minRecord = list.stream()
						.min(compMin)
						.get();

				result = MapUtil.getValueByKey(minRecord, aggExpression);
				break;
			case "COUNT":
				result = list.stream().count();
				break;
			default:
				break;
		}

		return result;
	}

	private static double getRecordDoubleValue(String aggExpression, Map<String, Object> record) {
		Object valueByKey = MapUtil.getValueByKey(record, aggExpression);
		if (valueByKey == null) {
			return 0D;
		}
		try {
			return Double.parseDouble(valueByKey.toString());
		} catch (NumberFormatException e) {
			return 0D;
		}
	}

	private static Map<List<Object>, List<Map<String, Object>>> groupListBy(Collection<Map<String, Object>> data, Function<Map<String, Object>, ?> mandatory, Function<Map<String, Object>, ?>... others) {
		return data.stream()
				.collect(groupingBy(cl -> Stream.concat(Stream.of(mandatory), Stream.of(others)).map(f -> f.apply(cl)).collect(toList())));
	}

	@Override
	public void stop() {

	}

	public static void main(String[] args) throws Exception {
		Stage config = new Stage();
		config.setAggregations(Arrays.asList(new Aggregation(
				"record.age >= 23 && record.sex == '男'",
				"COUNT",
				"age",
				Arrays.asList("sex", "name")
		)));

//    MemoryAggregationProcessor processor = new MemoryAggregationProcessor();
//    processor.initialize(null, config);

		Map<String, Object> record1 = new HashMap<>();
		record1.put("age", 25);
		record1.put("sex", "男");
		record1.put("name", "n1");
		Map<String, Object> record2 = new HashMap<>();
		record2.put("age", 23);
		record2.put("sex", "男");
		record2.put("name", "n1");
		Map<String, Object> record3 = new HashMap<>();
		record3.put("age", 35);
		record3.put("sex", "男");
		record3.put("name", "n2");
		Map<String, Object> record4 = new HashMap<>();
		record4.put("age", 35);
		record4.put("sex", "女");
		record4.put("name", "w1");
		Map<String, Object> record5 = new HashMap<>();
//    record5.put("age", null);
		record5.put("sex", "女");
		record5.put("name", "w1");

		List<MessageEntity> messageEntities = Arrays.asList(new MessageEntity(
				"i", record1, "Persons"
		), new MessageEntity(
				"i", record2, "Persons"
		), new MessageEntity(
				"i", record3, "Persons"
		), new MessageEntity(
				"i", record4, "Persons"
		));

//    List<MessageEntity> process = processor.process(messageEntities);
//
//    for (MessageEntity messageEntity : process) {
//      System.out.println(messageEntity);
//    }
	}
}
