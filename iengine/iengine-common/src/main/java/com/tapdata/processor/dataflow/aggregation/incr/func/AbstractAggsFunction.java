package com.tapdata.processor.dataflow.aggregation.incr.func;

import com.tapdata.entity.dataflow.Aggregation;
import com.tapdata.processor.dataflow.aggregation.FilterEval;
import com.tapdata.processor.dataflow.aggregation.incr.cache.BucketCache;
import com.tapdata.processor.dataflow.aggregation.incr.calc.ComposeCalculator;
import com.tapdata.processor.dataflow.aggregation.incr.service.SnapshotRecord;
import com.tapdata.processor.dataflow.aggregation.incr.service.SnapshotService;
import com.tapdata.processor.dataflow.aggregation.incr.service.model.AggrBucket;
import com.tapdata.processor.dataflow.aggregation.incr.service.model.BucketValue;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

abstract public class AbstractAggsFunction implements AggrFunction {

	private static final Logger log = LogManager.getLogger(AbstractAggsFunction.class);

	protected final String processName;
	protected final FilterEval filterEval;
	protected final List<String> groupByFieldList;
	protected final String valueField;

	protected final BucketCache<FuncCacheKey, BucketValue> cache;
	protected boolean hashCheckIndex = false;

	public AbstractAggsFunction(BucketCache<FuncCacheKey, BucketValue> cache, Aggregation aggregation) throws Throwable {
		this.processName = aggregation.getName();
		this.filterEval = StringUtils.isEmpty(aggregation.getFilterPredicate()) ? null : new FilterEval(aggregation.getFilterPredicate(), aggregation.getJsEngineName());
		this.groupByFieldList = Optional.ofNullable(aggregation.getGroupByExpression()).orElse(Collections.emptyList());
		this.valueField = aggregation.getAggExpression();
		this.cache = cache;
	}

	@Override
	public AggrBucket call(SnapshotService snapshotService, SnapshotRecord snapshotRecord) {
		if (!hashCheckIndex) {
			this.createIndex(snapshotService);
		}
		return this.doCall(snapshotService, snapshotRecord, this.formatBucketKey(snapshotRecord));
	}

	private void createIndex(SnapshotService snapshotService) {
		ArrayList<String> keyFieldList = new ArrayList<>(this.groupByFieldList);
		if (StringUtils.isNotBlank(this.valueField)) {
			keyFieldList.add(this.valueField);
		}
		if (keyFieldList.isEmpty()) {
			return;
		}
		String index = snapshotService.createIndex(keyFieldList);
		log.info("create index {} by groupByField: {}, valueField: {}", index, String.join(",", this.groupByFieldList), this.valueField);
		hashCheckIndex = true;
	}

	abstract protected AggrBucket doCall(SnapshotService snapshotService, SnapshotRecord snapshotRecord, Map<String, Object> groupByMap);

	@Override
	public String getProcessName() {
		return processName;
	}

	@Override
	public boolean isFilter(Map<String, Object> dataMap) {
		return this.filterEval == null || this.filterEval.filter(dataMap);
	}

	@Override
	public String getValueField() {
		return valueField;
	}

	@Override
	public Map<String, Object> formatBucketKey(SnapshotRecord snapshotRecord) {
		final LinkedHashMap<String, Object> groupByMap = new LinkedHashMap<>();
		for (String field : this.groupByFieldList) {
			groupByMap.put(field, snapshotRecord.getRecordValue(field));
		}
		return groupByMap;
	}

	@Override
	public boolean isBucketKeyEquals(SnapshotRecord r1, SnapshotRecord r2) {
		if (r1 == null || r2 == null) {
			return r1 == r2;
		}
		Map<String, Object> m1 = this.formatBucketKey(r1), m2 = this.formatBucketKey(r2);
		if (m1.size() != m2.size()) return false;
		return m1.entrySet().stream().allMatch(en -> {
			if (m2.containsKey(en.getKey())) {
				if (null == en.getValue()) {
					return null == m2.get(en.getKey());
				} else if (null == m2.get(en.getKey())) {
					return false;
				} else if (en.getValue() instanceof Short
						|| en.getValue() instanceof Integer
						|| en.getValue() instanceof Float
						|| en.getValue() instanceof Double
						|| en.getValue() instanceof BigDecimal) {
					return en.getValue().toString().equals(m2.get(en.getKey()).toString());
				}
				return en.getValue().equals(m2.get(en.getKey()));
			}
			return false;
		});
	}

	@Override
	public boolean isValueChanged(SnapshotRecord r1, SnapshotRecord r2) {
		if (StringUtils.isEmpty(this.valueField)) { // no field means no changed
			return false;
		}
		String valueField = this.getValueField();
		Number n1 = (Number) Optional.ofNullable(r1).map(r -> r.getRecordValue(valueField)).orElse(null);
		Number n2 = (Number) Optional.ofNullable(r2).map(r -> r.getRecordValue(valueField)).orElse(null);
		if (n1 == null || n2 == null) { // null check
			return n1 != n2;
		}
		return !ComposeCalculator.getInstance().eq(n1, n2);
	}
}
