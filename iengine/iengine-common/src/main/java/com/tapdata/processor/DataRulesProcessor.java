package com.tapdata.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.JSONUtil;
import com.tapdata.constant.MapUtil;
import com.tapdata.constant.MongodbUtil;
import com.tapdata.entity.DataQualityTag;
import com.tapdata.entity.DataRules;
import com.tapdata.entity.Mapping;
import com.tapdata.entity.MessageEntity;
import com.tapdata.entity.TapLog;
import com.tapdata.entity.dataflow.Stage;
import com.tapdata.processor.dataflow.DataFlowProcessor;
import com.tapdata.processor.dataflow.ProcessorContext;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * @author jackin
 */
public class DataRulesProcessor implements Processor, DataFlowProcessor {

	private static final Logger logger = LogManager.getLogger(DataRulesProcessor.class);

	public static final String AND = "and";
	public static final String OR = "or";

	public static final String LT = "lt";
	public static final String LTE = "lte";
	public static final String GT = "gt";
	public static final String GTE = "gte";
	public static final String NONE = "none";

	private Mapping mapping;
	private List<Mapping> mappings;
	private Map<String, List<DataRules>> dataRulesMap;
	private Stage stage;
	private ProcessorContext context;

	public DataRulesProcessor() {
	}

	public DataRulesProcessor(Map<String, List<DataRules>> dataRulesMap, List<Mapping> mappings) {
		this.mappings = mappings;
		this.dataRulesMap = dataRulesMap;
	}

	public MessageEntity process(MessageEntity record) {

		Map<String, Object> values = MapUtils.isNotEmpty(record.getAfter()) ? record.getAfter() : record.getBefore();
		this.mapping = record.getMapping();
		if (mapping != null) {

			dataRuleValidate(record, values);

		} else {
			String fromTable = record.getTableName();
			this.mapping = getMapping(fromTable);
			String op = record.getOp();

			if (this.mapping != null && MapUtils.isNotEmpty(values) && !op.equals(ConnectorConstant.MESSAGE_OPERATION_DELETE)) {

				dataRuleValidate(record, values);
			}
		}

		return record;
	}

	private void dataRuleValidate(MessageEntity record, Map<String, Object> values) {
		final String toTable = mapping.getTo_table();
		List<DataRules> dataRules = dataRulesMap.get(toTable);
		if (CollectionUtils.isNotEmpty(dataRules)) {

			// init DataQualityTag
			DataQualityTag dataQualityTag = record.getDataQualityTag();
			if (dataQualityTag == null) {
				dataQualityTag = new DataQualityTag(DataQualityTag.INVALID_RESULT);
				record.setDataQualityTag(dataQualityTag);
			}
			List<DataQualityTag.HitRules> hitRules = dataQualityTag.getHitRules();
			if (hitRules == null) {
				hitRules = new ArrayList<>();
				dataQualityTag.setHitRules(hitRules);
			}
			List<DataQualityTag.HitRules> passRules = dataQualityTag.getPassRules();
			if (passRules == null) {
				passRules = new ArrayList<>();
				dataQualityTag.setPassRules(passRules);
			}

			// start validate by each rules
			Map<String, Map<String, Object>> fieldRulesMap = getFieldRulesMap(dataRules);
			for (Map.Entry<String, Map<String, Object>> entry : fieldRulesMap.entrySet()) {
				if (entry != null) {
					String fieldName = entry.getKey();
					Map<String, Object> rules = entry.getValue();

					valueRulesValidate(values, rules, fieldName, hitRules, passRules);
				}
			}

			if (dataQualityTag.isNotEmpty()) {
				record.setDataQualityTag(dataQualityTag);
			} else {
				record.setDataQualityTag(null);
			}
		}
	}

	private Map<String, Map<String, Object>> getFieldRulesMap(List<DataRules> dataRules) {
		Map<String, Map<String, Object>> fieldRulesMap = new HashMap<>();

		if (CollectionUtils.isNotEmpty(dataRules)) {
			for (DataRules dataRule : dataRules) {

				String fieldName = dataRule.getFieldName();
				String rulesStr = dataRule.getRules();

				try {
					Map<String, Object> rule = Document.parse(rulesStr);

					if (fieldRulesMap.containsKey(fieldName)) {
						fieldRulesMap.get(fieldName).putAll(rule);
					} else {
						fieldRulesMap.put(fieldName, rule);
					}


				} catch (Exception e) {
					logger.warn("Data rule syntax is wrong, field name: {}, rule string: {}", fieldName, rulesStr);
				}
			}
		}

		return fieldRulesMap;
	}

	@Override
	public List<MessageEntity> process(List<MessageEntity> batch) {
		if (batch != null) {
			for (MessageEntity msg : batch) {
				if (msg != null) {
					process(msg);
				} else {
					continue;
				}
			}
		}
		return batch;
	}

	@Override
	public void stop() {

	}

	private Mapping getMapping(String fromTable) {
		for (Mapping mapping : mappings) {
			if (mapping.getFrom_table().equals(fromTable)) {
				return mapping;
			}
		}
		return null;
	}

	private void valueRulesValidate(Map<String, Object> values, Map<String, Object> rules, String fieldName,
									List<DataQualityTag.HitRules> hitRules, List<DataQualityTag.HitRules> passRules) {

		Object value = MapUtil.getValueByKey(values, fieldName);

		for (Map.Entry<String, Object> entry : rules.entrySet()) {
			String ruleType = entry.getKey();
			Object condition = entry.getValue();

			Boolean result = true;

			switch (DataRules.RuleType.fromString(ruleType)) {
				case NULLABLE:
					result = nullable(value, condition);
					break;
				case TYPE:
					result = type(value, condition);
					break;
				case RANGE:
					result = range(value, condition);
					break;
				case ENUM:
					result = enumChk(value, condition);
					break;
				case REGEX:
					result = regexChk(value, condition);
				default:
					break;
			}

			String ruleStr;
			try {
				ruleStr = JSONUtil.map2Json(new HashMap<String, Object>() {{
					put(ruleType, condition);
				}});

				ruleStr = ruleStr.replace("\\\\", "\\");

				DataQualityTag.HitRules hitRule = buildHitRule(ruleStr, fieldName, values);
				if (result == null) {
					// trigger exception or some unsupport operation
					continue;
				} else if (!result) {
					hitRules.add(hitRule);
				} else {
//                passRules.add(hitRule);
				}
			} catch (JsonProcessingException e) {
				continue;
			}
		}
	}

	public static Boolean exists(Object value, Object condition) {
		return nullable(value, condition);
	}

	public static Boolean nullable(Object value, Object condition) {
		Boolean con;
		try {
			con = (boolean) condition;
		} catch (Exception e) {
			return true;
		}
		if (con == null || con) return true;
		else return value != null;
	}

	public static Boolean type(Object value, Object condition) {
		if (value == null) return true;

		String con;
		try {
			con = condition.toString();
		} catch (Exception e) {
			return null;
		}

		switch (con) {
			case "string":
				return value.getClass().equals(String.class);
			case "short":
			case "int":
			case "long":
				return value.getClass().equals(Short.class) || value.getClass().equals(Integer.class) || value.getClass().equals(Long.class);
			case "double":
				return value.getClass().equals(Double.class);
			case "float":
				return value.getClass().equals(Float.class);
			case "bool":
			case "boolean":
				return value.getClass().equals(Boolean.class);
			default:
				return true;
		}
	}

	public static Boolean range(Object value, Object condition) {
		Map<String, Object> con;
		Double valueNumber;

		if (value == null) return true;

		try {
			con = (Map<String, Object>) condition;
			valueNumber = Double.valueOf(value.toString());
		} catch (Exception e) {
			return null;
		}

		if (con == null || valueNumber == null) return null;

		int compareResult = 1;
		boolean result = true;

		if (MapUtils.isNotEmpty(con) && con.size() >= 2) {
			for (Map.Entry<String, Object> entry : con.entrySet()) {

				String op = entry.getKey();
				Double compareNumber;
				try {
					if (!op.equals(NONE)) {
						compareNumber = Double.valueOf(entry.getValue().toString());
						compareResult = valueNumber.compareTo(compareNumber);
					}
				} catch (Exception e) {
					return null;
				}

				switch (op) {
					case LT:
						result = compareResult == -1;
						break;
					case LTE:
						result = (compareResult == -1 || compareResult == 0);
						break;
					case GT:
						result = compareResult == 1;
						break;
					case GTE:
						result = (compareResult == 1 || compareResult == 0);
						break;
					case NONE:
					default:
						result = true;
						break;
				}

				if (!result) {
					break;
				}
			}
			return result;
		} else {
			return null;
		}
	}

	public static Boolean enumChk(Object value, Object condition) {
		if (value == null) return true;
		List<Object> con;
		try {
			con = (List<Object>) condition;
		} catch (Exception e) {
			return null;
		}

		if (CollectionUtils.isEmpty(con)) return null;

		for (Object o : con) {
			if (value.equals(o)) {
				return true;
			}
		}

		return false;
	}

	public static Boolean regexChk(Object value, Object condition) {
		if (value == null || !(condition instanceof String)) return true;

		try {
			String regex = (String) condition;
			String patternStr = (String) value;

			return Pattern.matches(regex, patternStr);
		} catch (Exception e) {
			return null;
		}
	}

	private DataQualityTag.HitRules buildHitRule(String rules, String fieldName, Map<String, Object> values) {
		DataQualityTag.HitRules hitRules = new DataQualityTag.HitRules();

		if (StringUtils.isNotBlank(rules)) {
			hitRules.setFieldName(fieldName);
			hitRules.setRules(rules);

			String relationship = mapping.getRelationship();
			String target_path = mapping.getTarget_path();
			if (StringUtils.isNotBlank(target_path)) {
				switch (relationship) {
					case ConnectorConstant.RELATIONSHIP_ONE_ONE:
					case ConnectorConstant.RELATIONSHIP_ONE_MANY:

						hitRules.setFieldName(target_path + "." + hitRules.getFieldName());
						break;

					case ConnectorConstant.RELATIONSHIP_MANY_ONE:

						hitRules.setFieldName(target_path + "." + hitRules.getFieldName());
						List<Map<String, String>> match_condition = mapping.getMatch_condition();

						Map<String, Object> keys = new HashMap<>();
						hitRules.setKeys(keys);
						for (Map<String, String> condition : match_condition) {
							String field = condition.get("source");
							keys.put(field, values.get(field));
						}

						break;
					default:
						break;
				}
			}
		} else {
			return null;
		}

		return hitRules;
	}

	@Override
	public void initialize(ProcessorContext context, Stage stage) throws Exception {
		Map<String, List<DataRules>> dataRulesMap = MongodbUtil.getDataRules(context.getClientMongoOperator(), context.getTargetConn());
		if (dataRulesMap == null) {
			logger.error(TapLog.CONN_ERROR_0025.getMsg());
		}
		this.dataRulesMap = dataRulesMap;
		this.context = context;
		this.stage = stage;
		this.mappings = context.getJob().getMappings();
	}

	@Override
	public Stage getStage() {
		return stage;
	}
}
