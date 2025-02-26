package io.tapdata.flow.engine.V2.node;

import com.tapdata.tm.autoinspect.constants.AutoInspectConstants;
import com.tapdata.tm.commons.dag.process.HuaweiDrsKafkaConvertorNode;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author jackin
 * @date 2021/12/1 9:57 PM
 **/
public enum NodeTypeEnum {
	DATABASE("database"),
	TABLE("table"),
	CACHE("mem_cache"),
	AUTO_INSPECT(AutoInspectConstants.NODE_TYPE),
	VIRTUAL_TARGET("VirtualTarget"),
	JOIN("join_processor"),
	CACHE_LOOKUP_PROCESSOR("cache_lookup_processor"),
	ROW_FILTER_PROCESSOR("row_filter_processor"),
	JS_PROCESSOR("js_processor"),
	STANDARD_JS_PROCESSOR("standard_js_processor"),
	UNION_PROCESSOR("union_processor"),
	MIGRATE_UNION_PROCESSOR("migrate_union_processor"),

	MIGRATE_JS_PROCESSOR("migrate_js_processor"),
	MIGRATE_PYTHON_PROCESS("migrate_python_process"),
	STANDARD_MIGRATE_JS_PROCESSOR("standard_migrate_js_processor"),
	FIELD_PROCESSOR("field_processor"),
	AGGREGATION_PROCESSOR("aggregation_processor"),
	LOG_COLLECTOR("logCollector"),
	HAZELCASTIMDG("hazelcastIMDG"),
	MERGETABLE("merge_table_processor"),
	CUSTOM_PROCESSOR("custom_processor"),
	FIELD_RENAME_PROCESSOR("field_rename_processor"),
	FIELD_ADD_DEL_PROCESSOR("field_add_del_processor"),
	FIELD_CALC_PROCESSOR("field_calc_processor"),
	FIELD_MOD_TYPE_PROCESSOR("field_mod_type_processor"),

	TABLE_RENAME_PROCESSOR("table_rename_processor"),
	MIGRATE_FIELD_RENAME_PROCESSOR("migrate_field_rename_processor"),
	MIGRATE_DATE_PROCESSOR("migrate_date_processor"),
	DATE_PROCESSOR("date_processor"),

	MIGRATE_FIELD_MOD_TYPE_FILTER_PROCESSOR("migrate_field_mod_type_filter_processor"),
	FIELD_MOD_TYPE_FILTER_PROCESSOR("field_mod_type_filter_processor"),

	PYTHON_PROCESS("python_processor"),

	UNWIND_PROCESS("unwind_processor"),
	ADD_DATE_FIELD_PROCESS("add_date_field_processor"),
	MIGRATE_ADD_DATE_FIELD_PROCESSOR("migrate_add_date_field_processor"),
	PREVIEW_TARGET("preview_target"),
    HUAWEI_DRS_KAFKA_CONVERTOR(HuaweiDrsKafkaConvertorNode.TYPE),
	;

	public final String type;

	NodeTypeEnum(String type) {
		this.type = type;
	}

	private static final Map<String, NodeTypeEnum> ENUM_MAP;

	static {
		Map<String, NodeTypeEnum> map = new ConcurrentHashMap<>();
		for (NodeTypeEnum instance : NodeTypeEnum.values()) {
			map.put(instance.type.toLowerCase(), instance);
		}
		ENUM_MAP = Collections.unmodifiableMap(map);
	}

	public static NodeTypeEnum get(String name) {
		return ENUM_MAP.get(name.toLowerCase());
	}
}
