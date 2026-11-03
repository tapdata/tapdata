
package com.tapdata.tm.inspect.bean;

import java.util.List;
import java.util.Map;

import lombok.Data;

@Data
public class Task {
    private String taskId;
    /** */
    private Long batchSize;
    /** */
    private Object compareFn;
    /** */
    private Object confirmFn;
    /** */
    private Boolean fullMatch;
    /** */
    private Object limit;
    /** */
    private String script;
    /** */
    private Boolean showAdvancedVerification;
    /** */
    private Source source;
    /** */
	private Source target;

	/** Maps source custom-query result column names/aliases to target result column names/aliases. */
	private Map<String, String> fieldMapping;

	/**
	 * Source custom-query result column names/aliases used to order multi-row custom_query results.
	 * Target rows are mapped through fieldMapping before these names are applied. This differs from
	 * the legacy per-side Source.sortColumn, which is used for table-based inspection and does not
	 * control custom-query result ordering. Every configured name must exist in both compared rows
	 * after target field mapping.
	 */
	private List<String> sortFields;

    private  String webScript;
}
