package com.tapdata.entity.dataflow;

import java.io.Serializable;
import java.util.List;

/**
 * 聚合处理器配置项
 */
public class Aggregation implements Serializable {
	private static final long serialVersionUID = 7812338848941935255L;
	/**
	 * 聚合过滤器配置，js表达式，满足条件的数据参与聚合计算
	 */
	private String filterPredicate;

	/**
	 * 聚合函数名，eg: SUM/MAX/MIN/COUNT....
	 */
	private String aggFunction;

	/**
	 * 聚合函数对应的表达式，eg:字段名
	 */
	private String aggExpression;

	/**
	 * 分组聚合的字段名
	 */
	private List<String> groupByExpression;

	/**
	 * 聚合计算的名称
	 */
	private String name;

	/**
	 * 使用的js引擎名称（支持nashorn、graal.js）
	 */
	private String jsEngineName;

	public Aggregation() {
	}

	public Aggregation(String filterPredicate, String aggFunction, String aggExpression, List<String> groupByExpression) {
		this.filterPredicate = filterPredicate;
		this.aggFunction = aggFunction;
		this.aggExpression = aggExpression;
		this.groupByExpression = groupByExpression;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getFilterPredicate() {
		return filterPredicate;
	}

	public void setFilterPredicate(String filterPredicate) {
		this.filterPredicate = filterPredicate;
	}

	public String getAggFunction() {
		return aggFunction;
	}

	public void setAggFunction(String aggFunction) {
		this.aggFunction = aggFunction;
	}

	public String getAggExpression() {
		return aggExpression;
	}

	public void setAggExpression(String aggExpression) {
		this.aggExpression = aggExpression;
	}

	public List<String> getGroupByExpression() {
		return groupByExpression;
	}

	public void setGroupByExpression(List<String> groupByExpression) {
		this.groupByExpression = groupByExpression;
	}

	public String getJsEngineName() {
		return jsEngineName;
	}

	public void setJsEngineName(String jsEngineName) {
		this.jsEngineName = jsEngineName;
	}

	public enum AggregationFunction {
		SUM("SUM"),
		MAX("MAX"),
		MIN("MIN"),
		AVG("AVG"),
		COUNT("COUNT"),
		;

		private String functionName;

		AggregationFunction(String functionName) {
			this.functionName = functionName;
		}

		public String getFunctionName() {
			return functionName;
		}

		public static AggregationFunction getByFunctionName(String functionName) {
			for (AggregationFunction function : AggregationFunction.values()) {
				if (function.getFunctionName().equals(functionName)) {
					return function;
				}
			}

			return null;
		}


	}
}
