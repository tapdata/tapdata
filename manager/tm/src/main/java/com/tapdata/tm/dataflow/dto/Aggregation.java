/**
 * @title: Aggregation
 * @description:
 * @author lk
 * @date 2021/9/6
 */
package com.tapdata.tm.dataflow.dto;

import java.util.List;

public class Aggregation {

	private String filterPredicate;

	private String aggFunction;

	private String aggExpression;

	private List<String> groupByExpression;

	private String name;

	public String getFilterPredicate() {
		return filterPredicate;
	}

	public String getAggFunction() {
		return aggFunction;
	}

	public String getAggExpression() {
		return aggExpression;
	}

	public List<String> getGroupByExpression() {
		return groupByExpression;
	}

	public String getName() {
		return name;
	}

	public void setFilterPredicate(String filterPredicate) {
		this.filterPredicate = filterPredicate;
	}

	public void setAggFunction(String aggFunction) {
		this.aggFunction = aggFunction;
	}

	public void setAggExpression(String aggExpression) {
		this.aggExpression = aggExpression;
	}

	public void setGroupByExpression(List<String> groupByExpression) {
		this.groupByExpression = groupByExpression;
	}

	public void setName(String name) {
		this.name = name;
	}
}
