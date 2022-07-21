package com.tapdata.entity;

import java.io.Serializable;
import java.util.List;

/**
 * @author jackin
 */
public class OplogFilter implements Serializable {

	private static final long serialVersionUID = 1810181846184735588L;
	private String action;

	private List<Filter> filters;

	public String getAction() {
		return action;
	}

	public void setAction(String action) {
		this.action = action;
	}

	public List<Filter> getFilters() {
		return filters;
	}

	public void setFilters(List<Filter> filters) {
		this.filters = filters;
	}
}
