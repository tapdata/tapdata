/**
 * @title: EdgeMilestone
 * @description:
 * @author lk
 * @date 2021/9/6
 */
package com.tapdata.tm.dataflow.dto;

import java.util.List;

public class EdgeMilestone {

	private String srcVertexName;

	private String trgVertexName;

	private List<Milestone> milestones;

	public String getSrcVertexName() {
		return srcVertexName;
	}

	public String getTrgVertexName() {
		return trgVertexName;
	}

	public List<Milestone> getMilestones() {
		return milestones;
	}

	public void setSrcVertexName(String srcVertexName) {
		this.srcVertexName = srcVertexName;
	}

	public void setTrgVertexName(String trgVertexName) {
		this.trgVertexName = trgVertexName;
	}

	public void setMilestones(List<Milestone> milestones) {
		this.milestones = milestones;
	}
}
