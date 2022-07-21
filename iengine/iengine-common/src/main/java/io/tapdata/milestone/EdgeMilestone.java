package io.tapdata.milestone;

import com.tapdata.entity.Milestone;

import java.io.Serializable;
import java.util.List;

/**
 * @author samuel
 * @Description
 * @create 2021-07-29 21:34
 **/
public class EdgeMilestone implements Serializable {

	private static final long serialVersionUID = 4945470694231731437L;
	private String srcVertexName;
	private String trgVertexName;
	private List<Milestone> milestones;

	public EdgeMilestone() {
	}

	public EdgeMilestone(String srcVertexName, String trgVertexName, List<Milestone> milestones) {
		this.srcVertexName = srcVertexName;
		this.trgVertexName = trgVertexName;
		this.milestones = milestones;
	}

	public String getSrcVertexName() {
		return srcVertexName;
	}

	public String getTrgVertexName() {
		return trgVertexName;
	}

	public List<Milestone> getMilestones() {
		return milestones;
	}

	@Override
	public String toString() {
		return "EdgeMilestone{" +
				"srcVertexName='" + srcVertexName + '\'' +
				", trgVertexName='" + trgVertexName + '\'' +
				", milestones=" + milestones +
				'}';
	}
}
