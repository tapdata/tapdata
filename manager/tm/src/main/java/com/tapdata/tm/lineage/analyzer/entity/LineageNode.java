package com.tapdata.tm.lineage.analyzer.entity;

import com.tapdata.tm.commons.dag.Node;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;


/**
 * @author samuel
 * @Description
 * @create 2023-05-22 19:16
 **/
@Getter
@Setter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public abstract class LineageNode extends Node {
	private static final long serialVersionUID = -5823964448831847414L;

	public LineageNode(String id, String type) {
		super(type);
		this.id = id;
	}

	public static String genId(String... strings) {
		return String.join("_", strings);
	}
}
