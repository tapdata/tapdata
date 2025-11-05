package com.tapdata.tm.lineage.analyzer.entity;

import com.tapdata.tm.commons.dag.Node;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import java.util.Date;

/**
 * @author samuel
 * @Description
 * @create 2023-05-22 16:18
 **/
@Getter
@Setter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class LineageTask extends LineageAttr {
	public static final String ATTR_KEY = "tasks";
	private Node taskNode;
	private String syncType;
	private String status;
    private Date startTime;

	public LineageTask(String id, String name, Node taskNode, String syncType, String status, Date startTime) {
		super(id, ATTR_KEY, name);
		this.taskNode = taskNode;
		this.syncType = syncType;
		this.status = status;
        this.startTime = startTime;
	}
}
