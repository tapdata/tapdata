package com.tapdata.entity.task;

import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;

import java.util.ArrayList;
import java.util.List;

/**
 * @author samuel
 * @Description
 * @create 2022-04-11 15:24
 **/
public class NodeUtil {
	public static List<String> getTableNames(Node<?> node) {
		List<String> tableNames = new ArrayList<>();
		if (node instanceof TableNode) {
			tableNames.add(((TableNode) node).getTableName());
		} else if (node instanceof DatabaseNode) {
			tableNames.addAll(((DatabaseNode) node).getTableNames());
		}
		return tableNames;
	}

	public static String getConnectionId(Node<?> node) {
		String connectionId = "";
		if (node instanceof TableNode) {
			connectionId = ((TableNode) node).getConnectionId();
		} else if (node instanceof DatabaseNode) {
			connectionId = ((DatabaseNode) node).getConnectionId();
		}
		return connectionId;
	}
}
