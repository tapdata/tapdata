package io.tapdata.flow.engine.V2.util;

import com.tapdata.tm.commons.dag.process.MergeTableNode;
import io.tapdata.flow.engine.V2.node.hazelcast.HazelcastBaseNode;
import io.tapdata.flow.engine.V2.node.hazelcast.data.HazelcastTaskSourceAndTarget;
import io.tapdata.flow.engine.V2.node.hazelcast.data.HazelcastTaskTarget;
import org.apache.commons.collections4.MapUtils;

import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2022-03-05 00:03
 **/

public class MergeTableUtil {

	public static void setMergeTableIntoHZTarget(Map<String, MergeTableNode> mergeTableMap, HazelcastBaseNode targetNode) {
		if (MapUtils.isEmpty(mergeTableMap)) {
			return;
		}
		HazelcastTaskTarget hazelcastTaskTarget = null;
		if (targetNode instanceof HazelcastTaskTarget) {
			hazelcastTaskTarget = (HazelcastTaskTarget) targetNode;
		} else if (targetNode instanceof HazelcastTaskSourceAndTarget) {
			hazelcastTaskTarget = ((HazelcastTaskSourceAndTarget) targetNode).getTarget();
		}
		if (null == hazelcastTaskTarget) {
			return;
		}
		MergeTableNode mergeTableNode = mergeTableMap.getOrDefault(hazelcastTaskTarget.getNode().getId(), null);
		if (null != mergeTableNode) {
			hazelcastTaskTarget.setMergeTableNode(mergeTableNode);
		}
	}
}
