package io.tapdata.flow.engine.V2.node.hazelcast.data.batch.utils;

import com.tapdata.constant.ConnectionUtil;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.pdk.apis.entity.Capability;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/8/11 11:03 Create
 * @description
 */
public final class CapabilityChecker {
    private static final String STREAM_READ_ONE_BY_ONE_FUNCTION = "stream_read_one_by_one_function";

    private CapabilityChecker() {

    }

    public static boolean hasAutoIncrementalBatchSizeNode(TaskDto taskDto, Function<String, Connections> getConnection, ClientMongoOperator clientMongoOperator) {
        if (null == taskDto || null == taskDto.getDag()) {
            return false;
        }
        final com.tapdata.tm.commons.dag.DAG dag = taskDto.getDag();
        final List<Node> nodes = dag.getNodes();
        if (CollectionUtils.isEmpty(nodes)) {
            return false;
        }
        final List<Edge> edges = dag.getEdges();
        final Set<String> incomingNodeIds = CollectionUtils.isEmpty(edges)
                ? Collections.emptySet()
                : edges.stream()
                .filter(Objects::nonNull)
                .map(Edge::getTarget)
                .filter(StringUtils::isNotBlank)
                .collect(Collectors.toSet());
        final Set<String> outgoingNodeIds = CollectionUtils.isEmpty(edges)
                ? Collections.emptySet()
                : edges.stream()
                .filter(Objects::nonNull)
                .map(Edge::getSource)
                .filter(StringUtils::isNotBlank)
                .collect(Collectors.toSet());
        return nodes.stream()
                .filter(Objects::nonNull)
                .filter(node -> isAutoIncrementalBatchSizeCandidate(node, incomingNodeIds, outgoingNodeIds))
                .anyMatch(e -> CapabilityChecker.hasStreamReadOneByOneCapability(e, getConnection, clientMongoOperator));
    }

    static boolean isAutoIncrementalBatchSizeCandidate(Node<?> node, Set<String> incomingNodeIds, Set<String> outgoingNodeIds) {
        return node instanceof DataParentNode
                && StringUtils.isNotBlank(node.getId())
                && !incomingNodeIds.contains(node.getId())
                && outgoingNodeIds.contains(node.getId());
    }

    static boolean hasStreamReadOneByOneCapability(Node<?> node, Function<String, Connections> getConnection, ClientMongoOperator clientMongoOperator) {
        Connections connection = getConnection.apply(((DataParentNode<?>) node).getConnectionId());
        if (null == connection || !"pdk".equals(connection.getPdkType())) {
            return false;
        }
        if (hasStreamOneByOneCapability(connection.getCapabilities())) {
            return true;
        }
        if (StringUtils.isBlank(connection.getPdkHash())) {
            return false;
        }
        DatabaseTypeEnum.DatabaseType databaseType = ConnectionUtil.getDatabaseType(clientMongoOperator, connection.getPdkHash());
        return null != databaseType && hasStreamOneByOneCapability(databaseType.getCapabilities());
    }

    static boolean hasStreamOneByOneCapability(List<Capability> capabilities) {
        return CollectionUtils.isNotEmpty(capabilities)
                && capabilities.stream()
                .filter(Objects::nonNull)
                .map(Capability::getId)
                .anyMatch(STREAM_READ_ONE_BY_ONE_FUNCTION::equals);
    }
}
