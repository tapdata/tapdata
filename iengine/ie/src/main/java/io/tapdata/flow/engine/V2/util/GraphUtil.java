package io.tapdata.flow.engine.V2.util;

import com.tapdata.tm.commons.dag.Node;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/**
 * @author samuel
 * @Description
 * @create 2022-03-08 10:58
 **/
public class GraphUtil {
  public static List<Node<?>> successors(Node<?> node, Predicate<Node<?>> nodeFilter) {
    return successors(node, nodeFilter, null);
  }

  public static List<Node<?>> successors(Node<?> node, Predicate<Node<?>> nodeFilter, List<Node<?>> successors) {
    if (null == successors) {
      successors = new ArrayList<>();
    }
    if (null == node) {
      return successors;
    }
    List<? extends Node<?>> nodeSuccessors;
    if (CollectionUtils.isNotEmpty(successors)) {
      nodeSuccessors = successors;
    } else {
      nodeSuccessors = node.successors();
    }
    for (Node<?> nodeSuccessor : nodeSuccessors) {
      if (null == nodeFilter) {
        successors.add(nodeSuccessor);
      } else {
        if (nodeFilter.test(nodeSuccessor)) {
          successors.add(nodeSuccessor);
        } else {
          successors = successors(nodeSuccessor, nodeFilter, successors);
        }
      }
    }
    return successors;
  }

  public static List<Node<?>> predecessors(Node<?> node, Predicate<Node<?>> nodeFilter) {
    return predecessors(node, nodeFilter, null);
  }

  public static List<Node<?>> predecessors(Node<?> node, Predicate<Node<?>> nodeFilter, List<Node<?>> predecessors) {
    if (null == predecessors) {
      predecessors = new ArrayList<>();
    }
    if (null == node) {
      return predecessors;
    }
    List<? extends Node<?>> nodePredecessors;
    List<Node<?>> result = new ArrayList<>();
    if (CollectionUtils.isNotEmpty(predecessors)) {
      nodePredecessors = predecessors;
    } else {
      nodePredecessors = node.predecessors();
    }
    for (Node<?> nodePredecessor : nodePredecessors) {
      if (null == nodeFilter) {
        result.add(nodePredecessor);
      } else {
        if (nodeFilter.test(nodePredecessor)) {
          result.add(nodePredecessor);
        } else {
          result = predecessors(nodePredecessor, nodeFilter, result);
        }
      }
    }
    return result;
  }
}
