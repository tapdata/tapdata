
package com.tapdata.tm.commons.task.dto;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.tapdata.tm.base.convert.NodeDeserialize;
import com.tapdata.tm.base.convert.NodeSerialize;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Node;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Dag implements Serializable {

    private List<Edge> edges;
    @JsonSerialize( using = NodeSerialize.class)
    @JsonDeserialize( using = NodeDeserialize.class)
    private List<Node> nodes;
}
