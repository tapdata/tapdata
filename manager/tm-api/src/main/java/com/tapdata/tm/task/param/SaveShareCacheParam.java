package com.tapdata.tm.task.param;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Node;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class SaveShareCacheParam {
    private String name;
    private Map dag;

    private List edges;


}
