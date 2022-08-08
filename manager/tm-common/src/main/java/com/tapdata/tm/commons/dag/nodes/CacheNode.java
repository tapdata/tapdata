package com.tapdata.tm.commons.dag.nodes;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.EqField;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.NodeType;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.task.dto.JoinTable;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static com.tapdata.tm.commons.base.convert.ObjectIdDeserialize.toObjectId;


/**
 * 共享缓存node
 */
@NodeType("mem_cache")
@Getter
@ToString
@Setter
public class CacheNode extends Node<Object> {
    // 不能和task  外层的名称一样，所以叫cacheName
    private String cacheName;
    private List<String> fields;
    private Long ttl;
    private String cacheKeys;
    private Long maxRows;
    /** 最大缓存 单位M */
    private Integer maxMemory;


    public CacheNode() {
        super("mem_cache", NodeCatalog.memCache);
    }

    @Override
    public Object mergeSchema(List inputSchemas, Object o) {
        return null;
    }

    @Override
    protected Object loadSchema(List<String> includes) {
        return null;
    }

    @Override
    protected Object cloneSchema(Object o) {
        return null;
    }

    @Override
    protected Object saveSchema(Collection predecessors, String nodeId, Object schema, DAG.Options options) {
        return null;
    }
}
