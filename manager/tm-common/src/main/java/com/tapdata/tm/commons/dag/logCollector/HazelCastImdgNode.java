package com.tapdata.tm.commons.dag.logCollector;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.NodeType;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.List;


/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2021/11/5 上午10:11
 * @description
 */
@NodeType("hazelcastIMDG")
@Getter
@Setter
@ToString(callSuper = true)
@Slf4j
public class HazelCastImdgNode extends Node<Object> {

    private String externaltype;
    /** 是否开启增量并发写入*/
    private Boolean cdcConcurrent;
    /** 增量写入线程数*/
    private Integer cdcConcurrentWriteNum;
    public HazelCastImdgNode() {
        super("hazelcastIMDG");
    }

    @Override
    public Object mergeSchema(List<Object> inputSchemas, Object schemas, DAG.Options options) {
        return null;
    }

    @Override
    protected Object loadSchema(List<String> includes) {
        return null;
    }

    @Override
    protected Object saveSchema(Collection<String> pre, String nodeId, Object schemaList, DAG.Options options) {
        return null;
    }

    @Override
    protected Object cloneSchema(Object schemas) {
        return null;
    }
}
