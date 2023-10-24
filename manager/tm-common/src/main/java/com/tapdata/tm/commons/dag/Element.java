package com.tapdata.tm.commons.dag;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.github.openlg.graphlib.Graph;
import lombok.Data;
import org.bson.types.ObjectId;

import java.io.Serializable;
import java.util.Map;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2021/11/3 下午2:53
 * @description
 */
@Data
public abstract class Element implements Serializable {

    @EqField
    protected String id;
    private String name;

    private String desc;

    @JsonIgnore
    private final ElementType elementType;

    private Map<String, Object> attrs;

    @JsonIgnore
    private transient Graph<? extends Element, ? extends Element> graph;
    @JsonIgnore
    private transient DAG dag;

    private Element() {
        this.elementType = null;
    }

    public Element(ElementType elementType) {
        this.elementType = elementType;
    }

    public Element(String name, ElementType elementType) {
        this.name = name;
        this.elementType = elementType;
    }

    public Element(String name, ElementType elementType, Map<String, Object> attrs) {
        this.name = name;
        this.elementType = elementType;
        this.attrs = attrs;
    }

    @JsonIgnore
    public boolean isNode() {
        return elementType == ElementType.Node;
    }

    @JsonIgnore
    public boolean isLink() {
        return elementType == ElementType.Link;
    }

    public String ownerId() {
        return dag.getOwnerId();
    }
    public ObjectId taskId() {
        return dag.getTaskId();
    }

    @JsonIgnore
    public String getSyncType() {
        return dag.getSyncType();
    }

    public static enum ElementType {
        Node,
        Link
    }

}
