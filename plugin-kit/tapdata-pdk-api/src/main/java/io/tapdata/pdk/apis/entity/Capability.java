package io.tapdata.pdk.apis.entity;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Capability implements Serializable {
    public static final int TYPE_DDL = 10;
    public static final int TYPE_FUNCTION = 11;
    public static final int TYPE_OTHER = 20;
    private Integer type;
    public Capability type(int type) {
        this.type = type;
        return this;
    }
    private String id;
    public Capability id(String id) {
        this.id = id;
        return this;
    }
    private List<String> alternatives;
    public Capability alternatives(List<String> alternatives) {
        this.alternatives = alternatives;
        return this;
    }

    public Capability() {
    }
    public Capability(String id) {
        this.id = id;
    }

    public static Capability create(String id) {
        return new Capability(id);
    }

    public Capability alternative(String alternative) {
        if(alternatives == null)
            alternatives = new ArrayList<>();
        alternatives.add(alternative);
        return this;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<String> getAlternatives() {
        return alternatives;
    }

    public void setAlternatives(List<String> alternatives) {
        this.alternatives = alternatives;
    }

    public Integer getType() {
        return type;
    }

    public void setType(Integer type) {
        this.type = type;
    }
}
