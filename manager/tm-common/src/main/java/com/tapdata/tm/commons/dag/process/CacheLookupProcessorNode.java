package com.tapdata.tm.commons.dag.process;

import com.tapdata.tm.commons.dag.EqField;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.NodeType;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;


/**
 * @Author: Zed
 * @Date: 2021/11/5
 * @Description:
 */
@NodeType("cache_lookup_processor")
@Getter
@Setter
public class CacheLookupProcessorNode extends ProcessorNode {
    @EqField
    private String cacheId;
    @EqField
    private String joinKey;
    @EqField
    private List<JoinSettings> joinSettings;
    @EqField
    private String script;

    public CacheLookupProcessorNode() {
        super("cache_lookup_processor");
    }

    @Data
    public static class JoinSettings implements Serializable {
        private String cacheKey;
        private String sourceKey;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o instanceof CacheLookupProcessorNode) {
            Class className = CacheLookupProcessorNode.class;
            for (; className != Object.class; className = className.getSuperclass()) {
                java.lang.reflect.Field[] declaredFields = className.getDeclaredFields();
                for (java.lang.reflect.Field declaredField : declaredFields) {
                    EqField annotation = declaredField.getAnnotation(EqField.class);
                    if (annotation != null) {
                        try {
                            Object f2 = declaredField.get(o);
                            Object f1 = declaredField.get(this);
                            boolean b = Node.fieldEq(f1, f2);
                            if (!b) {
                                return false;
                            }
                        } catch (IllegalAccessException e) {
                        }
                    }
                }
            }
            return true;
        }
        return false;
    }

}
