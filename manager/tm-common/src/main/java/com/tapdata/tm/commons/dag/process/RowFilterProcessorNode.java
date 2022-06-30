package com.tapdata.tm.commons.dag.process;

import com.tapdata.tm.commons.dag.EqField;
import com.tapdata.tm.commons.dag.NodeType;
import lombok.Getter;
import lombok.Setter;

/**
 * @Author: Zed
 * @Date: 2021/11/5
 * @Description:
 */
@NodeType("row_filter_processor")
@Getter
@Setter
public class RowFilterProcessorNode extends ProcessorNode {
    @EqField
    private String action;
    @EqField
    private String expression;

    public RowFilterProcessorNode() {
        super("row_filter_processor");
    }
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o instanceof RowFilterProcessorNode) {
            Class className = RowFilterProcessorNode.class;
            for (; className != Object.class; className = className.getSuperclass()) {
                java.lang.reflect.Field[] declaredFields = className.getDeclaredFields();
                for (java.lang.reflect.Field declaredField : declaredFields) {
                    EqField annotation = declaredField.getAnnotation(EqField.class);
                    if (annotation != null) {
                        try {
                            Object f2 = declaredField.get(o);
                            Object f1 = declaredField.get(this);
                            boolean b = fieldEq(f1, f2);
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
