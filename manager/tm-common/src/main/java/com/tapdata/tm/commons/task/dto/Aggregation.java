
package com.tapdata.tm.commons.task.dto;


import com.tapdata.tm.commons.dag.EqField;
import com.tapdata.tm.commons.dag.Node;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class Aggregation implements Serializable {

    @EqField
    private String aggExpression;

    @EqField
    private String aggFunction;

    @EqField
    private String filterPredicate;

    @EqField
    private List<String> groupByExpression;

    private String name;

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o instanceof Aggregation) {
            Class className = Aggregation.class;
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
