package com.tapdata.tm.commons.dag.process;

import com.tapdata.tm.commons.dag.EqField;
import com.tapdata.tm.commons.dag.NodeType;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;


/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2021/11/5 上午10:22
 * @description
 */
@NodeType("field_calc_processor")
@Getter
@Setter
@Slf4j
public class FieldCalcProcessorNode extends FieldProcessorNode {

    public FieldCalcProcessorNode() {
        super("field_calc_processor");
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o instanceof FieldCalcProcessorNode) {
            Class className = FieldCalcProcessorNode.class;
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
