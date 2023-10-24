package io.tapdata.entity.schema;

import java.io.Serializable;
import java.lang.reflect.Parameter;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.List;

public abstract class TapItem<T> implements Serializable {
    /**
     * 从上到下的层级
     * Database的下一级就是Table的列表
     * Table的下一级是Field的列表
     *
     * Table， Field都应该是
     */
//    private List<T> items;
    /**
     * 层级结构的名字， 类名， 自动赋值
     */
    private String itemName;

    /**
     * 子类扩展实现获取下一级列表， 直至找到TapField列表
     * @return
     */
    public Collection<T> childItems() {
        return null;
    }

    /**
     * 获得当前层级的泛型类名
     * 泛型类名为TapField的时候就说明已经找到字段列表了
     *
     * @return
     */
    public String itemName() {
        if(itemName == null) {
            synchronized (this) {
                if(itemName == null) {
                    Type type = this.getClass().getGenericSuperclass();
                    if(type instanceof ParameterizedType) {
                        ParameterizedType parameterizedType = (ParameterizedType) type;
                        Type[] types = parameterizedType.getActualTypeArguments();
                        if(types != null && types.length == 1) {
                            itemName = types[0].getTypeName();
                        }
                    }
                }
            }
        }
        return itemName;
    }

}
