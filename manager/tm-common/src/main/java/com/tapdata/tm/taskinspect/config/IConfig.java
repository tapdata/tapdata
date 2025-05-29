package com.tapdata.tm.taskinspect.config;

import java.io.Serializable;

/**
 * 任务配置
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/3/13 17:42 Create
 */
public interface IConfig<T extends IConfig<T>> extends Serializable {

    T init(int depth);

    /**
     * 初始化给定的实例，如果实例为 null，则返回默认值
     *
     * @param <V> 泛型参数，表示实例和默认值的类型
     * @param ins 要初始化的实例
     * @param def 如果实例为 null 时返回的默认值
     * @return 如果 ins 不为 null，则返回 ins；否则返回 def
     */
    default <V> V init(V ins, V def) {
        // 检查 ins 是否为 null，如果为 null，则返回默认值 def
        if (null == ins) {
            return def;
        }
        // 如果 ins 不为 null，则直接返回 ins
        return ins;
    }

    /**
     * 根据指定深度初始化配置对象
     * 此方法用于递归初始化配置对象，直到达到指定的深度depth
     * 如果深度为-1，表示无限深度，即完全初始化
     *
     * @param <V> 扩展了IConfig接口的配置对象类型
     * @param ins 配置对象实例，如果为null，将尝试创建一个新的实例
     * @param depth 初始化的深度，-1表示完全初始化，大于0时将递归初始化到指定深度
     * @param clz 配置对象的类，用于创建新的实例
     * @return 初始化后的配置对象实例
     * @throws RuntimeException 如果实例化配置对象时发生错误，将抛出运行时异常
     */
    default <V extends IConfig<V>> V init(V ins, int depth, Class<V> clz) {
        // 如果配置对象实例为null，尝试根据提供的类创建一个新的实例
        if (null == ins) {
            try {
                ins = clz.newInstance();
            } catch (InstantiationException | IllegalAccessException e) {
                // 如果实例化失败，抛出运行时异常
                throw new RuntimeException(e);
            }
        }
        // 根据深度进行初始化
        if (depth < 0) {
            // 如果深度为-1，表示需要完全初始化
            ins.init(depth);
        } else if (depth > 0) {
            // 如果深度大于0，递归初始化到指定深度
            ins.init(depth - 1);
        }
        // 返回初始化后的配置对象实例
        return ins;
    }
}
