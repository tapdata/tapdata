package io.tapdata.flow.engine.V2.node.duckdb.converter;

/**
 * 统一的 Schema 转换器接口
 * 
 * @param <S> 源类型
 * @param <T> 目标类型
 */
public interface SchemaConverter<S, T> {
    
    /**
     * 将源对象转换为目标对象
     * 
     * @param source 源对象
     * @return 目标对象
     */
    T convert(S source);
    
    /**
     * 检查源对象是否已缓存
     * 
     * @param source 源对象
     * @return 是否已缓存
     */
    boolean isCached(S source);
    
    /**
     * 将转换结果缓存
     * 
     * @param source 源对象
     * @param target 目标对象
     */
    void cache(S source, T target);
    
    /**
     * 清空缓存
     */
    void clearCache();
}
