package io.tapdata.sybase.cdc;

/**
 * @author GavinXiao
 * @description CdcStep create by Gavin
 * @create 2023/7/13 11:19
 **/
public interface CdcStep <T> {
    public T compile();
    public default boolean checkStep() {
        return true;
    }
}
