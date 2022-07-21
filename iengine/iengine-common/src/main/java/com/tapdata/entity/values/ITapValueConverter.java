package com.tapdata.entity.values;

/**
 * @author Dexter
 */
public interface ITapValueConverter<T> {
	public T convert() throws Exception;
}
