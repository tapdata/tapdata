package io.tapdata.flow.engine.V2.sharecdc.impl;

import io.tapdata.flow.engine.V2.sharecdc.ReaderType;
import io.tapdata.flow.engine.V2.sharecdc.ShareCdcContext;
import io.tapdata.flow.engine.V2.sharecdc.ShareCdcReader;
import io.tapdata.flow.engine.V2.sharecdc.exception.ShareCdcUnsupportedException;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * @author samuel
 * @Description
 * @create 2022-02-21 16:51
 **/
public class ShareCdcFactory {

	public static ShareCdcReader shareCdcReader(ReaderType readerType, ShareCdcContext shareCdcContext, Object... params)
			throws ShareCdcUnsupportedException, ClassNotFoundException, InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
		if (readerType == null) {
			readerType = ReaderType.PDK_TASK_HAZELCAST;
		}
		if (shareCdcContext == null) {
			throw new IllegalArgumentException("Share cdc context is null");
		}
		ShareCdcReader shareCdcReader;
		String clazz = readerType.getClazz();
		Constructor<?> declaredConstructor = Class.forName(clazz).getDeclaredConstructor(readerType.getClasses());
		declaredConstructor.setAccessible(true);
		Object instance = declaredConstructor.newInstance(params);
		if (instance instanceof ShareCdcReader) {
			shareCdcReader = (ShareCdcReader) instance;
		} else {
			throw new IllegalArgumentException("Implementation class must be ShareCdcReader: " + clazz);
		}
		shareCdcReader.init(shareCdcContext);
		return shareCdcReader;
	}
}
