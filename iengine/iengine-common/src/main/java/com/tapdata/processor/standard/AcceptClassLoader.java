package com.tapdata.processor.standard;

import com.tapdata.processor.ScriptUtil;

import javax.script.Invocable;

interface AcceptClassLoader {
	String before(ClassLoader[] externalClassLoader);

	Invocable after(String before, ClassLoader[] externalClassLoader);

	default Invocable doAccept() {
		ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
		try {
			ClassLoader[] externalClassLoader = new ClassLoader[1];
			String before = before(externalClassLoader);
			if (externalClassLoader[0] != null) {
				Thread.currentThread().setContextClassLoader(externalClassLoader[0]);
			}
			if (Thread.currentThread().getContextClassLoader() == null) {
				Thread.currentThread().setContextClassLoader(ScriptUtil.class.getClassLoader());
			}
			return after(before, externalClassLoader);
		} finally {
			Thread.currentThread().setContextClassLoader(contextClassLoader);
		}
	}
}