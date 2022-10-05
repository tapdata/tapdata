package io.tapdata.wsclient.modules.imclient.impls;

import java.util.concurrent.ThreadFactory;

public class IMClientThreadFactory implements ThreadFactory {
	private final String name = "IMClient";

	@Override
	public Thread newThread(Runnable r) {
		Thread thread = new Thread(r);
		thread.setName(name + "-" + thread.getId());
		return thread;
	}
}
