package io.tapdata.sybase.cdc.dto.watch;

import io.tapdata.entity.error.CoreException;
import org.apache.commons.io.monitor.FileAlterationListener;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;

import java.io.File;

public class FileMonitor {

	private final FileAlterationMonitor monitor;

	public FileMonitor(long interval) {
		monitor = new FileAlterationMonitor(interval);
	}

	/**
	 * 给文件添加监听
	 *
	 * @param path     文件路径
	 * @param listener 文件监听器
	 */
	public void monitor(String path, FileAlterationListener listener) {
		FileAlterationObserver observer = new FileAlterationObserver(new File(path));
		observer.addListener(listener);
		monitor.addObserver(observer);
	}

	public void stop() {
		try {
			monitor.stop();
		}catch (Exception e){
			throw new CoreException("File monitor can not be stop, msg: " + e.getMessage());
		}
	}

	public void start() throws Exception {
		monitor.start();
	}
}
