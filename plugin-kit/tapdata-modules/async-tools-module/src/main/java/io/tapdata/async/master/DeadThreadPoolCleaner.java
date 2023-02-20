package io.tapdata.async.master;

import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.utils.Container;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author aplomb
 */
@Bean
public class DeadThreadPoolCleaner {
	private List<Container<String, ThreadPoolExecutor>> containerList = new CopyOnWriteArrayList<>();
	public void add(String asyncJobId, ThreadPoolExecutor threadPoolExecutor) {
//		Container
	}
}
