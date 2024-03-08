package io.tapdata.common;

import io.tapdata.utils.AppType;
import com.tapdata.entity.MessageEntity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

public class MessageConsumerTopLimitationManager {
	private static class SingletonHolder {
		private static final MessageConsumerTopLimitationManager INSTANCE = new MessageConsumerTopLimitationManager();
	}

	private MessageConsumerTopLimitationManager() {
	}

	public static final MessageConsumerTopLimitationManager getInstance() {
		return SingletonHolder.INSTANCE;
	}

	private static Class getClass(String classname) throws ClassNotFoundException {
		ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		if (classLoader == null) {
			classLoader = MessageConsumerTopLimitationManager.class.getClassLoader();
		}
		return (classLoader.loadClass(classname));
	}

	private Logger logger = LogManager.getLogger(getClass());
	private static final String CONSUMER_SPEED_TOTAL = "CONSUMER_SPEED_TOTAL";

	private final Lock lock = new ReentrantLock();
	private long counterIdx;
	private int counterPerSec;

	private Integer topLimitationTotal;

	private void init() {
		if (isInitialized()) {
			logger.debug("the singleton `MessageConsumerTopLimitationManager` has already been initialized, skip it...");
			return;
		}
		try {
			// set initial topLimitation to 0(no speed limit) for other kinds of products(DAAS, DFS)
			topLimitationTotal = 0;
			// read the limitation from env only for drs
			if (AppType.currentType().isDrs()) {
				String envVal = System.getenv(CONSUMER_SPEED_TOTAL);
				topLimitationTotal = Integer.parseInt(envVal.trim());
			}
		} catch (Exception e) {
			topLimitationTotal = 0;
		}
	}

	private boolean isInitialized() {
		return topLimitationTotal != null;
	}

	public boolean shouldLimit() {
		init();
		return topLimitationTotal != 0;
	}

	public void pushMessage(List<MessageEntity> msgs, Consumer<List<MessageEntity>> offerMessages) {
		init();
		lock.lock();
		try {
			List<MessageEntity> processMsgs = new ArrayList<>();
			for (MessageEntity msg : msgs) {
				long idx = System.currentTimeMillis() / 1000;
				// reset counter and offer the message
				if (idx != counterIdx) {
					offerMessages.accept(processMsgs);
					counterPerSec = 0;
					counterIdx = idx;
					processMsgs.clear();
				}
				if (counterPerSec >= topLimitationTotal) {
					offerMessages.accept(processMsgs);
					processMsgs.clear();
					logger.info("push speed is over the top limitation setting, stop pushing messages...");
					try {
						// sleep until next second
						Thread.sleep((counterIdx + 1) * 1000 - System.currentTimeMillis());
					} catch (InterruptedException ignore) {
					}
				}
				processMsgs.add(msg);
				counterPerSec += 1;
			}
			offerMessages.accept(processMsgs);
		} finally {
			lock.unlock();
		}
	}
}

