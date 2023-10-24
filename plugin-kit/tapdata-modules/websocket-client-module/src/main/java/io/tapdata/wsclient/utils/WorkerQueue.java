package io.tapdata.wsclient.utils;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class WorkerQueue<T> implements Runnable {
//	private static final String TAG = "STQ";
	private ConcurrentLinkedQueue<T> queue;
	private ThreadPoolExecutor threadPool;
	private Handler<T> handler;
	private boolean shutDown = false;
	public static abstract class Handler<T> {
		private WorkerQueue<T> queue;
		public boolean validate() {return true;}
		public void emptyNow() {}
		/**
		 * return true to continue;
		 * return false to stop iterating.
		 *
		 * @param t
		 * @return
		 */
		public abstract boolean handle(T t);
		public void error(T t, Throwable e){}
		public void closed() {}
		public WorkerQueue<T> getQueue() {
			return queue;
		}
	}

	public static abstract class BulkHandler<T> extends Handler<T> {
		private int bulkSize = 100;
		private ArrayList<T> bulkBuf = new ArrayList<>();
		public BulkHandler() {
		}
		public BulkHandler(int bulkSize) {
			this.bulkSize = bulkSize;
		}
		@Override
		public final boolean handle(T t) {
			boolean bool = true;
			if(bulkBuf.size() < bulkSize) {
				bulkBuf.add(t);
			} else {
				bool = bulkHandle(bulkBuf);
				bulkBuf.clear();
			}
			return bool;
		}

		public abstract boolean bulkHandle(ArrayList<T> bulk);

		@Override
		public void emptyNow() {
			bulkHandle(bulkBuf);
			bulkBuf.clear();
		}
	}

	public WorkerQueue() {
		this(null, null, null);
	}

	public WorkerQueue(ConcurrentLinkedQueue<T> queue) {
		this(queue, null, null);
	}

	public WorkerQueue(ConcurrentLinkedQueue<T> queue, ThreadPoolExecutor threadPool) {
		this(queue, threadPool, null);
	}

	public WorkerQueue(ConcurrentLinkedQueue<T> queue, ThreadPoolExecutor threadPool, Handler<T> handler) {
		this.queue = queue;
		if(this.queue == null) {
			this.queue = new ConcurrentLinkedQueue<>();
		}
		this.threadPool = threadPool;
		if(this.threadPool == null) {
			this.threadPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(2);
		}
		if(handler != null) {
			this.handler = handler;
			this.handler.queue = this;
		}
	}
	
	private int[] onlineUserThreadLock = new int[0];
	private boolean userThreadIsWorking = false;
	private Long userThreadStartTakes = null;
	public void offerAndStart(T t) {
		queue.offer(t);
		start();
	}
	public void offer(T t) {
		queue.offer(t);
	}
	
	public void start() {
		if(handler == null)
			throw new NullPointerException("handler for SingleThreadQueue is a must. but is null");
		if(!shutDown && handler.validate()) {
			synchronized (onlineUserThreadLock) {
				if(!userThreadIsWorking) {
					userThreadIsWorking = true;
					userThreadStartTakes = System.currentTimeMillis();
					threadPool.execute(this);
				}
			}	
		}
	}

	public boolean isEmpty() {
		return queue.isEmpty();
	}
	
	@Override
	public void run() {
//		LoggerEx.info(TAG, "Prepared to receiving... takes " + (userThreadStartTakes != null ? System.currentTimeMillis() - userThreadStartTakes : -1) + " for " + getDescription());
		while(!shutDown && handler.validate()) {
			T t = queue.poll();
			try {
				if(t == null) {
					handler.emptyNow();
					break;
				}
				if(!handler.handle(t)) {
					shutDown = true;
					break;
				}
			} catch(Throwable e) {
				e.printStackTrace();
				try {
					handler.error(t, e);
				} catch (Throwable e2) {
				}
			}
		}
		if(!shutDown) {
			if(handler.validate()) {
				synchronized (onlineUserThreadLock) {
					userThreadIsWorking = false;
					if(!queue.isEmpty()) {
						synchronized (onlineUserThreadLock) {
							if(!userThreadIsWorking) {
								userThreadIsWorking = true;
								userThreadStartTakes = System.currentTimeMillis();
								threadPool.execute(this);
							}
						}	
					}
				} 
			}
		} else {
			try {
				handler.closed();
			} catch (Throwable e) {
			}
		}
	}

	public ConcurrentLinkedQueue<T> getQueue() {
		return queue;
	}

	public void setQueue(ConcurrentLinkedQueue<T> queue) {
		this.queue = queue;
	}

	public ThreadPoolExecutor getThreadPoolExecutor() {
		return threadPool;
	}

	public void setThreadPoolExecutor(ThreadPoolExecutor threadPool) {
		this.threadPool = threadPool;
	}

	public Handler<T> getHandler() {
		return handler;
	}

	public void setHandler(Handler<T> handler) {
		this.handler = handler;
		this.handler.queue = this;

	}
}
