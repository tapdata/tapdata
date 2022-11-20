package io.tapdata.modules.api.async.master;

/**
 * @author aplomb
 */
public class JobContext<T> {
	public static <T> JobContext<T> create(T result) {
		return new JobContext<T>(result);
	}

	public JobContext(T result) {
		this.result = result;
	}
	private String id;
	public JobContext<T> id(String id) {
		this.id = id;
		return this;
	}

	private T result;

	private String jumpToId;
	public JobContext<T> jumpToId(String jumpToId) {
		this.jumpToId = jumpToId;
		return this;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public T getResult() {
		return result;
	}

//	private void assignResultClass(Object result) {
//		if(result == null) {
//			resultClass = null;
//		} else {
//			resultClass = result.getClass();
//		}
//	}

	public String getJumpToId() {
		return jumpToId;
	}

	public void setJumpToId(String jumpToId) {
		this.jumpToId = jumpToId;
	}

}
