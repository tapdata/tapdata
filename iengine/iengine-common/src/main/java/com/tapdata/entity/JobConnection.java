package com.tapdata.entity;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.Serializable;

/**
 * @author jackin
 * @ClassName: JobConnection
 * @Description: 任务连接信息
 * @date 17-10-20
 * @since 1.0
 */
public class JobConnection implements Serializable {

	private static final long serialVersionUID = -3020014758262577210L;
	private String source;

	private String target;

	/**
	 * 标记目标是否为缓存节点
	 * 缓存节点在connections中不会有对应的连接信息，需要单独处理
	 */
	private boolean cacheTarget;

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public String getTarget() {
		return target;
	}

	public void setTarget(String target) {
		this.target = target;
	}

	public boolean getCacheTarget() {
		return cacheTarget;
	}

	public void setCacheTarget(boolean cacheTarget) {
		this.cacheTarget = cacheTarget;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;

		if (o == null || getClass() != o.getClass()) return false;

		JobConnection that = (JobConnection) o;

		return new EqualsBuilder()
				.append(getCacheTarget(), that.getCacheTarget())
				.append(getSource(), that.getSource())
				.append(getTarget(), that.getTarget())
				.isEquals();
	}

	@Override
	public int hashCode() {
		return new HashCodeBuilder(17, 37)
				.append(getSource())
				.append(getTarget())
				.append(getCacheTarget())
				.toHashCode();
	}

	@Override
	public String toString() {
		return "JobConnection{" +
				"source='" + source + '\'' +
				", target='" + target + '\'' +
				", cacheTarget=" + cacheTarget +
				'}';
	}
}
