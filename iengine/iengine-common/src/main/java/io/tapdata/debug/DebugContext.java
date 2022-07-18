package io.tapdata.debug;

import com.tapdata.entity.Connections;
import com.tapdata.entity.Job;
import com.tapdata.mongo.ClientMongoOperator;

import java.io.Serializable;

public class DebugContext implements Serializable {

	private static final long serialVersionUID = 3333954172252628156L;

	private Job job;
	private ClientMongoOperator clientMongoOperator;
	private Connections sourceConn;
	private Connections targetConn;

	public DebugContext(Job job, ClientMongoOperator clientMongoOperator, Connections sourceConn, Connections targetConn) {
		this.job = job;
		this.clientMongoOperator = clientMongoOperator;
		this.sourceConn = sourceConn;
		this.targetConn = targetConn;
	}

	public Job getJob() {
		return job;
	}

	public void setJob(Job job) {
		this.job = job;
	}

	public ClientMongoOperator getClientMongoOperator() {
		return clientMongoOperator;
	}

	public void setClientMongoOperator(ClientMongoOperator clientMongoOperator) {
		this.clientMongoOperator = clientMongoOperator;
	}

	public Connections getSourceConn() {
		return sourceConn;
	}

	public void setSourceConn(Connections sourceConn) {
		this.sourceConn = sourceConn;
	}

	public Connections getTargetConn() {
		return targetConn;
	}

	public void setTargetConn(Connections targetConn) {
		this.targetConn = targetConn;
	}

	@Override
	public String toString() {
		return "DebugContext{" +
				"job=" + job.getId() +
				", clientMongoOperator=" + clientMongoOperator.getClass().getSimpleName() +
				", sourceConn=" + sourceConn.getId() +
				", targetConn=" + targetConn.getId() +
				'}';
	}
}
