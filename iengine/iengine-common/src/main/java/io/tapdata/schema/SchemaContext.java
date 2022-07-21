package io.tapdata.schema;

import com.tapdata.entity.Connections;
import com.tapdata.mongo.ClientMongoOperator;

import java.io.Serializable;

/**
 * @author samuel
 * @Description
 * @create 2021-11-11 12:06
 **/
public class SchemaContext implements Serializable {

	private static final long serialVersionUID = 6382236677320554054L;

	private ClientMongoOperator clientMongoOperator;
	private Connections connections;

	public SchemaContext(ClientMongoOperator clientMongoOperator) {
		this.clientMongoOperator = clientMongoOperator;
	}

	public SchemaContext(ClientMongoOperator clientMongoOperator, Connections connections) {
		this.clientMongoOperator = clientMongoOperator;
		this.connections = connections;
	}

	public ClientMongoOperator getClientMongoOperator() {
		return clientMongoOperator;
	}

	public Connections getConnections() {
		return connections;
	}

	void setClientMongoOperator(ClientMongoOperator clientMongoOperator) {
		this.clientMongoOperator = clientMongoOperator;
	}

	void setConnections(Connections connections) {
		this.connections = connections;
	}
}
