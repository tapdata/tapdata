package com.tapdata.entity.task.context;

import com.tapdata.entity.Connections;
import com.tapdata.tm.commons.dag.Node;

import java.io.Serializable;

/**
 * @author samuel
 * @Description
 * @create 2022-03-17 15:24
 **/
public class TypeConverterContext extends ProcessorBaseContext implements Serializable {

	private static final long serialVersionUID = -8603360683090494526L;

	private final Node<?> sourceNode;
	private final Node<?> targetNode;
	private final Connections sourceConn;
	private final Connections targetConn;

	private TypeConverterContext(TypeConverterContextBuilder builder) {
		super(builder);
		sourceNode = builder.sourceNode;
		targetNode = builder.targetNode;
		sourceConn = builder.sourceConn;
		targetConn = builder.targetConn;
	}

	public Node<?> getSourceNode() {
		return sourceNode;
	}

	public Node<?> getTargetNode() {
		return targetNode;
	}

	public Connections getSourceConn() {
		return sourceConn;
	}

	public Connections getTargetConn() {
		return targetConn;
	}

	public static TypeConverterContextBuilder newBuilder() {
		return new TypeConverterContextBuilder();
	}

	public static final class TypeConverterContextBuilder extends ProcessorBaseContextBuilder<TypeConverterContextBuilder> {
		private Node<?> sourceNode;
		private Node<?> targetNode;
		private Connections sourceConn;
		private Connections targetConn;

		public TypeConverterContextBuilder withSourceNode(Node<?> val) {
			sourceNode = val;
			return this;
		}

		public TypeConverterContextBuilder withTargetNode(Node<?> val) {
			targetNode = val;
			return this;
		}

		public TypeConverterContextBuilder withSourceConn(Connections val) {
			sourceConn = val;
			return this;
		}

		public TypeConverterContextBuilder withTargetConn(Connections val) {
			targetConn = val;
			return this;
		}

		public TypeConverterContext build() {
			return new TypeConverterContext(this);
		}
	}
}
