package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import com.tapdata.entity.task.context.DataProcessorContext;

/**
 * Used to simulate the open partial parent method of the hazelcast source pdk base node
 *
 * <p>If you need mock supper method, sample:</p>
 * <ol>
 *   <li>
 *     <p>Override supper method 'isRunning' in this class:</p>
 *     <pre>{@code
 *     @Override
 *     public boolean isRunning() {
 *     	return super.isRunning();
 *     }
 *     }</pre>
 *   </li>
 *   <li>
 *     <p>Mock return value:</p>
 *     <pre>{@code
 *     MockHazelcastSourcePdkBaseNode mockPdkNode = Mockito.mock(MockHazelcastSourcePdkBaseNode.class);
 *     Mockito.when(mockPdkNode.isRunning()).thenReturn(false);
 *     }</pre>
 *   </li>
 * </ol>
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/11/24 10:21 Create
 */
public abstract class MockHazelcastSourcePdkBaseNode extends HazelcastSourcePdkBaseNode {
	public MockHazelcastSourcePdkBaseNode(DataProcessorContext dataProcessorContext) {
		super(dataProcessorContext);
	}

	@Override
	public boolean isRunning() {
		return super.isRunning();
	}

	@Override
	public boolean isJetJobRunning() {
		return super.isJetJobRunning();
	}
}
