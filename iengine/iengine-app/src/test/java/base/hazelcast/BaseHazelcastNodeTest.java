package base.hazelcast;

import base.BaseTaskTest;
import com.hazelcast.jet.core.Processor;

import static org.mockito.Mockito.mock;

/**
 * @author samuel
 * @Description
 * @create 2023-11-22 11:46
 **/
public abstract class BaseHazelcastNodeTest extends BaseTaskTest {
	protected Processor.Context jetContext;

	@Override
	protected void allSetup() {
		super.allSetup();
		setupJetContext();
	}

	protected void setupJetContext() {
		jetContext = mock(Processor.Context.class);
	}
}
