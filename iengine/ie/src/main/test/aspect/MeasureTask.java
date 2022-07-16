package aspect;

import io.tapdata.entity.aspect.Aspect;
import io.tapdata.entity.aspect.AspectInterceptResult;
import io.tapdata.entity.simplify.pretty.ClassHandlers;
import io.tapdata.flow.engine.V2.aspect.ApplicationStartAspect;
import io.tapdata.flow.engine.V2.aspect.NodeCloseAspect;
import io.tapdata.flow.engine.V2.aspect.NodeInitAspect;
import io.tapdata.flow.engine.V2.aspect.StreamReadNodeAspect;
import io.tapdata.flow.engine.V2.aspect.task.AspectTask;
import io.tapdata.flow.engine.V2.aspect.task.AspectTaskSession;

import java.util.Arrays;
import java.util.List;

@AspectTaskSession
public class MeasureTask extends AspectTask {
	private final ClassHandlers observerClassHandlers = new ClassHandlers();
	@Override
	public void onStart() {
		//TaskStartAspect
		observerClassHandlers.register(NodeInitAspect.class, this::handleNodeInit);
		observerClassHandlers.register(NodeCloseAspect.class, this::handleNodeClose);
		observerClassHandlers.register(StreamReadNodeAspect.class, this::handleStreamReadNode);
	}

	private Void handleStreamReadNode(StreamReadNodeAspect streamReadNodeAspect) {
		return null;
	}

	private Void handleNodeClose(NodeCloseAspect nodeCloseAspect) {
		return null;
	}

	private Void handleNodeInit(NodeInitAspect nodeInitAspect) {
		return null;
	}

	@Override
	public void onStop() {
		//TaskStopAspect
	}

	@Override
	public List<Class<? extends Aspect>> observeAspects() {
		return Arrays.asList(NodeInitAspect.class, NodeCloseAspect.class, StreamReadNodeAspect.class, ApplicationStartAspect.class);
	}

	@Override
	public List<Class<? extends Aspect>> interceptAspects() {
		return null;
	}

	@Override
	public void onObserveAspect(Aspect aspect) {
		observerClassHandlers.handle(aspect);
	}


	@Override
	public AspectInterceptResult onInterceptAspect(Aspect aspect) {
		return null;
	}
}
