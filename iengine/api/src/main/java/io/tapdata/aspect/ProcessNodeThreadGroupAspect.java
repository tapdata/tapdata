package io.tapdata.aspect;

import com.tapdata.tm.commons.dag.Node;
import io.tapdata.aspect.utils.AspectUtils;
import io.tapdata.entity.aspect.AspectInterceptResult;
import org.apache.logging.log4j.Logger;

public class ProcessNodeThreadGroupAspect extends ThreadGroupAspect<ProcessNodeThreadGroupAspect> {
    public ProcessNodeThreadGroupAspect(Node<?> node, String associateId, ThreadGroup threadGroup) {
        super(node, associateId,threadGroup);
    }
    public static AspectInterceptResult execute(Node<?> node, String associateId, ThreadGroup threadGroup, Logger logger) {
        try {
            return AspectUtils.executeAspect(ProcessNodeThreadGroupAspect.class, () -> new ProcessNodeThreadGroupAspect(node, associateId, threadGroup));
        } catch (Exception e) {
            logger.warn(e);
            return null;
        }
    }
}
