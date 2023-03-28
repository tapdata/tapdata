package io.tapdata.aspect.supervisor;

import com.tapdata.tm.commons.dag.Node;
import io.tapdata.aspect.utils.AspectUtils;
import io.tapdata.entity.aspect.AspectInterceptResult;
import org.apache.logging.log4j.Logger;

public class DataNodeThreadGroupAspect extends ThreadGroupAspect<DataNodeThreadGroupAspect> {

    public DataNodeThreadGroupAspect(Node<?> node, String associateId, ThreadGroup threadGroup) {
        super(node, associateId,threadGroup);
    }

    public static AspectInterceptResult execute(Node<?> node, String associateId, ThreadGroup threadGroup, Logger logger) {
        try {
            return AspectUtils.executeAspect(DataNodeThreadGroupAspect.class, () -> new DataNodeThreadGroupAspect(node,associateId, threadGroup));
        } catch (Exception e) {
            logger.warn(e);
            return null;
        }
    }
}
