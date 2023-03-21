package io.tapdata.aspect.taskmilestones;

import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.task.dto.ParentTaskDto;
import io.tapdata.aspect.DataNodeAspect;
import io.tapdata.aspect.utils.AspectUtils;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/3/5 02:18 Create
 */
public class Snapshot2CDCAspect extends DataNodeAspect<Snapshot2CDCAspect> {

    private Boolean hasSnapshot;

    public Boolean getHasSnapshot() {
        return hasSnapshot;
    }

    public Snapshot2CDCAspect hasSnapshot(boolean hasSnapshot) {
        this.hasSnapshot = hasSnapshot;
        return this;
    }

    private Boolean hasCDC;

    public Boolean getHasCDC() {
        return hasCDC;
    }

    public Snapshot2CDCAspect hasCDC(boolean hasCDC) {
        this.hasCDC = hasCDC;
        return this;
    }

    public static void execute(DataProcessorContext dataProcessorContext) {
        String taskType = dataProcessorContext.getTaskDto().getType();
        AspectUtils.executeAspect(new Snapshot2CDCAspect()
                .dataProcessorContext(dataProcessorContext)
                .hasSnapshot(ParentTaskDto.TYPE_INITIAL_SYNC_CDC.equals(taskType) || ParentTaskDto.TYPE_INITIAL_SYNC.equals(taskType))
                .hasCDC(ParentTaskDto.TYPE_INITIAL_SYNC_CDC.equals(taskType) || ParentTaskDto.TYPE_CDC.equals(taskType))
        );
    }
}
