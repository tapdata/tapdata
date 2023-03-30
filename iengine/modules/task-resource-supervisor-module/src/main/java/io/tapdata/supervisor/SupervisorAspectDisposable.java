package io.tapdata.supervisor;

import io.tapdata.aspect.supervisor.DisposableThreadGroupAspect;
import io.tapdata.aspect.supervisor.entity.DisposableThreadGroupBase;
import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.aspect.AspectObserver;
import io.tapdata.entity.aspect.annotations.AspectObserverClass;

import java.util.Optional;

@AspectObserverClass(value = DisposableThreadGroupAspect.class, ignoreErrors = false, order = 2)
public class SupervisorAspectDisposable implements AspectObserver<DisposableThreadGroupAspect<?>> {
    private static final String TAG = SupervisorAspectTask.class.getSimpleName();
    @Bean
    TaskResourceSupervisorManager taskResourceSupervisorManager;

    public SupervisorAspectDisposable() {
    }

    private void handleDataStart(DisposableThreadGroupAspect<? extends DisposableThreadGroupBase> aspect) {
        Optional.ofNullable(aspect).flatMap(a -> Optional.ofNullable(aspect.getThreadGroup())).ifPresent(group -> {
            DisposableNodeInfo info = new DisposableNodeInfo();
            info.setNodeThreadGroup(group);
            info.setAspectConnector(this);
            info.setNodeMap(aspect.getEntity().summary());
            taskResourceSupervisorManager.addDisposableSubscribeInfo(group, info);
        });
    }
    private void handleDataStop(DisposableThreadGroupAspect<? extends DisposableThreadGroupBase> aspect) {
        Optional.ofNullable(aspect).flatMap(a -> Optional.ofNullable(aspect.getThreadGroup())).ifPresent(group -> {
            DisposableNodeInfo nodeInfo = taskResourceSupervisorManager.getDisposableSubscribeInfo(group);
            try {
                group.destroy();
                nodeInfo.setHasLaked(Boolean.FALSE);
                nodeInfo.setAspectConnector(null);
                nodeInfo.setNodeThreadGroup(null);
                nodeInfo.setNodeMap(null);
                taskResourceSupervisorManager.removeDisposableSubscribeInfo(group);
            } catch (Exception e) {
                nodeInfo.setHasLaked(Boolean.TRUE);
                //@todo 延时30s再destroy后统计节点上泄露的线程
            }
        });
    }

    @Override
    public void observe(DisposableThreadGroupAspect<?> aspect) {
        if (aspect.hasRelease()){
            this.handleDataStop(aspect);
        }else {
            this.handleDataStart(aspect);
        }
    }

//    @Override
//    public DataMap memory(String keyRegex, String memoryLevel) {
//        List<DataMap> connectors = new ArrayList<>();
//        threadGroupMap.forEach(((threadGroup, taskNodeInfo) -> {
//            connectors.add(taskNodeInfo.memory(keyRegex,memoryLevel));
//        }));
//        return super.memory(keyRegex, memoryLevel)
//                .kv("taskStartTime", startTime != null ? new Date(startTime) : null)
//                .kv("connectors", connectors);
//    }

}
