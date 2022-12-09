package io.tapdata.pdk.core.connector;

import com.alibaba.fastjson.JSON;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.memory.MemoryFetcher;
import io.tapdata.pdk.core.tapnode.TapNodeClassFactory;
import io.tapdata.pdk.core.tapnode.TapNodeInstance;
import io.tapdata.pdk.core.utils.state.StateMachine;

import java.io.File;
import java.util.Collection;
import java.util.Map;

/**
 * Each jar will have a TapConnector.
 */
public class TapConnector implements MemoryFetcher {
    private static final String TAG = TapConnector.class.getSimpleName();

    private TapNodeClassFactory tapNodeClassFactory;

    private File jarFile;
    /**
     * jarFile.lastModified() will fetch actual file last modified time every time, not cached at all.
     * So we need cache the old file time to compare for changes.
     */
    private Long jarFileTime;
    private File loadingJarFile;

    public static final String STATE_NONE = "None";
    public static final String STATE_IDLE = "Idle";
    public static final String STATE_BEING_USED = "Being used";
    public static final String STATE_LOADING = "Loading";
    public static final String STATE_TERMINATED = "Terminated";
    private volatile StateMachine<String, TapConnector> stateMachine;

    private final Object lock = new int[0];

    public String toString() {
        return TapConnector.class.getSimpleName() + "#" + (stateMachine != null ? stateMachine.getCurrentState() : STATE_NONE) + " jarFile " + (jarFile != null ? jarFile.getName() : "none") + " jarFileTime " + jarFileTime;
    }

    public TapConnector() {
        tapNodeClassFactory = new TapNodeClassFactory();
    }

    public String getState() {
        if(stateMachine == null)
            return STATE_NONE;
        return stateMachine.getCurrentState();
    }

    public void start() {
        if(stateMachine == null) {
            synchronized (lock) {
                if(stateMachine == null) {
                    StateMachine<String, TapConnector> theStateMachine = new StateMachine<>(TapConnector.class.getSimpleName() + "#" + jarFile.getName(), STATE_NONE, this);
                    theStateMachine.configState(STATE_NONE, theStateMachine.execute().nextStates(STATE_LOADING, STATE_TERMINATED))
                            .configState(STATE_LOADING, theStateMachine.execute().nextStates(STATE_IDLE, STATE_TERMINATED))
                            .configState(STATE_IDLE, theStateMachine.execute().nextStates(STATE_LOADING, STATE_BEING_USED, STATE_TERMINATED))
                            .configState(STATE_BEING_USED, theStateMachine.execute().nextStates(STATE_IDLE, STATE_TERMINATED))
                            .configState(STATE_TERMINATED, theStateMachine.execute().nextStates(STATE_LOADING, STATE_NONE))
                            .errorOccurred(this::handleError);
                    this.stateMachine = theStateMachine;
                }
            }
        }
    }

    public void startLoadJar() {
        if(stateMachine.getCurrentState().equals(STATE_NONE)) {
            stateMachine.gotoState(STATE_LOADING, "Start loading jar " + jarFile.getAbsolutePath(), (tapConnector, stateMachine1) -> {
                this.loadingJarFile = jarFile;
            });
        }
    }

    private void handleError(Throwable throwable, String fromState, String toState, TapConnector tapConnector, StateMachine<String, TapConnector> stateMachine) {
        TapLogger.error(TAG, "State machine occurred error {} from state {} to state {}", throwable.getMessage(), fromState, toState);
    }

    private void checkUsedOrNot() {
        if(tapNodeClassFactory.isAssociateIdEmpty() && stateMachine.getCurrentState().equals(STATE_BEING_USED)) {
            stateMachine.gotoState(STATE_IDLE, "Jar " + jarFile + " is idle now");
        } else if(!tapNodeClassFactory.isAssociateIdEmpty() && stateMachine.getCurrentState().equals(STATE_IDLE)) {
            stateMachine.gotoState(STATE_BEING_USED, "Jar " + jarFile + " is using now");
        }
    }

    public boolean hasTapConnectorNodeId(String pdkId, String group, String version) {
        return getTapNodeClassFactory().hasTapConnectorNodeId(pdkId, group, version);
    }

    public boolean hasTapProcessorNodeId(String pdkId, String group, String version) {
        return getTapNodeClassFactory().hasTapProcessorNodeId(pdkId, group, version);
    }

    public TapNodeInstance createTapConnector(String associateId, String pdkId, String group, String version) {
        synchronized (stateMachine) {
            try {
                return tapNodeClassFactory.createTapConnector(associateId, pdkId, group, version);
            } finally {
                checkUsedOrNot();
            }
        }
    }

    public TapNodeInstance createTapProcessor(String associateId, String pdkId, String group, String version) {
        synchronized (stateMachine) {
            try {
                return tapNodeClassFactory.createTapProcessor(associateId, pdkId, group, version);
            } finally {
                checkUsedOrNot();
            }
        }
    }

    public String releaseAssociateId(String associateId) {
        if(tapNodeClassFactory.isAssociated(associateId)) {
            synchronized(stateMachine) {
                if(tapNodeClassFactory.isAssociated(associateId)) {
                    try {
                        return tapNodeClassFactory.releaseAssociateId(associateId);
                    } finally {
                        checkUsedOrNot();
                    }
                }
            }
        }
        return null;
    }

    public Collection<String> associatingIds() {
        return tapNodeClassFactory.associatingIds();
    }

    public void clearAssociateIds() {
        synchronized (stateMachine) {
            try {
                tapNodeClassFactory.clearAssociateIds();
            } finally {
                checkUsedOrNot();
            }
        }
    }

    public File getJarFile() {
        return jarFile;
    }

    public void setJarFile(File jarFile) {
        this.jarFile = jarFile;
        this.jarFileTime = jarFile.lastModified();
    }

    public Long getModificationTime() {
        if(jarFileTime == null)
            return null;
        return jarFileTime;
    }

    public TapNodeClassFactory getTapNodeClassFactory() {
        return tapNodeClassFactory;
    }

    public static void main(String... args) {
        String json = "{'a' : [1, 2, 3], 'b' : [{'a' : 1}], 'c' : {'a' : 'aa'}}";
        Map<String, Object> map = JSON.parseObject(json);
        System.out.println("map " + map);
    }

    void willUpdateJar(final File jarFile) {
        if(stateMachine.getCurrentState().equals(STATE_IDLE)) {
            stateMachine.gotoState(STATE_LOADING, "Will update jar " + jarFile.getAbsolutePath(), (tapConnector, stateMachine1) -> {
                this.loadingJarFile = jarFile;
            });
        }
    }

    void loadCompleted(File jarFile, ClassLoader classLoader, Throwable throwable) {
        if(stateMachine.getCurrentState().equals(STATE_LOADING)) {
            stateMachine.gotoState(STATE_IDLE, "Jar " + jarFile.getAbsolutePath() + " load " + (throwable == null ? "successfully" : "failed, " + throwable.getMessage()), (tapConnector, stateMachine) -> {
                if(throwable == null && this.loadingJarFile != null) {
                    setJarFile(this.loadingJarFile);
                    this.loadingJarFile = null;
                    tapNodeClassFactory.applyNewClassloader(classLoader);
                    return;
                }
                if(throwable != null) {
                    this.loadingJarFile = null;
                }
            });
        } else {
            TapLogger.error(TAG, "State is not loading, but {}, failed to execute load completed, this is a serious bug, please check it, jarFile {}, classloader {}, throwable {}", stateMachine.getCurrentState(), jarFile.getAbsolutePath(), classLoader, throwable);
        }
    }

    @Override
    public DataMap memory(String keyRegex, String memoryLevel) {
        return DataMap.create().keyRegex(keyRegex)/*.prefix(this.getClass().getSimpleName())*/
                .kv("jarFile", jarFile)
                .kv("jarFileTime", jarFileTime)
                .kv("loadingJarFile", loadingJarFile)
                .kv("state", stateMachine != null ? stateMachine.getCurrentState() : null)
                .kv("tapNodeClassFactory", tapNodeClassFactory.memory(keyRegex, memoryLevel));
    }
}
