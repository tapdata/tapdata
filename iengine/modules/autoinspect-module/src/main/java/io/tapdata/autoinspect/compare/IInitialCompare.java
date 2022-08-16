package io.tapdata.autoinspect.compare;

import io.tapdata.autoinspect.connector.IConnector;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/8/14 17:37 Create
 */
public interface IInitialCompare {
    /**
     * @param sourceConnector
     * @param targetConnector
     * @param autoCompare
     * @throws Exception
     */
    void initialCompare(IConnector sourceConnector, IConnector targetConnector, IAutoCompare autoCompare) throws Exception;
}
