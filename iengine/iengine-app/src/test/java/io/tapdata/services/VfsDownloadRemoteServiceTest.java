package io.tapdata.services;

import org.junit.jupiter.api.BeforeEach;


import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 *
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0   Create
 */
class VfsDownloadRemoteServiceTest {
    private VfsDownloadRemoteService vfsDownloadRemoteService;
    private static final String TEST_INSPECT_ID = "test-inspect-id";
    private static final String TEST_INSPECT_RESULT_ID = "test-inspect-result-id";
    private static final String EXPORT_SQL = "exportSql";

    @BeforeEach
    void setUp() {
        vfsDownloadRemoteService = new VfsDownloadRemoteService();
    }

}
