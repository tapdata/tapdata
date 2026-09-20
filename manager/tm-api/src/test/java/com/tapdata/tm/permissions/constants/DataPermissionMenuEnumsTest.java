package com.tapdata.tm.permissions.constants;

import com.tapdata.tm.commons.task.dto.TaskDto;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class DataPermissionMenuEnumsTest {
    @Test
    void testApiClient() {
        Assertions.assertEquals("v2_api-client_all_data", DataPermissionMenuEnums.ApiClient.getAllDataPermissionName(DataPermissionActionEnums.View.name()));
        Assertions.assertEquals("v2_api-client_all_data_Edit", DataPermissionMenuEnums.ApiClient.getAllDataPermissionName(DataPermissionActionEnums.Edit.name()));
        Assertions.assertEquals("v2_api-client_all_data_Delete", DataPermissionMenuEnums.ApiClient.getAllDataPermissionName(DataPermissionActionEnums.Delete.name()));
        Assertions.assertEquals(DataPermissionDataTypeEnums.Application, DataPermissionMenuEnums.ApiClient.getDataType());
    }

    @Test
    void testApiServers() {
        Assertions.assertEquals("v2_api-servers_all_data", DataPermissionMenuEnums.ApiServers.getAllDataPermissionName(DataPermissionActionEnums.View.name()));
        Assertions.assertEquals("v2_api-servers_all_data_Edit", DataPermissionMenuEnums.ApiServers.getAllDataPermissionName(DataPermissionActionEnums.Edit.name()));
        Assertions.assertEquals("v2_api-servers_all_data_Delete", DataPermissionMenuEnums.ApiServers.getAllDataPermissionName(DataPermissionActionEnums.Delete.name()));
        Assertions.assertEquals(DataPermissionDataTypeEnums.ApiServer, DataPermissionMenuEnums.ApiServers.getDataType());
    }

    @Test
    void testConnections() {
        Assertions.assertEquals("v2_datasource_all_data", DataPermissionMenuEnums.Connections.getAllDataPermissionName(DataPermissionActionEnums.View.name()));
        Assertions.assertEquals("v2_datasource_all_data_Edit", DataPermissionMenuEnums.Connections.getAllDataPermissionName(DataPermissionActionEnums.Edit.name()));
        Assertions.assertEquals("v2_datasource_all_data_Delete", DataPermissionMenuEnums.Connections.getAllDataPermissionName(DataPermissionActionEnums.Delete.name()));
        Assertions.assertEquals(DataPermissionDataTypeEnums.Connections, DataPermissionMenuEnums.Connections.getDataType());
    }

    @Test
    void testMigrateTask() {
        Assertions.assertEquals("v2_data_replication_all_data", DataPermissionMenuEnums.MigrateTack.getAllDataPermissionName(DataPermissionActionEnums.View.name()));
        Assertions.assertEquals("v2_data_replication_all_data_Edit", DataPermissionMenuEnums.MigrateTack.getAllDataPermissionName(DataPermissionActionEnums.Edit.name()));
        Assertions.assertEquals("v2_data_replication_all_data_Delete", DataPermissionMenuEnums.MigrateTack.getAllDataPermissionName(DataPermissionActionEnums.Delete.name()));
        Assertions.assertEquals("v2_data_replication_all_data_Reset", DataPermissionMenuEnums.MigrateTack.getAllDataPermissionName(DataPermissionActionEnums.Reset.name()));
        Assertions.assertEquals("v2_data_replication_all_data_Start", DataPermissionMenuEnums.MigrateTack.getAllDataPermissionName(DataPermissionActionEnums.Start.name()));
        Assertions.assertEquals("v2_data_replication_all_data_Stop", DataPermissionMenuEnums.MigrateTack.getAllDataPermissionName(DataPermissionActionEnums.Stop.name()));
        Assertions.assertEquals(DataPermissionDataTypeEnums.Task, DataPermissionMenuEnums.MigrateTack.getDataType());
    }

    @Test
    void testSyncTask() {
        Assertions.assertEquals("v2_data_flow_all_data", DataPermissionMenuEnums.SyncTack.getAllDataPermissionName(DataPermissionActionEnums.View.name()));
        Assertions.assertEquals("v2_data_flow_all_data_Edit", DataPermissionMenuEnums.SyncTack.getAllDataPermissionName(DataPermissionActionEnums.Edit.name()));
        Assertions.assertEquals("v2_data_flow_all_data_Delete", DataPermissionMenuEnums.SyncTack.getAllDataPermissionName(DataPermissionActionEnums.Delete.name()));
        Assertions.assertEquals("v2_data_flow_all_data_Reset", DataPermissionMenuEnums.SyncTack.getAllDataPermissionName(DataPermissionActionEnums.Reset.name()));
        Assertions.assertEquals("v2_data_flow_all_data_Start", DataPermissionMenuEnums.SyncTack.getAllDataPermissionName(DataPermissionActionEnums.Start.name()));
        Assertions.assertEquals("v2_data_flow_all_data_Stop", DataPermissionMenuEnums.SyncTack.getAllDataPermissionName(DataPermissionActionEnums.Stop.name()));
        Assertions.assertEquals(DataPermissionDataTypeEnums.Task, DataPermissionMenuEnums.SyncTack.getDataType());
    }

    @Test
    void testLogCollectorTask() {
        Assertions.assertEquals("v2_log_collector_all_data", DataPermissionMenuEnums.LogCollectorTack.getAllDataPermissionName(DataPermissionActionEnums.View.name()));
        Assertions.assertEquals("v2_log_collector_all_data_Edit", DataPermissionMenuEnums.LogCollectorTack.getAllDataPermissionName(DataPermissionActionEnums.Edit.name()));
        Assertions.assertEquals("v2_log_collector_all_data_Delete", DataPermissionMenuEnums.LogCollectorTack.getAllDataPermissionName(DataPermissionActionEnums.Delete.name()));
        Assertions.assertEquals("v2_log_collector_all_data_Reset", DataPermissionMenuEnums.LogCollectorTack.getAllDataPermissionName(DataPermissionActionEnums.Reset.name()));
        Assertions.assertEquals("v2_log_collector_all_data_Start", DataPermissionMenuEnums.LogCollectorTack.getAllDataPermissionName(DataPermissionActionEnums.Start.name()));
        Assertions.assertEquals("v2_log_collector_all_data_Stop", DataPermissionMenuEnums.LogCollectorTack.getAllDataPermissionName(DataPermissionActionEnums.Stop.name()));
        Assertions.assertEquals(DataPermissionDataTypeEnums.Task, DataPermissionMenuEnums.LogCollectorTack.getDataType());
    }

    @Test
    void testConnectionHeartbeatTask() {
        Assertions.assertEquals("v2_conn_heartbeat_all_data", DataPermissionMenuEnums.ConnHeartbeatTack.getAllDataPermissionName(DataPermissionActionEnums.View.name()));
        Assertions.assertEquals("v2_conn_heartbeat_all_data_Delete", DataPermissionMenuEnums.ConnHeartbeatTack.getAllDataPermissionName(DataPermissionActionEnums.Delete.name()));
        Assertions.assertEquals("v2_conn_heartbeat_all_data_Reset", DataPermissionMenuEnums.ConnHeartbeatTack.getAllDataPermissionName(DataPermissionActionEnums.Reset.name()));
        Assertions.assertEquals("v2_conn_heartbeat_all_data_Start", DataPermissionMenuEnums.ConnHeartbeatTack.getAllDataPermissionName(DataPermissionActionEnums.Start.name()));
        Assertions.assertEquals("v2_conn_heartbeat_all_data_Stop", DataPermissionMenuEnums.ConnHeartbeatTack.getAllDataPermissionName(DataPermissionActionEnums.Stop.name()));
        Assertions.assertNull(DataPermissionMenuEnums.ConnHeartbeatTack.getAllDataPermissionName(DataPermissionActionEnums.Edit.name()));
        Assertions.assertEquals(DataPermissionDataTypeEnums.Task, DataPermissionMenuEnums.ConnHeartbeatTack.getDataType());
        Assertions.assertEquals(DataPermissionMenuEnums.ConnHeartbeatTack, DataPermissionMenuEnums.ofTaskSyncType(TaskDto.SYNC_TYPE_CONN_HEARTBEAT));
    }

    @Test
    void testMemCacheTask() {
        Assertions.assertEquals("v2_shared_cache_all_data", DataPermissionMenuEnums.MemCacheTack.getAllDataPermissionName(DataPermissionActionEnums.View.name()));
        Assertions.assertEquals("v2_shared_cache_all_data_Edit", DataPermissionMenuEnums.MemCacheTack.getAllDataPermissionName(DataPermissionActionEnums.Edit.name()));
        Assertions.assertEquals("v2_shared_cache_all_data_Delete", DataPermissionMenuEnums.MemCacheTack.getAllDataPermissionName(DataPermissionActionEnums.Delete.name()));
        Assertions.assertEquals("v2_shared_cache_all_data_Reset", DataPermissionMenuEnums.MemCacheTack.getAllDataPermissionName(DataPermissionActionEnums.Reset.name()));
        Assertions.assertEquals("v2_shared_cache_all_data_Start", DataPermissionMenuEnums.MemCacheTack.getAllDataPermissionName(DataPermissionActionEnums.Start.name()));
        Assertions.assertEquals("v2_shared_cache_all_data_Stop", DataPermissionMenuEnums.MemCacheTack.getAllDataPermissionName(DataPermissionActionEnums.Stop.name()));
        Assertions.assertEquals(DataPermissionDataTypeEnums.Task, DataPermissionMenuEnums.MemCacheTack.getDataType());
    }

    @Test
    void testTaskRebalance() {
        Assertions.assertEquals("v2_task_rebalance", DataPermissionMenuEnums.TaskRebalance.getAllDataPermissionName(DataPermissionActionEnums.View.name()));
        Assertions.assertEquals("v2_task_rebalance_Edit", DataPermissionMenuEnums.TaskRebalance.getAllDataPermissionName(DataPermissionActionEnums.Edit.name()));
        Assertions.assertNull(DataPermissionMenuEnums.TaskRebalance.getAllDataPermissionName(DataPermissionActionEnums.Start.name()));
        Assertions.assertNull(DataPermissionMenuEnums.TaskRebalance.getAllDataPermissionName(DataPermissionActionEnums.Stop.name()));
        Assertions.assertEquals(DataPermissionDataTypeEnums.Task, DataPermissionMenuEnums.TaskRebalance.getDataType());
    }

    @Test
    void testModules() {
        Assertions.assertEquals("v2_data-server-list_all_data", DataPermissionMenuEnums.Modules.getAllDataPermissionName(DataPermissionActionEnums.View.name()));
        Assertions.assertEquals("v2_data-server-list_all_data_Edit", DataPermissionMenuEnums.Modules.getAllDataPermissionName(DataPermissionActionEnums.Edit.name()));
        Assertions.assertEquals("v2_data-server-list_all_data_Publish", DataPermissionMenuEnums.Modules.getAllDataPermissionName(DataPermissionActionEnums.Publish.name()));
        Assertions.assertEquals("v2_data-server-list_all_data_Delete", DataPermissionMenuEnums.Modules.getAllDataPermissionName(DataPermissionActionEnums.Delete.name()));
        Assertions.assertEquals("v2_data-server-list_all_data_Revoke", DataPermissionMenuEnums.Modules.getAllDataPermissionName(DataPermissionActionEnums.Revoke.name()));
        Assertions.assertEquals(DataPermissionDataTypeEnums.Modules, DataPermissionMenuEnums.Modules.getDataType());
    }

    @Test
    void testInspect() {
        Assertions.assertEquals("v2_data_check_all_data", DataPermissionMenuEnums.INSPECT_TACK.getAllDataPermissionName(DataPermissionActionEnums.View.name()));
        Assertions.assertEquals("v2_data_check_all_data_Edit", DataPermissionMenuEnums.INSPECT_TACK.getAllDataPermissionName(DataPermissionActionEnums.Edit.name()));
        Assertions.assertEquals("v2_data_check_all_data_Delete", DataPermissionMenuEnums.INSPECT_TACK.getAllDataPermissionName(DataPermissionActionEnums.Delete.name()));
        Assertions.assertEquals("v2_data_check_all_data_Start", DataPermissionMenuEnums.INSPECT_TACK.getAllDataPermissionName(DataPermissionActionEnums.Start.name()));
        Assertions.assertEquals("v2_data_check_all_data_Stop", DataPermissionMenuEnums.INSPECT_TACK.getAllDataPermissionName(DataPermissionActionEnums.Stop.name()));
        Assertions.assertNull(DataPermissionMenuEnums.INSPECT_TACK.getAllDataPermissionName(DataPermissionActionEnums.Reset.name()));
        Assertions.assertEquals(DataPermissionDataTypeEnums.INSPECT, DataPermissionMenuEnums.INSPECT_TACK.getDataType());
    }

    @Test
    void testProjectMenus() {
        Assertions.assertEquals("v2_project_management_all_data_Delete", DataPermissionMenuEnums.ProjectManagement.getAllDataPermissionName("Delete"));
        Assertions.assertEquals("v2_project_import_and_export_all_data", DataPermissionMenuEnums.ProjectImportAndExport.getAllDataPermissionName("View"));
        Assertions.assertNull(DataPermissionMenuEnums.ProjectImportAndExport.getAllDataPermissionName("Edit"));
    }

    @Test
    void testUserManagement() {
        Assertions.assertEquals("v2_user_management_menu_all_data", DataPermissionMenuEnums.UserManagement.getAllDataPermissionName("View"));
        Assertions.assertEquals("v2_user_management_menu_all_data_Edit", DataPermissionMenuEnums.UserManagement.getAllDataPermissionName("Edit"));
        Assertions.assertEquals("v2_user_management_menu_all_data_Delete", DataPermissionMenuEnums.UserManagement.getAllDataPermissionName("Delete"));
        Assertions.assertEquals(DataPermissionDataTypeEnums.User, DataPermissionMenuEnums.UserManagement.getDataType());
    }
}
