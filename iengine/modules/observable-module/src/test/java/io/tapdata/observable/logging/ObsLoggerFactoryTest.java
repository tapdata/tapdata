package io.tapdata.observable.logging;

import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.entity.Setting;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.common.SettingService;
import io.tapdata.flow.engine.V2.entity.GlobalConstant;
import io.tapdata.observable.logging.util.Conf.LogConfiguration;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2024/10/30 18:23
 */
public class ObsLoggerFactoryTest {

    private SettingService settingService;
    private ClientMongoOperator clientOperator;
    private TaskDto taskDto;

    @BeforeEach
    void beforeEach() {
        taskDto = new TaskDto();
        taskDto.setId(new ObjectId());
        taskDto.setName("taskName");

        settingService = mock(SettingService.class);
        clientOperator = mock(ClientMongoOperator.class);

        Setting setting = new Setting();
        setting.setValue("INFO");
        when(settingService.getSetting("logLevel")).thenReturn(setting);

        ConfigurationCenter configurationCenter = new ConfigurationCenter();
        configurationCenter.putConfig("workDir", "/tmp");
        GlobalConstant.getInstance().configurationCenter(configurationCenter);
    }

    @DisplayName("test Get Task Log Configuration ")
    @Test
    void test1(){
        ObsLoggerFactory obsLoggerFactory = mock(ObsLoggerFactory.class);
        SettingService settingService = mock(SettingService.class);
        ReflectionTestUtils.setField(obsLoggerFactory,"settingService",settingService);
        String prefix="task";
        when(settingService.getInt(prefix+"_log_file_save_time", 180)).thenReturn(180);
        when(settingService.getInt(prefix+"_log_file_save_size",1024)).thenReturn(1024);
        when(settingService.getInt(prefix + "_log_file_save_count", 100)).thenReturn(100);
        doCallRealMethod().when(obsLoggerFactory).getLogConfiguration(prefix);
        LogConfiguration logConfiguration = obsLoggerFactory.getLogConfiguration(prefix);
        assertEquals(180,logConfiguration.getLogSaveTime());
        assertEquals(1024,logConfiguration.getLogSaveSize());
        assertEquals(100,logConfiguration.getLogSaveCount());
    }
    @DisplayName("test Get null Log Configuration")
    @Test
    void test2(){
        ObsLoggerFactory obsLoggerFactory = mock(ObsLoggerFactory.class);
        SettingService settingService = mock(SettingService.class);
        ReflectionTestUtils.setField(obsLoggerFactory, "settingService", settingService);
        String prefix = null;
        when(settingService.getInt(prefix + "_log_file_save_time", 180)).thenReturn(180);
        when(settingService.getInt(prefix + "_log_file_save_size", 1024)).thenReturn(1024);
        when(settingService.getInt(prefix + "_log_file_save_count", 100)).thenReturn(100);
        doCallRealMethod().when(obsLoggerFactory).getLogConfiguration(prefix);
        LogConfiguration logConfiguration = obsLoggerFactory.getLogConfiguration(prefix);
        assertEquals(180, logConfiguration.getLogSaveTime());
        assertEquals(1024, logConfiguration.getLogSaveSize());
        assertEquals(100, logConfiguration.getLogSaveCount());
    }

    @Test
    public void testGetObsLogger() {

        try (MockedStatic<BeanUtil> beanUtilMock = mockStatic(BeanUtil.class)) {

            beanUtilMock.when(() -> BeanUtil.getBean(eq(SettingService.class))).thenReturn(settingService);
            beanUtilMock.when(() -> BeanUtil.getBean(eq(ClientMongoOperator.class))).thenReturn(clientOperator);

            ObsLogger obsLogger = ObsLoggerFactory.getInstance().getObsLogger(taskDto, "nodeId", "nodeName");
            Assertions.assertNotNull(obsLogger);
            ObsLoggerFactory.getInstance().removeTaskLogger(taskDto);
        } catch (Exception e) {
            e.printStackTrace(System.err);
        }

    }

    @Test
    void testOpenCatchData() {
        try (MockedStatic<BeanUtil> beanUtilMock = mockStatic(BeanUtil.class)) {

            beanUtilMock.when(() -> BeanUtil.getBean(eq(SettingService.class))).thenReturn(settingService);
            beanUtilMock.when(() -> BeanUtil.getBean(eq(ClientMongoOperator.class))).thenReturn(clientOperator);

            boolean result = ObsLoggerFactory.getInstance().openCatchData("taskId", 1L, 1L);
            Assertions.assertFalse(result);
            result = ObsLoggerFactory.getInstance().closeCatchData("taskId");
            Assertions.assertFalse(result);

            ObsLogger obsLogger = ObsLoggerFactory.getInstance().getObsLogger(taskDto, "nodeId", "nodeName");
            Assertions.assertNotNull(obsLogger);

            result = ObsLoggerFactory.getInstance().openCatchData(taskDto.getId().toHexString(), 1L, 1L);
            Assertions.assertTrue(result);

            result = ObsLoggerFactory.getInstance().closeCatchData(taskDto.getId().toHexString());
            Assertions.assertTrue(result);

        } catch (Exception e) {
            e.printStackTrace(System.err);
        }
    }

    @Test
    void testGetCatchDataStatus() {
        try (MockedStatic<BeanUtil> beanUtilMock = mockStatic(BeanUtil.class)) {

            beanUtilMock.when(() -> BeanUtil.getBean(eq(SettingService.class))).thenReturn(settingService);
            beanUtilMock.when(() -> BeanUtil.getBean(eq(ClientMongoOperator.class))).thenReturn(clientOperator);

            Map<String, Object> status = ObsLoggerFactory.getInstance().getCatchDataStatus("taskId");
            Assertions.assertNotNull(status);

            ObsLogger obsLogger = ObsLoggerFactory.getInstance().getObsLogger(taskDto, "nodeId", "nodeName");
            Assertions.assertNotNull(obsLogger);

            status = ObsLoggerFactory.getInstance().getCatchDataStatus("testId");
            Assertions.assertNotNull(status);
            Assertions.assertFalse(status.containsKey("taskLogger.enableDebugLogger"));

            status = ObsLoggerFactory.getInstance().getCatchDataStatus(taskDto.getId().toHexString());
            Assertions.assertNotNull(status);
            Assertions.assertTrue(status.containsKey("taskLogger.enableDebugLogger"));
            Assertions.assertFalse((Boolean) status.get("taskLogger.enableDebugLogger"));

            boolean result = ObsLoggerFactory.getInstance().openCatchData(taskDto.getId().toHexString(), 1L, 1L);
            Assertions.assertTrue(result);

            status = ObsLoggerFactory.getInstance().getCatchDataStatus(taskDto.getId().toHexString());
            Assertions.assertNotNull(status);
            Assertions.assertTrue(status.containsKey("taskLogger.enableDebugLogger"));
            Assertions.assertTrue((Boolean) status.get("taskLogger.enableDebugLogger"));

        } catch (Exception e) {
            e.printStackTrace(System.err);
        }
    }

    @Test
    void setOnFetchCacheData() {
        try (MockedStatic<BeanUtil> beanUtilMock = mockStatic(BeanUtil.class)) {

            beanUtilMock.when(() -> BeanUtil.getBean(eq(SettingService.class))).thenReturn(settingService);
            beanUtilMock.when(() -> BeanUtil.getBean(eq(ClientMongoOperator.class))).thenReturn(clientOperator);

            Assertions.assertDoesNotThrow(() -> {
                ObsLoggerFactory.getInstance().onFetchCacheData("test");
            });
            Map<String, TaskLogger> taskLoggersMap = (Map<String, TaskLogger>)
                    ReflectionTestUtils.getField(ObsLoggerFactory.getInstance(), "taskLoggersMap");

            TaskLogger taskLogger = mock(TaskLogger.class);
            taskLoggersMap.put("taskId", taskLogger);

            ObsLoggerFactory.getInstance().onFetchCacheData("taskId");
            verify(taskLogger, times(1)).setIntervalCeiling(any());
        }
    }
}
