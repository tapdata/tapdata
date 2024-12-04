package io.tapdata.observable.logging;

import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.entity.Setting;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.common.SettingService;
import io.tapdata.flow.engine.V2.entity.GlobalConstant;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import static org.mockito.Mockito.*;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2024/10/30 18:23
 */
public class ObsLoggerFactoryTest {

    @Test
    public void testGetObsLogger() {

        TaskDto taskDto = new TaskDto();
        taskDto.setId(new ObjectId());
        taskDto.setName("taskName");

        SettingService settingService = mock(SettingService.class);
        ClientMongoOperator clientOperator = mock(ClientMongoOperator.class);

        Setting setting = new Setting();
        setting.setValue("INFO");
        when(settingService.getSetting("logLevel")).thenReturn(setting);

        ConfigurationCenter configurationCenter = new ConfigurationCenter();
        configurationCenter.putConfig("workDir", "/tmp");
        GlobalConstant.getInstance().configurationCenter(configurationCenter);
        try (MockedStatic<BeanUtil> beanUtilMock = mockStatic(BeanUtil.class);
             MockedStatic<ObsLoggerFactory> obsLoggerFactoryMockedStatic = mockStatic(ObsLoggerFactory.class);) {

            beanUtilMock.when(() -> BeanUtil.getBean(SettingService.class)).thenReturn(settingService);
            beanUtilMock.when(() -> BeanUtil.getBean(ClientMongoOperator.class)).thenReturn(clientOperator);
            obsLoggerFactoryMockedStatic.when(()->{ObsLoggerFactory.getInstance();}).thenCallRealMethod();
            ObsLogger obsLogger = ObsLoggerFactory.getInstance().getObsLogger(taskDto, "nodeId", "nodeName");
            Assertions.assertNotNull(obsLogger);
            ObsLoggerFactory.getInstance().removeTaskLogger(taskDto);
        }

    }

}
