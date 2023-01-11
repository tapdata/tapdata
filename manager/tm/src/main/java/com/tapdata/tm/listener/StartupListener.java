package com.tapdata.tm.listener;

import com.tapdata.tm.ds.service.impl.DataSourceService;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2023/1/11 上午6:58
 */
public class StartupListener implements ApplicationListener<ApplicationStartedEvent> {
    @Override
    public void onApplicationEvent(ApplicationStartedEvent event) {
        ConfigurableApplicationContext context = event.getApplicationContext();
        doInit(context);
    }

    private void doInit(ConfigurableApplicationContext context) {
        DataSourceService dataSourceService = context.getBean(DataSourceService.class);
        dataSourceService.batchEncryptConfig();
    }
}
