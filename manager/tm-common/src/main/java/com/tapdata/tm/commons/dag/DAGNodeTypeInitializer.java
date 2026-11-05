package com.tapdata.tm.commons.dag;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;

public class DAGNodeTypeInitializer implements ApplicationListener<ContextRefreshedEvent> {

    private static final Logger logger = LoggerFactory.getLogger(DAGNodeTypeInitializer.class);

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        logger.info("ContextRefreshedEvent received for context [{}], start initializing DAG node type mapping.", event.getApplicationContext().getDisplayName());
        try {
            DAG.goInit();
            logger.info("DAG node type mapping initialized successfully for context [{}], registered {} node types.",
                    event.getApplicationContext().getDisplayName(), DAG.nodeMapping.size());
        } catch (Exception e) {
            logger.error("Failed to initialize DAG node type mapping on ContextRefreshedEvent for context [{}], will retry on first node type access.",
                    event.getApplicationContext().getDisplayName(), e);
        }
    }
}
