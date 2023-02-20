package com.tapdata.tm.listener;

import com.tapdata.tm.Settings.entity.Settings;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.mp.entity.MpAccessToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.UncategorizedMongoDbException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.IndexOperations;
import org.springframework.data.mongodb.core.index.MongoPersistentEntityIndexResolver;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;

import java.util.Arrays;
import java.util.List;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2023/1/11 上午6:58
 */
public class StartupListener implements ApplicationListener<ApplicationStartedEvent> {

    private Logger log = LoggerFactory.getLogger(StartupListener.class);

    @Override
    public void onApplicationEvent(ApplicationStartedEvent event) {
        ConfigurableApplicationContext context = event.getApplicationContext();
        doInit(context);

        ensureIndex(context);
    }

    private void doInit(ConfigurableApplicationContext context) {
        DataSourceService dataSourceService = context.getBean(DataSourceService.class);
        dataSourceService.batchEncryptConfig();
    }

    private void ensureIndex(ApplicationContext context) {

        MongoTemplate mongoTemplate = context.getBean(MongoTemplate.class);
        MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext =
                mongoTemplate.getConverter().getMappingContext();
        // Scan all entity
        /*SimpleBeanDefinitionRegistry registry = new SimpleBeanDefinitionRegistry();
        ClassPathScanningCandidateComponentProvider provider = new ClassPathBeanDefinitionScanner(registry);
        provider.resetFilters(false);
        provider.addIncludeFilter(new AnnotationTypeFilter(Document.class, true, false));
        String _package = this.getClass().getPackage().getName();
        _package = _package.substring(0, _package.lastIndexOf('.'));
        Set<BeanDefinition> entities = provider.findCandidateComponents(_package);*/

        List<BeanDefinition> entities = Arrays.asList(
                BeanDefinitionBuilder.genericBeanDefinition(MpAccessToken.class).getBeanDefinition(),
                BeanDefinitionBuilder.genericBeanDefinition(Settings.class).getBeanDefinition()
        );
        entities.forEach((beanDefinition -> {
            try {
                Class<?> entity = Class.forName(beanDefinition.getBeanClassName());
                if (entity.isAnnotationPresent(Document.class)) {
                    IndexOperations indexOps = mongoTemplate.indexOps(entity);
                    MongoPersistentEntityIndexResolver resolver = new MongoPersistentEntityIndexResolver(mappingContext);
                    resolver.resolveIndexFor(entity).forEach(indexOps::ensureIndex);
                }
            } catch (ClassNotFoundException e) {
                log.error("Load entity for {} failed", beanDefinition.getBeanClassName(), e);
            } catch (UncategorizedMongoDbException e) {
                log.error("Apply index for {} failed", beanDefinition.getBeanClassName(), e);
            }
        }));

    }
}
