package com.tapdata.tm.config;

import com.tapdata.tm.dag.convert.DagDeserializeConvert;
import com.tapdata.tm.dag.convert.DagSerializeConvert;
import com.tapdata.tm.monitor.service.MeasurementServiceV2;
import com.tapdata.tm.monitoringlogs.service.MonitoringLogsService;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.*;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.data.mongodb.core.convert.*;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.gridfs.GridFsTemplate;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;

import java.util.ArrayList;
import java.util.List;

@Configuration
@EnableMongoRepositories(mongoTemplateRef = "mongoTemplate", basePackages = {"com.tapdata.tm"},
        excludeFilters = @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, value = {MonitoringLogsService.class, MeasurementServiceV2.class}))
public class DefaultMongoConfig {

    @Value("${spring.data.mongodb.default.uri}")
    private String uri;

    @Bean(name = "mongoCusConversions")
    @Primary
    public CustomConversions mongoCustomConversions() {
        List<Object> converters = new ArrayList<>();
        converters.add(new DagSerializeConvert());
        converters.add(new DagDeserializeConvert());

        return new MongoCustomConversions(converters);
    }

    @Primary
    @Bean(name = "mongoTemplate")
    public MongoTemplate mongoTemplate(MongoDatabaseFactory mongoDbFactory, MongoConverter converter) throws Exception {
        return new MongoTemplate(mongoDbFactory, converter);
    }

    @Bean
    public MongoDatabaseFactory mongoDbFactory() {
        return new SimpleMongoClientDatabaseFactory(uri);
    }

    @Bean
    public MappingMongoConverter mappingMongoConverter(MongoDatabaseFactory factory, BeanFactory beanFactory,
                                                       @Qualifier("mongoCusConversions") CustomConversions conversions) {
        DbRefResolver dbRefResolver = new DefaultDbRefResolver(factory);
        MappingMongoConverter mappingConverter = new MappingMongoConverter(dbRefResolver, new MongoMappingContext());
        mappingConverter.setCustomConversions(conversions);
        return mappingConverter;
    }

    @Bean
    public GridFsTemplate gridFsTemplate(MongoDatabaseFactory mongoDbFactory, MongoConverter converter) {
        return new GridFsTemplate(mongoDbFactory, converter);
    }
}
