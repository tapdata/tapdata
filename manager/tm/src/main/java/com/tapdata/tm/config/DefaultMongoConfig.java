package com.tapdata.tm.config;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.tapdata.tm.dag.convert.DagDeserializeConvert;
import com.tapdata.tm.dag.convert.DagSerializeConvert;
import com.tapdata.tm.monitor.service.MeasurementServiceV2;
import com.tapdata.tm.monitoringlogs.service.MonitoringLogsService;
import com.tapdata.tm.utils.SSLUtil;
import lombok.SneakyThrows;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.*;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.config.AbstractMongoClientConfiguration;
import org.springframework.data.mongodb.core.convert.*;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.gridfs.GridFsTemplate;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;

import java.util.ArrayList;
import java.util.List;

@Configuration
@EnableMongoRepositories(mongoTemplateRef = "mongoTemplate", basePackages = {"com.tapdata.tm"},
        excludeFilters = @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, value = {MonitoringLogsService.class, MeasurementServiceV2.class}))
public class DefaultMongoConfig extends AbstractMongoClientConfiguration {

    @Value("${spring.data.mongodb.default.uri}")
    private String uri;
    @Value("${spring.data.mongodb.ssl}")
    private boolean ssl;
    @Value("${spring.data.mongodb.caPath}")
    private String caPath;
    @Value("${spring.data.mongodb.keyPath}")
    private String keyPath;

    @Bean(name = "mongoCusConversions")
    @Primary
    public MongoCustomConversions mongoCustomConversions() {
        List<Object> converters = new ArrayList<>();
        converters.add(new DagSerializeConvert());
        converters.add(new DagDeserializeConvert());

        return new MongoCustomConversions(converters);
    }

    @SneakyThrows
    @Override
    protected MongoClientSettings mongoClientSettings() {
        return SSLUtil.mongoClientSettings(ssl, keyPath, caPath, uri);
    }

    @Override
    public MappingMongoConverter mappingMongoConverter(MongoDatabaseFactory databaseFactory,
                                                       @Qualifier("mongoCusConversions") MongoCustomConversions conversions,
                                                       MongoMappingContext mappingContext) {
        return super.mappingMongoConverter(databaseFactory, conversions, mappingContext);
    }

    @Bean
    public GridFsTemplate gridFsTemplate(MongoDatabaseFactory mongoDbFactory, MongoConverter converter) {
        return new GridFsTemplate(mongoDbFactory, converter);
    }

    @Override
    protected String getDatabaseName() {
        return getConnectionString().getDatabase();
    }

    protected ConnectionString getConnectionString() {
        return new ConnectionString(uri);
    }
}
