package io.tapdata.connector.kafka;

import com.alibaba.fastjson.JSON;
import io.tapdata.common.CommonDbConfig;
import io.tapdata.common.CommonDbTest;
import io.tapdata.connector.kafka.admin.DefaultAdmin;
import io.tapdata.connector.kafka.config.AdminConfiguration;
import io.tapdata.connector.kafka.config.KafkaConfig;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.entity.TestItem;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeAclsResult;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

import static io.tapdata.base.ConnectorBase.testItem;

public class KafkaTest extends CommonDbTest {

    private List<String> WITHOUT_READ_PRIVILEGE = new ArrayList<>();
    private List<String> WITHOUT_WRITE_PRIVILEGE = new ArrayList<>();

    private KafkaConfig kafkaConfig;
    private KafkaService kafkaService;
    private boolean isSchemaRegister;
    private KafkaSRService kafkaSRService;

    public KafkaTest(KafkaConfig kafkaConfig, Consumer<TestItem> consumer, KafkaService kafkaService, CommonDbConfig config, Boolean isSchemaRegister, KafkaSRService kafkaSRService) {
        super(config, consumer);
        this.kafkaConfig = kafkaConfig;
        this.kafkaService = kafkaService;
        this.isSchemaRegister = isSchemaRegister;
        this.kafkaSRService = kafkaSRService;
    }

    @Override
    public Boolean testHostPort() {
        TestItem testHostAndPort = kafkaService.testHostAndPort();
        consumer.accept(testHostAndPort);
        if (testHostAndPort.getResult() == TestItem.RESULT_FAILED) {
            return false;
        }
        return true;
    }

    @Override
    public Boolean testVersion() {
        return true;
    }

    @Override
    public Boolean testConnect() {
        TestItem testConnect = kafkaService.testConnect();
        consumer.accept(testConnect);
        if (this.isSchemaRegister) {
            TestItem testSRConnect = this.kafkaSRService.testConnect();
            consumer.accept(testSRConnect);
            if (testSRConnect.getResult() == TestItem.RESULT_FAILED) {
                return false;
            }
        }
        if (testConnect.getResult() == TestItem.RESULT_FAILED) {
            return false;
        }
        return true;
    }
    @Override
    public Boolean testWritePrivilege() {
        AdminConfiguration configuration = new AdminConfiguration(kafkaConfig, kafkaService.getConnectorId());
        try (
                DefaultAdmin defaultAdmin = new DefaultAdmin(configuration)
        ) {
            String user = kafkaConfig.getMqUsername();
            if (EmptyKit.isEmpty(user)) {
                consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_SUCCESSFULLY));
                return true;
            }
            AdminClient adminClient = defaultAdmin.getAdminClient();
            ResourcePatternFilter resourcePatternFilter = new ResourcePatternFilter(ResourceType.TOPIC, user, PatternType.ANY);
            AclBindingFilter ANY = new AclBindingFilter(resourcePatternFilter, AccessControlEntryFilter.ANY);
            DescribeAclsResult describeAclsResult = adminClient.describeAcls(ANY);
            Collection<AclBinding> aclBindings = describeAclsResult.values().get();
            if (aclBindings.isEmpty()) {
                consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_SUCCESSFULLY));
                return true;
            }
            for (AclBinding get : aclBindings) {
                if ("DENY".equalsIgnoreCase(get.entry().permissionType().toString())) {
                    if ("WRITE".equalsIgnoreCase(get.entry().operation().toString())) {
                        WITHOUT_WRITE_PRIVILEGE.add(get.pattern().name());
                    } else if ("READ".equalsIgnoreCase(get.entry().operation().toString())) {
                        WITHOUT_READ_PRIVILEGE.add(get.pattern().name());
                    }
                }
            }
        } catch (Exception e) {
            consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_FAILED, e.getMessage()));

        }
        if (WITHOUT_WRITE_PRIVILEGE.size() > 0) {
            consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_FAILED, JSON.toJSONString(WITHOUT_WRITE_PRIVILEGE)));
            return false;
        }
        consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_SUCCESSFULLY));
        return true;
    }

    public Boolean testReadPrivilege() {
        if (WITHOUT_WRITE_PRIVILEGE.size() > 0) {
            consumer.accept(testItem(TestItem.ITEM_READ, TestItem.RESULT_FAILED, JSON.toJSONString(WITHOUT_WRITE_PRIVILEGE)));
            return false;
        }
        consumer.accept(testItem(TestItem.ITEM_READ, TestItem.RESULT_SUCCESSFULLY));
        return true;
    }

    public Boolean testStreamRead() {
        consumer.accept(testItem(TestItem.ITEM_READ_LOG, TestItem.RESULT_SUCCESSFULLY));
        return true;
    }
}
