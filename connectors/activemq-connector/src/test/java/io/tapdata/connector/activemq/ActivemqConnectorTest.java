package io.tapdata.connector.activemq;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

class ActivemqConnectorTest {

    private ActivemqConnector activemqConnectorUnderTest;

    @BeforeEach
    void setUp() {
        activemqConnectorUnderTest = new ActivemqConnector();
    }

    @Test
    void testOnStart() throws Throwable {
        // Setup
        final TapNodeSpecification tapNodeSpecification = new TapNodeSpecification();
        tapNodeSpecification.setGroup("group");
        tapNodeSpecification.setName("name");
        tapNodeSpecification.setId("id");
        tapNodeSpecification.setVersion("version");
        tapNodeSpecification.setIcon("icon");
        tapNodeSpecification.setConfigOptions(new DataMap());
        tapNodeSpecification.setDataTypesMap(new DefaultExpressionMatchingMap(new HashMap<>()));
        final TapConnectionContext connectionContext = new TapConnectionContext(tapNodeSpecification, new DataMap());

        // Run the test
        activemqConnectorUnderTest.onStart(connectionContext);

        // Verify the results
    }

    @Test
    void testOnStart_ThrowsThrowable() {
        // Setup
        final TapNodeSpecification tapNodeSpecification = new TapNodeSpecification();
        tapNodeSpecification.setGroup("group");
        tapNodeSpecification.setName("name");
        tapNodeSpecification.setId("id");
        tapNodeSpecification.setVersion("version");
        tapNodeSpecification.setIcon("icon");
        tapNodeSpecification.setConfigOptions(new DataMap());
        tapNodeSpecification.setDataTypesMap(new DefaultExpressionMatchingMap(new HashMap<>()));
        final TapConnectionContext connectionContext = new TapConnectionContext(tapNodeSpecification, new DataMap());

        // Run the test
        assertThatThrownBy(() -> activemqConnectorUnderTest.onStart(connectionContext)).isInstanceOf(Throwable.class);
    }

    @Test
    void testOnStop() {
        // Setup
        final TapNodeSpecification tapNodeSpecification = new TapNodeSpecification();
        tapNodeSpecification.setGroup("group");
        tapNodeSpecification.setName("name");
        tapNodeSpecification.setId("id");
        tapNodeSpecification.setVersion("version");
        tapNodeSpecification.setIcon("icon");
        tapNodeSpecification.setConfigOptions(new DataMap());
        tapNodeSpecification.setDataTypesMap(new DefaultExpressionMatchingMap(new HashMap<>()));
        final TapConnectionContext connectionContext = new TapConnectionContext(tapNodeSpecification, new DataMap());

        // Run the test
        activemqConnectorUnderTest.onStop(connectionContext);

        // Verify the results
    }

    @Test
    void testRegisterCapabilities() {
        // Setup
        final ConnectorFunctions connectorFunctions = new ConnectorFunctions();
        final TapCodecsRegistry codecRegistry = new TapCodecsRegistry();

        // Run the test
        activemqConnectorUnderTest.registerCapabilities(connectorFunctions, codecRegistry);

        // Verify the results
    }

    @Test
    void testDiscoverSchema() throws Throwable {
        // Setup
        final TapNodeSpecification tapNodeSpecification = new TapNodeSpecification();
        tapNodeSpecification.setGroup("group");
        tapNodeSpecification.setName("name");
        tapNodeSpecification.setId("id");
        tapNodeSpecification.setVersion("version");
        tapNodeSpecification.setIcon("icon");
        tapNodeSpecification.setConfigOptions(new DataMap());
        tapNodeSpecification.setDataTypesMap(new DefaultExpressionMatchingMap(new HashMap<>()));
        final TapConnectionContext connectionContext = new TapConnectionContext(tapNodeSpecification, new DataMap());
        final Consumer<List<TapTable>> mockConsumer = mock(Consumer.class);

        // Run the test
        activemqConnectorUnderTest.discoverSchema(connectionContext, Arrays.asList("value"), 0, mockConsumer);

        // Verify the results
    }

    @Test
    void testDiscoverSchema_ThrowsThrowable() {
        // Setup
        final TapNodeSpecification tapNodeSpecification = new TapNodeSpecification();
        tapNodeSpecification.setGroup("group");
        tapNodeSpecification.setName("name");
        tapNodeSpecification.setId("id");
        tapNodeSpecification.setVersion("version");
        tapNodeSpecification.setIcon("icon");
        tapNodeSpecification.setConfigOptions(new DataMap());
        tapNodeSpecification.setDataTypesMap(new DefaultExpressionMatchingMap(new HashMap<>()));
        final TapConnectionContext connectionContext = new TapConnectionContext(tapNodeSpecification, new DataMap());
        final Consumer<List<TapTable>> mockConsumer = mock(Consumer.class);

        // Run the test
        assertThatThrownBy(() -> activemqConnectorUnderTest.discoverSchema(connectionContext, Arrays.asList("value"), 0,
                mockConsumer)).isInstanceOf(Throwable.class);
    }

    @Test
    void testConnectionTest() {
        // Setup
        final TapNodeSpecification tapNodeSpecification = new TapNodeSpecification();
        tapNodeSpecification.setGroup("group");
        tapNodeSpecification.setName("name");
        tapNodeSpecification.setId("id");
        tapNodeSpecification.setVersion("version");
        tapNodeSpecification.setIcon("icon");
        tapNodeSpecification.setConfigOptions(new DataMap());
        tapNodeSpecification.setDataTypesMap(new DefaultExpressionMatchingMap(new HashMap<>()));
        final TapConnectionContext connectionContext = new TapConnectionContext(tapNodeSpecification, new DataMap());
        final Consumer<TestItem> mockConsumer = mock(Consumer.class);

        // Run the test
        final ConnectionOptions result = activemqConnectorUnderTest.connectionTest(connectionContext, mockConsumer);

        // Verify the results
    }

    @Test
    void testTableCount() throws Throwable {
        // Setup
        final TapNodeSpecification tapNodeSpecification = new TapNodeSpecification();
        tapNodeSpecification.setGroup("group");
        tapNodeSpecification.setName("name");
        tapNodeSpecification.setId("id");
        tapNodeSpecification.setVersion("version");
        tapNodeSpecification.setIcon("icon");
        tapNodeSpecification.setConfigOptions(new DataMap());
        tapNodeSpecification.setDataTypesMap(new DefaultExpressionMatchingMap(new HashMap<>()));
        final TapConnectionContext connectionContext = new TapConnectionContext(tapNodeSpecification, new DataMap());

        // Run the test
        final int result = activemqConnectorUnderTest.tableCount(connectionContext);

        // Verify the results
        assertThat(result).isEqualTo(0);
    }

    @Test
    void testTableCount_ThrowsThrowable() {
        // Setup
        final TapNodeSpecification tapNodeSpecification = new TapNodeSpecification();
        tapNodeSpecification.setGroup("group");
        tapNodeSpecification.setName("name");
        tapNodeSpecification.setId("id");
        tapNodeSpecification.setVersion("version");
        tapNodeSpecification.setIcon("icon");
        tapNodeSpecification.setConfigOptions(new DataMap());
        tapNodeSpecification.setDataTypesMap(new DefaultExpressionMatchingMap(new HashMap<>()));
        final TapConnectionContext connectionContext = new TapConnectionContext(tapNodeSpecification, new DataMap());

        // Run the test
        assertThatThrownBy(() -> activemqConnectorUnderTest.tableCount(connectionContext))
                .isInstanceOf(Throwable.class);
    }
}
