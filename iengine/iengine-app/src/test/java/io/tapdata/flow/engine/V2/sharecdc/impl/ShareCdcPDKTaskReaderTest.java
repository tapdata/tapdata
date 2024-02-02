package io.tapdata.flow.engine.V2.sharecdc.impl;

import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.flow.engine.V2.sharecdc.ShareCdcContext;
import io.tapdata.flow.engine.V2.sharecdc.ShareCdcTaskPdkContext;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.pdk.core.api.ConnectorNode;
import org.bson.Document;
import org.junit.jupiter.api.*;
import org.mockito.MockedStatic;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class ShareCdcPDKTaskReaderTest {
    @Nested
    class TestTapEventWrapper{
        private Document document;
        private final String CLAIMID="CL_000000001";
        private final String POLICYID="PC_000000001";
        @BeforeEach
        void beoforeEach(){
            document = new Document();
            document.put("fromTable","CLAIM");
            document.put("timestamp",1705056957000L);
            document.put("before",new Object());
            String op="u";
            document.put("op",op);
            Document after = new Document();
            after.put("CLAIM_ID",CLAIMID);
            after.put("POLICY_ID",POLICYID);
            document.put("after",after);
        }
        @DisplayName("Test TapUpdateEvent removeField is not null")
        @Test
        void test1(){
            List<String> removeFields=new ArrayList<>();
            removeFields.add("age");
            document.put("removeFields",removeFields);
            try(MockedStatic<PdkUtil> pdkUtilMockedStatic = mockStatic(PdkUtil.class)){
                when(PdkUtil.decodeOffset(anyString(),any(ConnectorNode.class))).thenReturn("123");
                ShareCdcPDKTaskReader shareCdcPDKTaskReader = spy(new ShareCdcPDKTaskReader(new ConcurrentHashMap<>()));
                ShareCdcContext shareCdcContext = mock(ShareCdcTaskPdkContext.class);
                shareCdcPDKTaskReader.shareCdcContext=shareCdcContext;
                ShareCdcBaseReader.ShareCDCReaderEvent shareCDCReaderEvent = shareCdcPDKTaskReader.tapEventWrapper(document);
                TapUpdateRecordEvent tapUpdateRecordEvent = (TapUpdateRecordEvent) shareCDCReaderEvent.getTapEvent();
                assertEquals(CLAIMID,tapUpdateRecordEvent.getAfter().get("CLAIM_ID"));
                assertEquals(POLICYID,tapUpdateRecordEvent.getAfter().get("POLICY_ID"));
                List<String> removedFields = tapUpdateRecordEvent.getRemovedFields();
                assertEquals("age",removedFields.get(0));
            }
        }
        @DisplayName("Test TapUpdateEvent removeField is null")
        @Test
        void test2(){
            try(MockedStatic<PdkUtil> pdkUtilMockedStatic = mockStatic(PdkUtil.class)){
                when(PdkUtil.decodeOffset(anyString(),any(ConnectorNode.class))).thenReturn("123");
                ShareCdcPDKTaskReader shareCdcPDKTaskReader = spy(new ShareCdcPDKTaskReader(new ConcurrentHashMap<>()));
                ShareCdcContext shareCdcContext = mock(ShareCdcTaskPdkContext.class);
                shareCdcPDKTaskReader.shareCdcContext=shareCdcContext;
                ShareCdcBaseReader.ShareCDCReaderEvent shareCDCReaderEvent = shareCdcPDKTaskReader.tapEventWrapper(document);
                TapUpdateRecordEvent tapUpdateRecordEvent = (TapUpdateRecordEvent) shareCDCReaderEvent.getTapEvent();
                assertEquals(CLAIMID,tapUpdateRecordEvent.getAfter().get("CLAIM_ID"));
                assertEquals(POLICYID,tapUpdateRecordEvent.getAfter().get("POLICY_ID"));
                List<String> removedFields = tapUpdateRecordEvent.getRemovedFields();
                Assertions.assertNull(removedFields);
            }
        }
        @DisplayName("Test TapupdateEvent replaceEvent is true")
        @Test
        void test3(){
            document.put("isReplaceEvent",true);
            try(MockedStatic<PdkUtil> pdkUtilMockedStatic = mockStatic(PdkUtil.class)){
                when(PdkUtil.decodeOffset(anyString(),any(ConnectorNode.class))).thenReturn("123");
                ShareCdcPDKTaskReader shareCdcPDKTaskReader = spy(new ShareCdcPDKTaskReader(new ConcurrentHashMap<>()));
                ShareCdcContext shareCdcContext = mock(ShareCdcTaskPdkContext.class);
                shareCdcPDKTaskReader.shareCdcContext=shareCdcContext;
                ShareCdcBaseReader.ShareCDCReaderEvent shareCDCReaderEvent = shareCdcPDKTaskReader.tapEventWrapper(document);
                TapUpdateRecordEvent tapUpdateRecordEvent = (TapUpdateRecordEvent) shareCDCReaderEvent.getTapEvent();
                assertEquals(CLAIMID,tapUpdateRecordEvent.getAfter().get("CLAIM_ID"));
                assertEquals(POLICYID,tapUpdateRecordEvent.getAfter().get("POLICY_ID"));
                assertEquals(Boolean.TRUE,tapUpdateRecordEvent.getIsReplaceEvent());
            }
        }
        @DisplayName("Test TapupdateEvent replaceEvent is false")
        @Test
        void test4(){
            document.put("isReplaceEvent",false);
            try(MockedStatic<PdkUtil> pdkUtilMockedStatic = mockStatic(PdkUtil.class)){
                when(PdkUtil.decodeOffset(anyString(),any(ConnectorNode.class))).thenReturn("123");
                ShareCdcPDKTaskReader shareCdcPDKTaskReader = spy(new ShareCdcPDKTaskReader(new ConcurrentHashMap<>()));
                ShareCdcContext shareCdcContext = mock(ShareCdcTaskPdkContext.class);
                shareCdcPDKTaskReader.shareCdcContext=shareCdcContext;
                ShareCdcBaseReader.ShareCDCReaderEvent shareCDCReaderEvent = shareCdcPDKTaskReader.tapEventWrapper(document);
                TapUpdateRecordEvent tapUpdateRecordEvent = (TapUpdateRecordEvent) shareCDCReaderEvent.getTapEvent();
                assertEquals(CLAIMID,tapUpdateRecordEvent.getAfter().get("CLAIM_ID"));
                assertEquals(POLICYID,tapUpdateRecordEvent.getAfter().get("POLICY_ID"));
                assertEquals(Boolean.FALSE,tapUpdateRecordEvent.getIsReplaceEvent());
            }
        }
        @DisplayName("Test TapUpdateEvent replaceEvent is null")
        @Test
        void test5(){
            document.put("isReplaceEvent",null);
            try(MockedStatic<PdkUtil> pdkUtilMockedStatic = mockStatic(PdkUtil.class)){
                when(PdkUtil.decodeOffset(anyString(),any(ConnectorNode.class))).thenReturn("123");
                ShareCdcPDKTaskReader shareCdcPDKTaskReader = spy(new ShareCdcPDKTaskReader(new ConcurrentHashMap<>()));
                ShareCdcContext shareCdcContext = mock(ShareCdcTaskPdkContext.class);
                shareCdcPDKTaskReader.shareCdcContext=shareCdcContext;
                ShareCdcBaseReader.ShareCDCReaderEvent shareCDCReaderEvent = shareCdcPDKTaskReader.tapEventWrapper(document);
                TapUpdateRecordEvent tapUpdateRecordEvent = (TapUpdateRecordEvent) shareCDCReaderEvent.getTapEvent();
                assertEquals(CLAIMID,tapUpdateRecordEvent.getAfter().get("CLAIM_ID"));
                assertEquals(POLICYID,tapUpdateRecordEvent.getAfter().get("POLICY_ID"));
                assertEquals(Boolean.FALSE,tapUpdateRecordEvent.getIsReplaceEvent());
            }
        }
    }
}
