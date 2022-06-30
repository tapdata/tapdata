package com.tapdata.processor.dataflow.pb;

import com.github.os72.protobuf.dynamic.DynamicSchema;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.huawei.shade.com.alibaba.fastjson.JSON;
import com.tapdata.constant.MapUtilV2;
import com.tapdata.constant.UUIDGenerator;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.compress.utils.Lists;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

@Ignore
public class DynamicProtoUtilTest {

	@Test
	public void test() {
		byte[] bytes = new byte[0];
		System.out.println(bytes);
	}

	@Test
	public void longTest() {
		Long l = 11L;
		System.out.println(l.toString());
		Object a = l;
		System.out.println(a.toString());
	}

	@Test
	public void loadSchema() throws IOException, Descriptors.DescriptorValidationException {
		String pathStr = "src/main/test/com/tapdata/processor/dataflow/pb/Periodic.json";
		Path path = Paths.get(pathStr);
//    byte[] bytes = Files.readAllBytes(path);
		String jsonStr = Files.readAllLines(path, StandardCharsets.UTF_8).stream().collect(Collectors.joining());

		PbModel pbModel = JSON.parseObject(jsonStr, PbModel.class);

		long l1 = System.currentTimeMillis();
		DynamicSchema dynamicSchema = DynamicProtoUtil.generateSchema(pbModel);
		long l2 = System.currentTimeMillis();


		Map<String, String> mapping = new HashMap<>();
		mapping.put("Unit.targetsObject.density", "a.b.c.d");
		mapping.put("Unit.targetsObject.contentLength", "a.b.c.d");
		mapping.put("Unit.targetsObject.targetsObjectData.targetObjectNum", "a.b.c.e.f");
		mapping.put("Unit.targetsObject.targetsObjectData.targetObject.type", "a.b.c.e.f");
		mapping.put("Unit.targetsObject.targetsObjectData.targetObject.riskStatus", "a.b.c.e.f");
		mapping.put("Unit.targetsObject.targetsObjectData.targetObject.relativeLateralPosition", "a.b.c.e.f");
		mapping.put("Unit.targetsObject.targetsObjectData.targetObject.relativeLongitudinalPosition", "a.b.c.e.f");
		mapping.put("Unit.targetsObject.targetsObjectData.targetObject.relativeLateralVelocity", "a.b.c.e.f");
		mapping.put("Unit.targetsObject.targetsObjectData.targetObject.relativeLongitudinalVelocity", "a.b.c.e.f");
		mapping.put("Unit.targetsObject.targetsObjectData.targetObject.length", "a.b.c.e.f");
		mapping.put("Unit.targetsObject.targetsObjectData.targetObject.height", "a.b.c.e.f");
		mapping.put("Unit.targetsObject.targetsObjectData.targetObject.width", "a.b.c.e.f");

		mapping.put("Unit.position.density", "a.b.c.d");
		mapping.put("Unit.position.contentLength", "a.b.c.d");
		mapping.put("Unit.position.positionData.longitude", "a.b.c.e.f");
		mapping.put("Unit.position.positionData.latitude", "a.b.c.e.f");
		mapping.put("Unit.position.positionData.height", "a.b.c.e.f");
		mapping.put("Unit.position.positionData.validMark", "a.b.c.e.f");

		mapping.put("Unit.decision.density", "a.b.c.d");
		mapping.put("Unit.decision.contentLength", "a.b.c.d");
		mapping.put("Unit.decision.decisionData.gear", "a.b.c.e.f");
		mapping.put("Unit.decision.decisionData.acceleratorPedal", "a.b.c.e.f");
		mapping.put("Unit.decision.decisionData.brakePedal", "a.b.c.e.f");
		mapping.put("Unit.decision.decisionData.steeringAngle", "a.b.c.e.f");
		mapping.put("Unit.decision.decisionData.adReqGear", "a.b.c.e.f");
		mapping.put("Unit.decision.decisionData.adSysReqRelativeLateralVelocity", "a.b.c.e.f");
		mapping.put("Unit.decision.decisionData.adSysReqRelativeLongitudinalVelocity", "a.b.c.e.f");
		mapping.put("Unit.decision.decisionData.adSysReqSteeringAngle", "a.b.c.e.f");
		mapping.put("Unit.decision.decisionData.adSysReqSteeringTorque", "a.b.c.e.f");
		mapping.put("Unit.decision.decisionData.adSysReqLongitudinalMoment", "a.b.c.e.f");
		mapping.put("Unit.decision.decisionData.adSysReqFlashLampStatus", "a.b.c.e.f");
		mapping.put("Unit.decision.decisionData.adSysReqWiperStatus", "a.b.c.e.f");
		mapping.put("Unit.decision.decisionData.driverTakeOverAbility", "a.b.c.e.f");

		mapping.put("Unit.vehiclePerformance.density", "a.b.c.d");
		mapping.put("Unit.vehiclePerformance.contentLength", "a.b.c.d");
		mapping.put("Unit.vehiclePerformance.vehiclePerformanceData.instantaneousVelocity", "a.b.c.e.f");
		mapping.put("Unit.vehiclePerformance.vehiclePerformanceData.lateralAcceleration", "a.b.c.e.f");
		mapping.put("Unit.vehiclePerformance.vehiclePerformanceData.longitudinalAcceleration", "a.b.c.e.f");
		mapping.put("Unit.vehiclePerformance.vehiclePerformanceData.headingAngle", "a.b.c.e.f");
		mapping.put("Unit.vehiclePerformance.vehiclePerformanceData.yawRate", "a.b.c.e.f");
		mapping.put("Unit.vehiclePerformance.vehiclePerformanceData.rollSpeed", "a.b.c.e.f");
		mapping.put("Unit.vehiclePerformance.vehiclePerformanceData.pitchAngularVelocity", "a.b.c.e.f");

		mapping.put("Unit.sendingDataExternally.density", "a.b.c.d");
		mapping.put("Unit.sendingDataExternally.contentLength", "a.b.c.d");
		mapping.put("Unit.sendingDataExternally.sendingDataExternallyData.id", "a.b.c.e.f");

		mapping.put("Unit.roadInfo.density", "a.b.c.d");
		mapping.put("Unit.roadInfo.contentLength", "a.b.c.d");
		mapping.put("Unit.roadInfo.roadInfoData.trafficSigns", "a.b.c.e.f");
		mapping.put("Unit.roadInfo.roadInfoData.laneNumber", "a.b.c.e.f");
		mapping.put("Unit.roadInfo.roadInfoData.laneType", "a.b.c.e.f");
		mapping.put("Unit.roadInfo.roadInfoData.roadSpeedLimit", "a.b.c.e.f");
		mapping.put("Unit.roadInfo.roadInfoData.abnormalRoadConditions", "a.b.c.e.f");
		mapping.put("Unit.roadInfo.roadInfoData.trafficControlInfo", "a.b.c.e.f");
		mapping.put("Unit.roadInfo.roadInfoData.frontSignalSign", "a.b.c.e.f");

		mapping.put("Unit.environment.density", "a.b.c.d");
		mapping.put("Unit.environment.contentLength", "a.b.c.d");
		mapping.put("Unit.environment.environmentData.externalLightInfo", "a.b.c.e.f");
		mapping.put("Unit.environment.environmentData.weatherInfo", "a.b.c.e.f");
		mapping.put("Unit.environment.environmentData.externalTemperatureInfo", "a.b.c.e.f");
		mapping.put("Unit.environment.environmentData.externalHumidityInfo", "a.b.c.e.f");

		mapping.put("Unit.vehicleStatus.density", "a.b.c.d");
		mapping.put("Unit.vehicleStatus.contentLength", "a.b.c.d");
		mapping.put("Unit.vehicleStatus.vehicleStatusData.powerOnStatus", "a.b.c.e.f");
		mapping.put("Unit.vehicleStatus.vehicleStatusData.controlModel", "a.b.c.e.f");
		mapping.put("Unit.vehicleStatus.vehicleStatusData.dynamicModel", "a.b.c.e.f");
		mapping.put("Unit.vehicleStatus.vehicleStatusData.chargeStatus", "a.b.c.e.f");
		mapping.put("Unit.vehicleStatus.vehicleStatusData.gear", "a.b.c.e.f");
		mapping.put("Unit.vehicleStatus.vehicleStatusData.brakingStatus", "a.b.c.e.f");
		mapping.put("Unit.vehicleStatus.vehicleStatusData.lightSwitch", "a.b.c.e.f");
		mapping.put("Unit.vehicleStatus.vehicleStatusData.batterySoh", "a.b.c.e.f");
		mapping.put("Unit.vehicleStatus.vehicleStatusData.currentOilVolume", "a.b.c.e.f");
		mapping.put("Unit.vehicleStatus.vehicleStatusData.currentCapacity", "a.b.c.e.f");
		mapping.put("Unit.vehicleStatus.vehicleStatusData.accumulatedMileage", "a.b.c.e.f");
		mapping.put("Unit.vehicleStatus.vehicleStatusData.wiperStatus", "a.b.c.e.f");
		mapping.put("Unit.vehicleStatus.vehicleStatusData.networkShape", "a.b.c.e.f");
		mapping.put("Unit.vehicleStatus.vehicleStatusData.signalStrengthLevel", "a.b.c.e.f");
		mapping.put("Unit.vehicleStatus.vehicleStatusData.uplinkRate", "a.b.c.e.f");
		mapping.put("Unit.vehicleStatus.vehicleStatusData.downlinkRate", "a.b.c.e.f");
		mapping.put("Unit.vehicleStatus.vehicleStatusData.afs", "a.b.c.e.f");
		mapping.put("Unit.vehicleStatus.vehicleStatusData.esc", "a.b.c.e.f");
		mapping.put("Unit.vehicleStatus.vehicleStatusData.dcBusVoltage", "a.b.c.e.f");
		mapping.put("Unit.vehicleStatus.vehicleStatusData.igbtTemperature", "a.b.c.e.f");
		mapping.put("Unit.vehicleStatus.vehicleStatusData.threePhaseCurrent", "a.b.c.e.f");
		mapping.put("Unit.vehicleStatus.vehicleStatusData.coolantFlow", "a.b.c.e.f");
		mapping.put("Unit.vehicleStatus.vehicleStatusData.coolantTemperature", "a.b.c.e.f");
		mapping.put("Unit.vehicleStatus.vehicleStatusData.allChargeAndDischargeValue", "a.b.c.e.f");
		mapping.put("Unit.vehicleStatus.vehicleStatusData.thermalRunawayState", "a.b.c.e.f");
		mapping.put("Unit.vehicleStatus.vehicleStatusData.equalizingCellStatus", "a.b.c.e.f");
		mapping.put("Unit.vehicleStatus.vehicleStatusData.currentSignal", "a.b.c.e.f");
		mapping.put("Unit.vehicleStatus.vehicleStatusData.cellVoltageSignal", "a.b.c.e.f");
		mapping.put("Unit.vehicleStatus.vehicleStatusData.batteryTemperature", "a.b.c.e.f");

		mapping.put("Unit.personnel.density", "a.b.c.d");
		mapping.put("Unit.personnel.contentLength", "a.b.c.d");
		mapping.put("Unit.personnel.personnelData.seatBeltStatus", "a.b.c.e.f");
		mapping.put("Unit.personnel.personnelData.steeringWheelStatus", "a.b.c.e.f");
		mapping.put("Unit.personnel.personnelData.driverSeatStatus", "a.b.c.e.f");

		mapping.put("Unit.vehicleComponent.density", "a.b.c.d");
		mapping.put("Unit.vehicleComponent.contentLength", "a.b.c.d");
		mapping.put("Unit.vehicleComponent.vehicleComponentData.airbagStatus", "a.b.c.e.f");
		mapping.put("Unit.vehicleComponent.vehicleComponentData.gnssStatus", "a.b.c.e.f");
		mapping.put("Unit.vehicleComponent.vehicleComponentData.imuStatus", "a.b.c.e.f");
		mapping.put("Unit.vehicleComponent.vehicleComponentData.drivingAutomationSystemStatus", "a.b.c.e.f");
		mapping.put("Unit.vehicleComponent.vehicleComponentData.highPrecisionMapStatus", "a.b.c.e.f");
		mapping.put("Unit.vehicleComponent.vehicleComponentData.obuStatus", "a.b.c.e.f");
		mapping.put("Unit.vehicleComponent.vehicleComponentData.cameraStatus", "a.b.c.e.f");
		mapping.put("Unit.vehicleComponent.vehicleComponentData.lidarStatus", "a.b.c.e.f");
		mapping.put("Unit.vehicleComponent.vehicleComponentData.ultrasonicRadarStatus", "a.b.c.e.f");
		mapping.put("Unit.vehicleComponent.vehicleComponentData.millimeterWaveRadarStatus", "a.b.c.e.f");
		mapping.put("Unit.vehicleComponent.vehicleComponentData.nightVisionSystemStatus", "a.b.c.e.f");

		Map<String, Object> dataMap = new HashMap<String, Object>() {{
			put("a", new HashMap<String, Object>() {{
				put("b", new HashMap<String, Object>() {{
					put("c", new HashMap<String, Object>() {{
						put("d", UUIDGenerator.uuid());
						put("e", new ArrayList<Object>() {{
							add(new HashMap<String, Object>() {{
								put("f", UUIDGenerator.uuid());
							}});
							add(new HashMap<String, Object>() {{
								put("f", UUIDGenerator.uuid());
							}});
							add(new HashMap<String, Object>() {{
								put("f", UUIDGenerator.uuid());
							}});
						}});
					}});
				}});
			}});
		}};
		System.out.println(MapUtilV2.getValueByKey(dataMap, "a.b.c.d"));
		System.out.println(MapUtilV2.getValueByKey(dataMap, "a.b.c.e.f"));

		long l3 = System.currentTimeMillis();
		PbConfiguration pbConfiguration = new PbConfiguration(dynamicSchema, DynamicProtoUtil.getFieldTypeMappingMap(pbModel));
		long l4 = System.currentTimeMillis();
		byte[] pbMsgByteArray = DynamicProtoUtil.getPbMsgByteArray(dataMap, pbConfiguration, mapping);
		long l5 = System.currentTimeMillis();

		System.out.println((l5 - l4) + "----" + (l4 - l3) + "----" + (l2 - l1));
		System.out.println(pbMsgByteArray);

//    Descriptors.Descriptor tarObjDes = dynamicSchema.getMessageDescriptor("Unit.TargetsObject");
//    Descriptors.Descriptor tarObjDataDes = dynamicSchema.getMessageDescriptor("Unit.TargetsObject.TargetsObjectData");
//    Descriptors.Descriptor tarObjDataTarDes = dynamicSchema.getMessageDescriptor("Unit.TargetsObject.TargetsObjectData.TargetObject");
//
//    DynamicMessage tarMsg = dynamicSchema.newMessageBuilder("Unit.TargetsObject")
//      .setField(tarObjDes.findFieldByName("density"), 10)
//      .setField(tarObjDes.findFieldByName("contentLength"), 1000)
//      .addRepeatedField(tarObjDes.findFieldByName("targetsObjectData"), dynamicSchema.newMessageBuilder("Unit.TargetsObject.TargetsObjectData")
//          .setField(tarObjDataDes.findFieldByName("targetObjectNum"), 2)
//          .addRepeatedField(tarObjDataDes.findFieldByName("targetObject"), dynamicSchema.newMessageBuilder("Unit.TargetsObject.TargetsObjectData.TargetObject")
//            .setField(tarObjDataTarDes.findFieldByName("type"), 1).setField(tarObjDataTarDes.findFieldByName("riskStatus"), 9)
//            .build())
//        .build())
//      .build();

		System.out.println(dynamicSchema.toString());
	}

}
