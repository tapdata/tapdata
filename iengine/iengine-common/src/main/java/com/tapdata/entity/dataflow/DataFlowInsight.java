package com.tapdata.entity.dataflow;


import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.tapdata.constant.JSONUtil;

import java.util.HashMap;
import java.util.Map;

/**
 * @author jackin
 */
public class DataFlowInsight {

	private String id;

	private String statsType;

	@JsonIgnore
	private long createTime;

	private String dataFlowId;

	@JsonInclude(JsonInclude.Include.NON_NULL)
	private String statsTime;

	@JsonInclude(JsonInclude.Include.NON_NULL)
	private String granularity;

	private Object statsData;

	public DataFlowInsight() {
	}

	public DataFlowInsight(String statsType, long createTime, String statsTime, String dataFlowId, String granularity, Object statsData) {
		this.statsType = statsType;
		this.createTime = createTime;
		this.dataFlowId = dataFlowId;
		this.granularity = granularity;
		this.statsData = statsData;
		this.statsTime = statsTime;
	}

	public void convertStatsDataToEntity() {
		StatsTypeEnum statsTypeEnum = StatsTypeEnum.fromString(statsType);
		switch (statsTypeEnum) {
			case RUNTIME_STATS:
				if (statsData instanceof Map) {
					this.statsData = JSONUtil.map2POJO((Map) statsData, DataFlowStats.class);
				}
				break;
			default:
				break;
		}
	}
	//    public static List<DataFlowStats> initDataFlowStats(String dataFlowId, List<Stage> stages) {
//
//        List<DataFlowStats> dataFlowStatsList = new ArrayList<>();
//        for (StatsTypeEnum statsType : StatsTypeEnum.values()) {
//
//            if (statsType == StatsTypeEnum.DATA_OVERVIEW) {
//                DataFlowStats dataFlowStats = getDataFlowStats(statsType, Granularity.FLOW, dataFlowId, null);
//                dataFlowStatsList.add(dataFlowStats);
//            } else {
//                for (Granularity granularity : Granularity.values()) {
//                    if (granularity.value.startsWith("FLOW_")) {
//                        DataFlowStats dataFlowStats = getDataFlowStats(statsType, granularity, dataFlowId, null);
//                        dataFlowStatsList.add(dataFlowStats);
//                    }
//                }
//            }
//
//        }
//
//        for (Stage stage : stages) {
//            for (StatsTypeEnum statsType : StatsTypeEnum.values()) {
//                if (statsType == StatsTypeEnum.DATA_OVERVIEW) {
//                    DataFlowStats dataFlowStats = getDataFlowStats(statsType, Granularity.STAGE, dataFlowId, stage.getId());
//                    dataFlowStatsList.add(dataFlowStats);
//                } else {
//                    for (Granularity granularity : Granularity.values()) {
//                        if (granularity.value.startsWith("STAGE_")) {
//                            DataFlowStats dataFlowStats = getDataFlowStats(statsType, granularity, dataFlowId, stage.getId());
//                            dataFlowStatsList.add(dataFlowStats);
//                        }
//                    }
//                }
//            }
//
//        }
//
//        return dataFlowStatsList;
//    }
//
//    private static DataFlowStats getDataFlowStats(StatsTypeEnum statsType, Granularity granularity, String dataFlowId, String stageId) {
//        DataFlowStats dataFlowStats = new DataFlowStats();
//        dataFlowStats.setCreateTime(System.currentTimeMillis());
//        dataFlowStats.setDataFlowId(dataFlowId);
//        dataFlowStats.setStageId(stageId);
//        dataFlowStats.setStatsType(statsType.type);
//        dataFlowStats.setGranularity(granularity.value);
//
//        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//        if (statsType == StatsTypeEnum.THROUGHPUT) {
//            List<DataFlowStatsTPPoint> tpStatsPoints = new ArrayList<>();
//            for (int i = 0; i < 20; i++) {
//                DataFlowStatsTPPoint statsPoint = new DataFlowStatsTPPoint(sdf.format(new Date()), 0, 0, 0, 0);
//                tpStatsPoints.add(statsPoint);
//            }
//            dataFlowStats.setStatsData(tpStatsPoints);
//        } else if (statsType == StatsTypeEnum.DATA_OVERVIEW) {
//            DataFlowStatsTPPoint statsPoint = new DataFlowStatsTPPoint(sdf.format(new Date()), 0, 0, 0, 0);
//            dataFlowStats.setStatsData(statsPoint);
//        } else {
//
//            List<DataFlowStatsPoint> statsPoints = new ArrayList<>();
//            for (int i = 0; i < 20; i++) {
//                DataFlowStatsPoint statsPoint = new DataFlowStatsPoint(sdf.format(new Date()), 0);
//                statsPoints.add(statsPoint);
//            }
//
//            dataFlowStats.setStatsData(statsPoints);
//        }
//
//        return dataFlowStats;
//    }
//
//    public void adapteToDataFlow(List<DataFlowStats> dataFlowStats, Map<String, StageRuntimeStatsInfo> stageRuntimeStatsInfo, Map<String, StageRuntimeStatsInfo> previousStageRuntimeStats, List<Stage> stages) {
//        for (Stage stage : stages) {
//            String stageId = stage.getId();
//
//            StageRuntimeStatsInfo previousRuntimeStats = previousStageRuntimeStats.get(stageId);
//
//            StageRuntimeStatsInfo runtimeStatsInfo = stageRuntimeStatsInfo.get(stageId);
//            StageRuntimeThroughput input = runtimeStatsInfo.getInput();
//            StageRuntimeThroughput output = runtimeStatsInfo.getOutput();
//            long transmissionTime = runtimeStatsInfo.getTransmissionTime();
//            StageRuntimeReplicateLag replicationLag = runtimeStatsInfo.getReplicationLag();
//
//            for (DataFlowStats dataFlowStat : dataFlowStats) {
//                if (stageId.equals(dataFlowStat.getStageId())) {
//                    String statsType = dataFlowStat.getStatsType();
//                    StatsTypeEnum statsTypeEnum = StatsTypeEnum.fromString(statsType);
//                    switch (statsTypeEnum) {
//                        case THROUGHPUT:
//                            List<DataFlowStatsTPPoint> statsDataTP = (List<DataFlowStatsTPPoint>) dataFlowStat.getStatsData();
//                            DataFlowStatsTPPoint statsTPPoint = new DataFlowStatsTPPoint();
//
//                            break;
//                        case TRANS_TIME:
//                        case REPL_LAG:
//                            List<DataFlowStatsPoint> statsData = (List<DataFlowStatsPoint>) dataFlowStat.getStatsData();
//                        case DATA_OVERVIEW:
//                            DataFlowStatsTPPoint statsDataOV = (DataFlowStatsTPPoint) dataFlowStat.getStatsData();
//                            break;
//                        default:
//                            break;
//                    }
//
//                    break;
//                }
//            }
//        }
//    }
//
//    public void transferStatsDataToEntity() throws IOException {
//
//        StatsTypeEnum statsTypeEnum = StatsTypeEnum.fromString(statsType);
//        switch (statsTypeEnum) {
//            case THROUGHPUT:
//                if (statsData instanceof List) {
//                    statsData = JSONUtil.json2List(JSONUtil.obj2Json(statsData), DataFlowStatsTPPoint.class);
//                }
//                break;
//            case REPL_LAG:
//            case TRANS_TIME:
//
//                if (statsData instanceof List) {
//                    statsData = JSONUtil.json2List(JSONUtil.obj2Json(statsData), DataFlowStatsPoint.class);
//                }
//
//            case DATA_OVERVIEW:
//
//                if (statsData instanceof Map) {
//                    statsData = JSONUtil.map2POJO((Map) statsData, DataFlowStatsTPPoint.class);
//                }
//                break;
//            default:
//                break;
//        }
//    }

	public String getStatsType() {
		return statsType;
	}

	public void setStatsType(String statsType) {
		this.statsType = statsType;
	}

	public long getCreateTime() {
		return createTime;
	}

	public void setCreateTime(long createTime) {
		this.createTime = createTime;
	}

	public String getDataFlowId() {
		return dataFlowId;
	}

	public void setDataFlowId(String dataFlowId) {
		this.dataFlowId = dataFlowId;
	}

	public String getGranularity() {
		return granularity;
	}

	public void setGranularity(String granularity) {
		this.granularity = granularity;
	}

	public Object getStatsData() {
		return statsData;
	}

	public void setStatsData(Object statsData) {
		this.statsData = statsData;
	}

	public String getStatsTime() {
		return statsTime;
	}

	public void setStatsTime(String statsTime) {
		this.statsTime = statsTime;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public enum StatsTypeEnum {
		RUNTIME_STATS("runtime_stats"),
		DATAFLOW_DETAILS_STATS("dataFlowDetailsStats"),
		;

		public String type;

		StatsTypeEnum(String type) {
			this.type = type;
		}

		private static final Map<String, StatsTypeEnum> map = new HashMap<>();

		static {
			for (StatsTypeEnum statsTypeEnum : StatsTypeEnum.values()) {
				map.put(statsTypeEnum.type, statsTypeEnum);
			}
		}

		public static StatsTypeEnum fromString(String stageType) {
			return map.get(stageType);
		}

		public String getType() {
			return type;
		}
	}

	public enum Granularity {
		SECOND("second"),
		MINUTE("minute"),
		HOUR("hour"),
		DAY("day");

		public String value;

		Granularity(String value) {
			this.value = value;
		}
	}

//    public static void main(String[] args) throws JsonProcessingException {
//        DataFlowStats dataFlowStatsOV = getDataFlowStats(UuidUtil.getTimeBasedUuid().toString());
//
//        DataFlowStats dataFlowStatsTP = new DataFlowStats();
//        dataFlowStatsTP.setCreateTime(System.currentTimeMillis());
//        dataFlowStatsTP.setDataFlowId(UuidUtil.getTimeBasedUuid().toString());
//        dataFlowStatsTP.setStageId(UuidUtil.getTimeBasedUuid().toString());
//        dataFlowStatsTP.setStatsType(StatsType.THROUGHPUT.type);
//        dataFlowStatsTP.setGranularity(Granularity.FLOW_SECOND.value);
//
//        DataFlowStats dataFlowStatsTT = new DataFlowStats();
//        dataFlowStatsTT.setCreateTime(System.currentTimeMillis());
//        dataFlowStatsTT.setDataFlowId(UuidUtil.getTimeBasedUuid().toString());
//        dataFlowStatsTT.setStageId(UuidUtil.getTimeBasedUuid().toString());
//        dataFlowStatsTT.setStatsType(StatsType.TRANS_TIME.type);
//        dataFlowStatsTT.setGranularity(Granularity.FLOW_SECOND.value);
//
//        DataFlowStats dataFlowStatsRL = new DataFlowStats();
//        dataFlowStatsRL.setCreateTime(System.currentTimeMillis());
//        dataFlowStatsRL.setDataFlowId(UuidUtil.getTimeBasedUuid().toString());
//        dataFlowStatsRL.setStageId(UuidUtil.getTimeBasedUuid().toString());
//        dataFlowStatsRL.setStatsType(StatsType.REPL_LAG.type);
//        dataFlowStatsRL.setGranularity(Granularity.FLOW_SECOND.value);
//
//        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//        List<DataFlowStatsPoint> statsPoints = new ArrayList<>();
//        for (int i = 0; i < 20; i++) {
//            DataFlowStatsPoint statsPoint = new DataFlowStatsPoint(sdf.format(new Date()), RandomUtils.nextLong(0, 100000));
//            statsPoints.add(statsPoint);
//        }
//
//        List<DataFlowStatsTPPoint> tpStatsPoints = new ArrayList<>();
//        for (int i = 0; i < 20; i++) {
//            DataFlowStatsTPPoint statsPoint = new DataFlowStatsTPPoint(sdf.format(new Date()), RandomUtils.nextLong(0, 100000), RandomUtils.nextLong(0, 100000), RandomUtils.nextLong(0, 100000), RandomUtils.nextLong(0, 100000));
//            tpStatsPoints.add(statsPoint);
//        }
//
//        dataFlowStatsTP.setStatsData(tpStatsPoints);
//        dataFlowStatsTT.setStatsData(statsPoints);
//        dataFlowStatsRL.setStatsData(statsPoints);
//        dataFlowStatsOV.setStatsData(tpStatsPoints.get(0));
//
//        System.out.println(JSONUtil.obj2Json(dataFlowStatsTP));
//        System.out.println(JSONUtil.obj2Json(dataFlowStatsTT));
//        System.out.println(JSONUtil.obj2Json(dataFlowStatsRL));
//        System.out.println(JSONUtil.obj2Json(dataFlowStatsOV));
//
//    }
}
