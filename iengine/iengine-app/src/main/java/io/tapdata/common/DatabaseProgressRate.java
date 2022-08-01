package io.tapdata.common;

import com.mongodb.MongoClient;
import com.tapdata.entity.Connections;
import com.tapdata.entity.Job;
import com.tapdata.entity.ProgressRateStats;
import io.tapdata.Target;

import java.util.List;

public interface DatabaseProgressRate {

	ProgressRateStats progressRateInfo(Job job, Connections sourceConn, Connections targetConn, MongoClient targetMongoClient, List<Target> targets);

	long getCdcLastTime(Connections sourceConn, Job job);
}
