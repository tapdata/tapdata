package io.tapdata;

import com.tapdata.entity.TapdataEvent;

/**
 * @author samuel
 * @Description
 * @create 2024-11-26 10:48
 **/
public interface HazelcastTaskNodeOffer {
	boolean offer(TapdataEvent tapdataEvent) throws Exception;
}
