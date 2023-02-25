package io.tapdata.flow.engine.V2.util;

import io.tapdata.entity.event.TapEvent;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * @author samuel
 * @Description
 * @create 2022-11-16 11:12
 **/
public class TargetTapEventFilter {
	private static final Logger logger = LogManager.getLogger(TargetTapEventFilter.class);
	private List<TapEventPredicate> predicates;

	public static TargetTapEventFilter create() {
		TargetTapEventFilter tapEventFilter = new TargetTapEventFilter();
		tapEventFilter.predicates = new ArrayList<>();
		return tapEventFilter;
	}

	public void addFilter(TapEventPredicate tapEventPredicate) {
		this.predicates.add(tapEventPredicate);
	}

	/**
	 * Test Tap Event should be filtered
	 *
	 * @param <E> {@link TapEvent}
	 * @return true: event should be ignored
	 * false: event should be process
	 */
	public <E extends TapEvent> boolean test(E tapEvent) {
		if (CollectionUtils.isEmpty(predicates)) {
			return false;
		}
		for (TapEventPredicate predicate : predicates) {
			if (predicate.test(tapEvent)) {
				predicate.failHandler(tapEvent);
				return true;
			}
		}
		return false;
	}

	public interface TapEventPredicate {
		<E extends TapEvent> boolean test(E tapEvent);

		default <E extends TapEvent> void failHandler(E tapEvent) {
			logger.warn("Tap event will be filter by {}\n{}", this.getClass().getName(), tapEvent);
		}
	}
}
