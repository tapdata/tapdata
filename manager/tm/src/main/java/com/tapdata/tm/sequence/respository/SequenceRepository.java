package com.tapdata.tm.sequence.respository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.sequence.entity.Sequence;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;

import java.util.Date;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2021/4/21 下午2:32
 * @description
 */
@Repository
@Slf4j
public class SequenceRepository extends BaseRepository<Sequence, ObjectId> {
	public SequenceRepository(MongoTemplate mongoOperations) {
		super(Sequence.class, mongoOperations);
	}

	/**
	 * 获取下一个序列值
	 * @param key
	 * @param begin
	 * @param stage
	 * @return
	 */
	public Sequence nextSeq(String key, int begin, int stage){
		return nextSeq(key, null, begin, stage);
	}

	/**
	 * 获取下一个序列值，可以指定过期时间，过期后将从头开始计数
	 * @param key
	 * @param expire
	 * @param begin
	 * @param stage
	 * @return
	 */
	public Sequence nextSeq(String key, Long expire, int begin, int stage){

		if (stage < 1) {
			log.warn("Sequence stage must be > 0");
			stage = 1;
		}

		Query query = Query.query(Criteria.where("field").is(key));

		Update update = new Update().setOnInsert("begin", begin).inc("seq", stage);
		if (expire != null) {
			update.setOnInsert("expire", new Date(System.currentTimeMillis() + expire));
		}

		FindAndModifyOptions options = FindAndModifyOptions.options().returnNew(true).upsert(true);
		Sequence sequence = getMongoOperations().findAndModify(query, update, options, Sequence.class);
		if (sequence != null){
			sequence.setSeq(sequence.getBegin() + sequence.getSeq());
		}
		return sequence;
	}

	/**
	 * 获取当前的 seq
	 * @param key
	 * @return
	 */
	public Sequence currentSeq(String key) {
		return getMongoOperations().findOne(Query.query(Criteria.where("field").is(key)), Sequence.class);
	}
}
