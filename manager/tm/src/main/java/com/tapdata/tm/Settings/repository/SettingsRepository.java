package com.tapdata.tm.Settings.repository;

import com.tapdata.tm.Settings.entity.Settings;
import org.springframework.data.mongodb.repository.MongoRepository;


/**
 * 因为Settings 的表  _id类型为字符串，导致这个类不能继承BaseRepository，所以  直接继承MongoRepository
 */
public interface SettingsRepository extends MongoRepository<Settings,String> {
     Settings findByCategoryAndKey(String category,String key);
}
