package io.tapdata.mongodb;

import com.mongodb.*;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.reflection.ClassAnnotationHandler;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.ReflectionUtil;
import io.tapdata.mongodb.annotation.MongoDAO;
import io.tapdata.mongodb.dao.AbstractMongoDAO;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;

import java.lang.annotation.Annotation;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Set;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

@Bean
public class MongoDAOAnnotationHandler extends ClassAnnotationHandler {
	private static final String TAG = MongoDAOAnnotationHandler.class.getSimpleName();

	@Bean
	private MongoClientFactory mongoClientFactory;


	@Override
	public void handle(Set<Class<?>> classes) throws CoreException {
		if (classes != null) {
			for(Class<?> clazz : classes) {
				MongoDAO mongoDocument = clazz.getAnnotation(MongoDAO.class);
				if (clazz.isAnnotationPresent(MongoDAO.class) && !clazz.isInterface() && !Modifier.isAbstract(clazz.getModifiers())) {
//                        AbstractObject<AbstractMongoDAO> beanObject = context.getAndCreateBean(SwitchCaseUtils.lowerFirstCase(clazz.getSimpleName()), clazz) as AbstractObject<AbstractMongoDAO>
//                        AbstractMongoDAO dao = beanObject.getObject(false)

					String collectionName = mongoDocument.collectionName();

					if(StringUtils.isEmpty(collectionName)) {
						collectionName = clazz.getSimpleName();
					}
					if (!AbstractMongoDAO.class.isAssignableFrom(clazz)) {
						TapLogger.warn(TAG, "MongoDAO annotation is found on class {}, but not extend AbstractMongoDAO which is a must. Ignore this class...", clazz);
						continue;
					}
					if (!ReflectionUtil.canBeInitiated(clazz)) {
						TapLogger.warn(TAG, "MongoDAO annotation is found on class {}, but not be initialized with empty parameter which is a must. Ignore this class...", clazz);
						continue;
					}
					String mongoUri = CommonUtils.getProperty("TAPDATA_MONGO_URI");
					//初始化mongoClient
					MongoClientHolder mongoClient = mongoClientFactory.getClient(mongoUri, mongoUri);
					ConnectionString connectionString = mongoClient.getConnectionString();
					String dbName = StringUtils.isEmpty(connectionString.getDatabase()) ? mongoDocument.dbName() : connectionString.getDatabase();
					if(StringUtils.isEmpty(dbName)) {
						TapLogger.warn(TAG, "MongoDAO annotation is found on class {}, but no dbName specified which is a must. Ignore this class... dbName {}", clazz, dbName);
						continue;
					}

					AbstractMongoDAO<?> dao = (AbstractMongoDAO<?>) InstanceFactory.bean(clazz, true);

//					AbstractMongoDAO dao = context.getAndCreateBean(SwitchCaseUtils.lowerFirstCase(clazz.getSimpleName()), clazz) as AbstractMongoDAO

					//MongoCollection
					CodecRegistry pojoCodecRegistry = fromRegistries(MongoClientSettings.getDefaultCodecRegistry(),
							fromProviders(PojoCodecProvider.builder().automatic(true).build()));
					MongoDatabase database = mongoClient.getMongoClient().getDatabase(dbName)
							.withCodecRegistry(pojoCodecRegistry)
							.withWriteConcern(WriteConcern.JOURNALED)//写策略
							.withReadConcern(ReadConcern.MAJORITY)//读策略：只能读到成功写入大多数节点的数据（所以有可能读到旧的数据）
							.withReadPreference(ReadPreference.nearest());//读选取节点策略：网络最近

					Class<?> domainClass = null;
					Type type = clazz.getGenericSuperclass();
					if (type instanceof ParameterizedType) {
						ParameterizedType parameterizedType = ((ParameterizedType) type);
						domainClass = (Class<?>) parameterizedType.getActualTypeArguments()[0];
					}

					if(domainClass == null) {
						TapLogger.warn(TAG, "Data class not found on class {}. Ignore this class...", clazz);
						continue;
					}
					MongoCollection mongoCollection = database.getCollection(collectionName, domainClass);
					dao.setMongoCollection(mongoCollection);
					dao.setCollectionName(collectionName);
					TapLogger.debug(TAG, "init {} DAO finish", collectionName);
				}
			}
		}
	}

	@Override
	public Class<? extends Annotation> watchAnnotation() {
		return MongoDAO.class;
	}
}
