package io.tapdata.mongodb;

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.mongodb.*;
import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.reflection.ClassAnnotationHandler;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.ReflectionUtil;
import io.tapdata.mongodb.annotation.EnsureMongoDBIndex;
import io.tapdata.mongodb.annotation.EnsureMongoDBIndexes;
import io.tapdata.mongodb.annotation.MongoDAO;
import io.tapdata.mongodb.dao.AbstractMongoDAO;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;

import java.lang.annotation.Annotation;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

@Bean
public class MongoDAOAnnotationHandler extends ClassAnnotationHandler {
	private static final String TAG = MongoDAOAnnotationHandler.class.getSimpleName();

	@SuppressWarnings("unused")
	@Bean
	private MongoClientFactory mongoClientFactory;


	@Override
	public void handle(Set<Class<?>> classes) throws CoreException {
		if (classes != null) {
			for(Class<?> clazz : classes) {
				MongoDAO mongoDocument = clazz.getAnnotation(MongoDAO.class);
				EnsureMongoDBIndexes ensureMongoDBIndexes = clazz.getAnnotation(EnsureMongoDBIndexes.class);
				EnsureMongoDBIndex ensureMongoDBIndex1 = clazz.getAnnotation(EnsureMongoDBIndex.class);
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
					String mongoUri = CommonUtils.getProperty("tapdata_proxy_mongodb_uri");
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
					//noinspection rawtypes
					MongoCollection mongoCollection = database.getCollection(collectionName, domainClass);
					//noinspection unchecked
					dao.setMongoCollection(mongoCollection);
					dao.setCollectionName(collectionName);
					TapLogger.debug(TAG, "Initialized collection {} for database {}", collectionName, database);
					List<EnsureMongoDBIndex> annotationList = new ArrayList<>();
					if(ensureMongoDBIndex1 != null) {
						annotationList.add(ensureMongoDBIndex1);
					} else if(ensureMongoDBIndexes != null && ensureMongoDBIndexes.value() != null) {
						annotationList.addAll(Arrays.asList(ensureMongoDBIndexes.value()));
					}
					ListIndexesIterable<Document> indexesIterable = mongoCollection.listIndexes();
					for(EnsureMongoDBIndex ensureMongoDBIndex : annotationList) {
						String indexJson = ensureMongoDBIndex.value();
						if(!StringUtils.isEmpty(indexJson)) {
							Document indexDocument = Document.parse(indexJson);
							boolean hasIndex = false;
							for(Document document : indexesIterable) {
								MapDifference<String, Object> difference = Maps.difference(indexDocument, (Map<String, Object>)document.get("key"));
								if(difference.areEqual()) {
									hasIndex = true;
									break;
								}
							}
							if(!hasIndex) {
								TapLogger.debug(TAG, "Start creating index {}", indexDocument);
								long time = System.currentTimeMillis();
								IndexOptions indexOptions = new IndexOptions().unique(ensureMongoDBIndex.unique()).sparse(ensureMongoDBIndex.sparse()).background(ensureMongoDBIndex.background());
								long expireAfterSeconds = ensureMongoDBIndex.expireAfterSeconds();
								if(expireAfterSeconds > 0) {
									indexOptions.expireAfter(expireAfterSeconds, TimeUnit.SECONDS);
								}
								mongoCollection.createIndex(indexDocument, indexOptions);
								TapLogger.debug(TAG, "Index {} created, takes {}", indexDocument, (System.currentTimeMillis() - time));
							}
						}
					}
				}
			}
		}
	}

	@Override
	public Class<? extends Annotation> watchAnnotation() {
		return MongoDAO.class;
	}
}
