package io.tapdata.connector.redis;

import redis.clients.jedis.*;
import redis.clients.jedis.args.*;
import redis.clients.jedis.commands.PipelineCommands;
import redis.clients.jedis.params.*;
import redis.clients.jedis.resps.*;
import redis.clients.jedis.util.KeyValue;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RedisPipeline implements PipelineCommands, Closeable {

    private final PipelineCommands pipelineCommands;

    public RedisPipeline(PipelineCommands pipelineCommands) {
        this.pipelineCommands = pipelineCommands;
    }

    @Override
    public void close() {
        if (pipelineCommands instanceof Closeable) {
            try {
                ((Closeable) pipelineCommands).close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void sync() {
        if (pipelineCommands instanceof Pipeline) {
            ((Pipeline) pipelineCommands).sync();
        } else if (pipelineCommands instanceof ClusterPipeline) {
            ((ClusterPipeline) pipelineCommands).sync();
        } else if (pipelineCommands instanceof ShardedPipeline) {
            ((ShardedPipeline) pipelineCommands).sync();
        }
    }

    @Override
    public Response<Object> fcall(String s, List<String> list, List<String> list1) {
        return null;
    }

    @Override
    public Response<Object> fcallReadonly(String s, List<String> list, List<String> list1) {
        return null;
    }

    @Override
    public Response<String> functionDelete(String s) {
        return null;
    }

    @Override
    public Response<byte[]> functionDump() {
        return null;
    }

    @Override
    public Response<String> functionFlush() {
        return null;
    }

    @Override
    public Response<String> functionFlush(FlushMode flushMode) {
        return null;
    }

    @Override
    public Response<String> functionKill() {
        return null;
    }

    @Override
    public Response<List<LibraryInfo>> functionList() {
        return null;
    }

    @Override
    public Response<List<LibraryInfo>> functionList(String s) {
        return null;
    }

    @Override
    public Response<List<LibraryInfo>> functionListWithCode() {
        return null;
    }

    @Override
    public Response<List<LibraryInfo>> functionListWithCode(String s) {
        return null;
    }

    @Override
    public Response<String> functionLoad(String s) {
        return null;
    }

    @Override
    public Response<String> functionLoadReplace(String s) {
        return null;
    }

    @Override
    public Response<String> functionRestore(byte[] bytes) {
        return null;
    }

    @Override
    public Response<String> functionRestore(byte[] bytes, FunctionRestorePolicy functionRestorePolicy) {
        return null;
    }

    @Override
    public Response<FunctionStats> functionStats() {
        return null;
    }

    @Override
    public Response<Long> geoadd(String s, double v, double v1, String s1) {
        return null;
    }

    @Override
    public Response<Long> geoadd(String s, Map<String, GeoCoordinate> map) {
        return null;
    }

    @Override
    public Response<Long> geoadd(String s, GeoAddParams geoAddParams, Map<String, GeoCoordinate> map) {
        return null;
    }

    @Override
    public Response<Double> geodist(String s, String s1, String s2) {
        return null;
    }

    @Override
    public Response<Double> geodist(String s, String s1, String s2, GeoUnit geoUnit) {
        return null;
    }

    @Override
    public Response<List<String>> geohash(String s, String... strings) {
        return null;
    }

    @Override
    public Response<List<GeoCoordinate>> geopos(String s, String... strings) {
        return null;
    }

    @Override
    public Response<List<GeoRadiusResponse>> georadius(String s, double v, double v1, double v2, GeoUnit geoUnit) {
        return null;
    }

    @Override
    public Response<List<GeoRadiusResponse>> georadiusReadonly(String s, double v, double v1, double v2, GeoUnit geoUnit) {
        return null;
    }

    @Override
    public Response<List<GeoRadiusResponse>> georadius(String s, double v, double v1, double v2, GeoUnit geoUnit, GeoRadiusParam geoRadiusParam) {
        return null;
    }

    @Override
    public Response<List<GeoRadiusResponse>> georadiusReadonly(String s, double v, double v1, double v2, GeoUnit geoUnit, GeoRadiusParam geoRadiusParam) {
        return null;
    }

    @Override
    public Response<List<GeoRadiusResponse>> georadiusByMember(String s, String s1, double v, GeoUnit geoUnit) {
        return null;
    }

    @Override
    public Response<List<GeoRadiusResponse>> georadiusByMemberReadonly(String s, String s1, double v, GeoUnit geoUnit) {
        return null;
    }

    @Override
    public Response<List<GeoRadiusResponse>> georadiusByMember(String s, String s1, double v, GeoUnit geoUnit, GeoRadiusParam geoRadiusParam) {
        return null;
    }

    @Override
    public Response<List<GeoRadiusResponse>> georadiusByMemberReadonly(String s, String s1, double v, GeoUnit geoUnit, GeoRadiusParam geoRadiusParam) {
        return null;
    }

    @Override
    public Response<Long> georadiusStore(String s, double v, double v1, double v2, GeoUnit geoUnit, GeoRadiusParam geoRadiusParam, GeoRadiusStoreParam geoRadiusStoreParam) {
        return null;
    }

    @Override
    public Response<Long> georadiusByMemberStore(String s, String s1, double v, GeoUnit geoUnit, GeoRadiusParam geoRadiusParam, GeoRadiusStoreParam geoRadiusStoreParam) {
        return null;
    }

    @Override
    public Response<List<GeoRadiusResponse>> geosearch(String s, String s1, double v, GeoUnit geoUnit) {
        return null;
    }

    @Override
    public Response<List<GeoRadiusResponse>> geosearch(String s, GeoCoordinate geoCoordinate, double v, GeoUnit geoUnit) {
        return null;
    }

    @Override
    public Response<List<GeoRadiusResponse>> geosearch(String s, String s1, double v, double v1, GeoUnit geoUnit) {
        return null;
    }

    @Override
    public Response<List<GeoRadiusResponse>> geosearch(String s, GeoCoordinate geoCoordinate, double v, double v1, GeoUnit geoUnit) {
        return null;
    }

    @Override
    public Response<List<GeoRadiusResponse>> geosearch(String s, GeoSearchParam geoSearchParam) {
        return null;
    }

    @Override
    public Response<Long> geosearchStore(String s, String s1, String s2, double v, GeoUnit geoUnit) {
        return null;
    }

    @Override
    public Response<Long> geosearchStore(String s, String s1, GeoCoordinate geoCoordinate, double v, GeoUnit geoUnit) {
        return null;
    }

    @Override
    public Response<Long> geosearchStore(String s, String s1, String s2, double v, double v1, GeoUnit geoUnit) {
        return null;
    }

    @Override
    public Response<Long> geosearchStore(String s, String s1, GeoCoordinate geoCoordinate, double v, double v1, GeoUnit geoUnit) {
        return null;
    }

    @Override
    public Response<Long> geosearchStore(String s, String s1, GeoSearchParam geoSearchParam) {
        return null;
    }

    @Override
    public Response<Long> geosearchStoreStoreDist(String s, String s1, GeoSearchParam geoSearchParam) {
        return null;
    }

    @Override
    public Response<Long> hset(String s, String s1, String s2) {
        return pipelineCommands.hset(s, s1, s2);
    }

    @Override
    public Response<Long> hset(String s, Map<String, String> map) {
        return null;
    }

    @Override
    public Response<String> hget(String s, String s1) {
        return null;
    }

    @Override
    public Response<Long> hsetnx(String s, String s1, String s2) {
        return null;
    }

    @Override
    public Response<String> hmset(String s, Map<String, String> map) {
        return pipelineCommands.hmset(s, map);
    }

    @Override
    public Response<List<String>> hmget(String s, String... strings) {
        return null;
    }

    @Override
    public Response<Long> hincrBy(String s, String s1, long l) {
        return null;
    }

    @Override
    public Response<Double> hincrByFloat(String s, String s1, double v) {
        return null;
    }

    @Override
    public Response<Boolean> hexists(String s, String s1) {
        return null;
    }

    @Override
    public Response<Long> hdel(String s, String... strings) {
        return pipelineCommands.hdel(s, strings);
    }

    @Override
    public Response<Long> hlen(String s) {
        return null;
    }

    @Override
    public Response<Set<String>> hkeys(String s) {
        return null;
    }

    @Override
    public Response<List<String>> hvals(String s) {
        return null;
    }

    @Override
    public Response<Map<String, String>> hgetAll(String s) {
        return null;
    }

    @Override
    public Response<String> hrandfield(String s) {
        return null;
    }

    @Override
    public Response<List<String>> hrandfield(String s, long l) {
        return null;
    }

    @Override
    public Response<Map<String, String>> hrandfieldWithValues(String s, long l) {
        return null;
    }

    @Override
    public Response<ScanResult<Map.Entry<String, String>>> hscan(String s, String s1, ScanParams scanParams) {
        return null;
    }

    @Override
    public Response<Long> hstrlen(String s, String s1) {
        return null;
    }

    @Override
    public Response<Long> pfadd(String s, String... strings) {
        return null;
    }

    @Override
    public Response<String> pfmerge(String s, String... strings) {
        return null;
    }

    @Override
    public Response<Long> pfcount(String s) {
        return null;
    }

    @Override
    public Response<Long> pfcount(String... strings) {
        return null;
    }

    @Override
    public Response<Boolean> exists(String s) {
        return null;
    }

    @Override
    public Response<Long> exists(String... strings) {
        return null;
    }

    @Override
    public Response<Long> persist(String s) {
        return null;
    }

    @Override
    public Response<String> type(String s) {
        return null;
    }

    @Override
    public Response<byte[]> dump(String s) {
        return null;
    }

    @Override
    public Response<String> restore(String s, long l, byte[] bytes) {
        return null;
    }

    @Override
    public Response<String> restore(String s, long l, byte[] bytes, RestoreParams restoreParams) {
        return null;
    }

    @Override
    public Response<Long> expire(String s, long l) {
        return null;
    }

    @Override
    public Response<Long> expire(String s, long l, ExpiryOption expiryOption) {
        return null;
    }

    @Override
    public Response<Long> pexpire(String s, long l) {
        return null;
    }

    @Override
    public Response<Long> pexpire(String s, long l, ExpiryOption expiryOption) {
        return null;
    }

    @Override
    public Response<Long> expireTime(String s) {
        return null;
    }

    @Override
    public Response<Long> pexpireTime(String s) {
        return null;
    }

    @Override
    public Response<Long> expireAt(String s, long l) {
        return null;
    }

    @Override
    public Response<Long> expireAt(String s, long l, ExpiryOption expiryOption) {
        return null;
    }

    @Override
    public Response<Long> pexpireAt(String s, long l) {
        return null;
    }

    @Override
    public Response<Long> pexpireAt(String s, long l, ExpiryOption expiryOption) {
        return null;
    }

    @Override
    public Response<Long> ttl(String s) {
        return null;
    }

    @Override
    public Response<Long> pttl(String s) {
        return null;
    }

    @Override
    public Response<Long> touch(String s) {
        return null;
    }

    @Override
    public Response<Long> touch(String... strings) {
        return null;
    }

    @Override
    public Response<List<String>> sort(String s) {
        return null;
    }

    @Override
    public Response<Long> sort(String s, String s1) {
        return null;
    }

    @Override
    public Response<List<String>> sort(String s, SortingParams sortingParams) {
        return null;
    }

    @Override
    public Response<Long> sort(String s, SortingParams sortingParams, String s1) {
        return null;
    }

    @Override
    public Response<List<String>> sortReadonly(String s, SortingParams sortingParams) {
        return null;
    }

    @Override
    public Response<Long> del(String s) {
        return pipelineCommands.del(s);
    }

    @Override
    public Response<Long> del(String... strings) {
        return null;
    }

    @Override
    public Response<Long> unlink(String s) {
        return null;
    }

    @Override
    public Response<Long> unlink(String... strings) {
        return null;
    }

    @Override
    public Response<Boolean> copy(String s, String s1, boolean b) {
        return null;
    }

    @Override
    public Response<String> rename(String s, String s1) {
        return null;
    }

    @Override
    public Response<Long> renamenx(String s, String s1) {
        return null;
    }

    @Override
    public Response<Long> memoryUsage(String s) {
        return null;
    }

    @Override
    public Response<Long> memoryUsage(String s, int i) {
        return null;
    }

    @Override
    public Response<Long> objectRefcount(String s) {
        return null;
    }

    @Override
    public Response<String> objectEncoding(String s) {
        return null;
    }

    @Override
    public Response<Long> objectIdletime(String s) {
        return null;
    }

    @Override
    public Response<Long> objectFreq(String s) {
        return null;
    }

    @Override
    public Response<String> migrate(String s, int i, String s1, int i1) {
        return null;
    }

    @Override
    public Response<String> migrate(String s, int i, int i1, MigrateParams migrateParams, String... strings) {
        return null;
    }

    @Override
    public Response<Set<String>> keys(String s) {
        return null;
    }

    @Override
    public Response<ScanResult<String>> scan(String s) {
        return null;
    }

    @Override
    public Response<ScanResult<String>> scan(String s, ScanParams scanParams) {
        return null;
    }

    @Override
    public Response<ScanResult<String>> scan(String s, ScanParams scanParams, String s1) {
        return null;
    }

    @Override
    public Response<String> randomKey() {
        return null;
    }

    @Override
    public Response<Long> rpush(String s, String... strings) {
        return pipelineCommands.rpush(s, strings);
    }

    @Override
    public Response<Long> lpush(String s, String... strings) {
        return null;
    }

    @Override
    public Response<Long> llen(String s) {
        return null;
    }

    @Override
    public Response<List<String>> lrange(String s, long l, long l1) {
        return null;
    }

    @Override
    public Response<String> ltrim(String s, long l, long l1) {
        return null;
    }

    @Override
    public Response<String> lindex(String s, long l) {
        return null;
    }

    @Override
    public Response<String> lset(String s, long l, String s1) {
        return null;
    }

    @Override
    public Response<Long> lrem(String s, long l, String s1) {
        return pipelineCommands.lrem(s, l, s1);
    }

    @Override
    public Response<String> lpop(String s) {
        return null;
    }

    @Override
    public Response<List<String>> lpop(String s, int i) {
        return null;
    }

    @Override
    public Response<Long> lpos(String s, String s1) {
        return null;
    }

    @Override
    public Response<Long> lpos(String s, String s1, LPosParams lPosParams) {
        return null;
    }

    @Override
    public Response<List<Long>> lpos(String s, String s1, LPosParams lPosParams, long l) {
        return null;
    }

    @Override
    public Response<String> rpop(String s) {
        return null;
    }

    @Override
    public Response<List<String>> rpop(String s, int i) {
        return null;
    }

    @Override
    public Response<Long> linsert(String s, ListPosition listPosition, String s1, String s2) {
        return null;
    }

    @Override
    public Response<Long> lpushx(String s, String... strings) {
        return null;
    }

    @Override
    public Response<Long> rpushx(String s, String... strings) {
        return null;
    }

    @Override
    public Response<List<String>> blpop(int i, String s) {
        return null;
    }

    @Override
    public Response<KeyedListElement> blpop(double v, String s) {
        return null;
    }

    @Override
    public Response<List<String>> brpop(int i, String s) {
        return null;
    }

    @Override
    public Response<KeyedListElement> brpop(double v, String s) {
        return null;
    }

    @Override
    public Response<List<String>> blpop(int i, String... strings) {
        return null;
    }

    @Override
    public Response<KeyedListElement> blpop(double v, String... strings) {
        return null;
    }

    @Override
    public Response<List<String>> brpop(int i, String... strings) {
        return null;
    }

    @Override
    public Response<KeyedListElement> brpop(double v, String... strings) {
        return null;
    }

    @Override
    public Response<String> rpoplpush(String s, String s1) {
        return null;
    }

    @Override
    public Response<String> brpoplpush(String s, String s1, int i) {
        return null;
    }

    @Override
    public Response<String> lmove(String s, String s1, ListDirection listDirection, ListDirection listDirection1) {
        return null;
    }

    @Override
    public Response<String> blmove(String s, String s1, ListDirection listDirection, ListDirection listDirection1, double v) {
        return null;
    }

    @Override
    public Response<KeyValue<String, List<String>>> lmpop(ListDirection listDirection, String... strings) {
        return null;
    }

    @Override
    public Response<KeyValue<String, List<String>>> lmpop(ListDirection listDirection, int i, String... strings) {
        return null;
    }

    @Override
    public Response<KeyValue<String, List<String>>> blmpop(long l, ListDirection listDirection, String... strings) {
        return null;
    }

    @Override
    public Response<KeyValue<String, List<String>>> blmpop(long l, ListDirection listDirection, int i, String... strings) {
        return null;
    }

    @Override
    public Response<Long> waitReplicas(String s, int i, long l) {
        return null;
    }

    @Override
    public Response<Object> eval(String s, String s1) {
        return null;
    }

    @Override
    public Response<Object> evalsha(String s, String s1) {
        return null;
    }

    @Override
    public Response<List<Boolean>> scriptExists(String s, String... strings) {
        return null;
    }

    @Override
    public Response<String> scriptLoad(String s, String s1) {
        return null;
    }

    @Override
    public Response<String> scriptFlush(String s) {
        return null;
    }

    @Override
    public Response<String> scriptFlush(String s, FlushMode flushMode) {
        return null;
    }

    @Override
    public Response<String> scriptKill(String s) {
        return null;
    }

    @Override
    public Response<Object> eval(String s) {
        return null;
    }

    @Override
    public Response<Object> eval(String s, int i, String... strings) {
        return pipelineCommands.eval(s, i, strings);
    }

    @Override
    public Response<Object> eval(String s, List<String> list, List<String> list1) {
        return null;
    }

    @Override
    public Response<Object> evalReadonly(String s, List<String> list, List<String> list1) {
        return null;
    }

    @Override
    public Response<Object> evalsha(String s) {
        return null;
    }

    @Override
    public Response<Object> evalsha(String s, int i, String... strings) {
        return null;
    }

    @Override
    public Response<Object> evalsha(String s, List<String> list, List<String> list1) {
        return null;
    }

    @Override
    public Response<Object> evalshaReadonly(String s, List<String> list, List<String> list1) {
        return null;
    }

    @Override
    public Response<Long> sadd(String s, String... strings) {
        return null;
    }

    @Override
    public Response<Set<String>> smembers(String s) {
        return null;
    }

    @Override
    public Response<Long> srem(String s, String... strings) {
        return null;
    }

    @Override
    public Response<String> spop(String s) {
        return null;
    }

    @Override
    public Response<Set<String>> spop(String s, long l) {
        return null;
    }

    @Override
    public Response<Long> scard(String s) {
        return null;
    }

    @Override
    public Response<Boolean> sismember(String s, String s1) {
        return null;
    }

    @Override
    public Response<List<Boolean>> smismember(String s, String... strings) {
        return null;
    }

    @Override
    public Response<String> srandmember(String s) {
        return null;
    }

    @Override
    public Response<List<String>> srandmember(String s, int i) {
        return null;
    }

    @Override
    public Response<ScanResult<String>> sscan(String s, String s1, ScanParams scanParams) {
        return null;
    }

    @Override
    public Response<Set<String>> sdiff(String... strings) {
        return null;
    }

    @Override
    public Response<Long> sdiffstore(String s, String... strings) {
        return null;
    }

    @Override
    public Response<Set<String>> sinter(String... strings) {
        return null;
    }

    @Override
    public Response<Long> sinterstore(String s, String... strings) {
        return null;
    }

    @Override
    public Response<Long> sintercard(String... strings) {
        return null;
    }

    @Override
    public Response<Long> sintercard(int i, String... strings) {
        return null;
    }

    @Override
    public Response<Set<String>> sunion(String... strings) {
        return null;
    }

    @Override
    public Response<Long> sunionstore(String s, String... strings) {
        return null;
    }

    @Override
    public Response<Long> smove(String s, String s1, String s2) {
        return null;
    }

    @Override
    public Response<Long> zadd(String s, double v, String s1) {
        return null;
    }

    @Override
    public Response<Long> zadd(String s, double v, String s1, ZAddParams zAddParams) {
        return null;
    }

    @Override
    public Response<Long> zadd(String s, Map<String, Double> map) {
        return null;
    }

    @Override
    public Response<Long> zadd(String s, Map<String, Double> map, ZAddParams zAddParams) {
        return null;
    }

    @Override
    public Response<Double> zaddIncr(String s, double v, String s1, ZAddParams zAddParams) {
        return null;
    }

    @Override
    public Response<Long> zrem(String s, String... strings) {
        return null;
    }

    @Override
    public Response<Double> zincrby(String s, double v, String s1) {
        return null;
    }

    @Override
    public Response<Double> zincrby(String s, double v, String s1, ZIncrByParams zIncrByParams) {
        return null;
    }

    @Override
    public Response<Long> zrank(String s, String s1) {
        return null;
    }

    @Override
    public Response<Long> zrevrank(String s, String s1) {
        return null;
    }

    @Override
    public Response<List<String>> zrange(String s, long l, long l1) {
        return null;
    }

    @Override
    public Response<List<String>> zrevrange(String s, long l, long l1) {
        return null;
    }

    @Override
    public Response<List<Tuple>> zrangeWithScores(String s, long l, long l1) {
        return null;
    }

    @Override
    public Response<List<Tuple>> zrevrangeWithScores(String s, long l, long l1) {
        return null;
    }

    @Override
    public Response<String> zrandmember(String s) {
        return null;
    }

    @Override
    public Response<List<String>> zrandmember(String s, long l) {
        return null;
    }

    @Override
    public Response<List<Tuple>> zrandmemberWithScores(String s, long l) {
        return null;
    }

    @Override
    public Response<Long> zcard(String s) {
        return null;
    }

    @Override
    public Response<Double> zscore(String s, String s1) {
        return null;
    }

    @Override
    public Response<List<Double>> zmscore(String s, String... strings) {
        return null;
    }

    @Override
    public Response<Tuple> zpopmax(String s) {
        return null;
    }

    @Override
    public Response<List<Tuple>> zpopmax(String s, int i) {
        return null;
    }

    @Override
    public Response<Tuple> zpopmin(String s) {
        return null;
    }

    @Override
    public Response<List<Tuple>> zpopmin(String s, int i) {
        return null;
    }

    @Override
    public Response<Long> zcount(String s, double v, double v1) {
        return null;
    }

    @Override
    public Response<Long> zcount(String s, String s1, String s2) {
        return null;
    }

    @Override
    public Response<List<String>> zrangeByScore(String s, double v, double v1) {
        return null;
    }

    @Override
    public Response<List<String>> zrangeByScore(String s, String s1, String s2) {
        return null;
    }

    @Override
    public Response<List<String>> zrevrangeByScore(String s, double v, double v1) {
        return null;
    }

    @Override
    public Response<List<String>> zrangeByScore(String s, double v, double v1, int i, int i1) {
        return null;
    }

    @Override
    public Response<List<String>> zrevrangeByScore(String s, String s1, String s2) {
        return null;
    }

    @Override
    public Response<List<String>> zrangeByScore(String s, String s1, String s2, int i, int i1) {
        return null;
    }

    @Override
    public Response<List<String>> zrevrangeByScore(String s, double v, double v1, int i, int i1) {
        return null;
    }

    @Override
    public Response<List<Tuple>> zrangeByScoreWithScores(String s, double v, double v1) {
        return null;
    }

    @Override
    public Response<List<Tuple>> zrevrangeByScoreWithScores(String s, double v, double v1) {
        return null;
    }

    @Override
    public Response<List<Tuple>> zrangeByScoreWithScores(String s, double v, double v1, int i, int i1) {
        return null;
    }

    @Override
    public Response<List<String>> zrevrangeByScore(String s, String s1, String s2, int i, int i1) {
        return null;
    }

    @Override
    public Response<List<Tuple>> zrangeByScoreWithScores(String s, String s1, String s2) {
        return null;
    }

    @Override
    public Response<List<Tuple>> zrevrangeByScoreWithScores(String s, String s1, String s2) {
        return null;
    }

    @Override
    public Response<List<Tuple>> zrangeByScoreWithScores(String s, String s1, String s2, int i, int i1) {
        return null;
    }

    @Override
    public Response<List<Tuple>> zrevrangeByScoreWithScores(String s, double v, double v1, int i, int i1) {
        return null;
    }

    @Override
    public Response<List<Tuple>> zrevrangeByScoreWithScores(String s, String s1, String s2, int i, int i1) {
        return null;
    }

    @Override
    public Response<List<String>> zrange(String s, ZRangeParams zRangeParams) {
        return null;
    }

    @Override
    public Response<List<Tuple>> zrangeWithScores(String s, ZRangeParams zRangeParams) {
        return null;
    }

    @Override
    public Response<Long> zrangestore(String s, String s1, ZRangeParams zRangeParams) {
        return null;
    }

    @Override
    public Response<Long> zremrangeByRank(String s, long l, long l1) {
        return null;
    }

    @Override
    public Response<Long> zremrangeByScore(String s, double v, double v1) {
        return null;
    }

    @Override
    public Response<Long> zremrangeByScore(String s, String s1, String s2) {
        return null;
    }

    @Override
    public Response<Long> zlexcount(String s, String s1, String s2) {
        return null;
    }

    @Override
    public Response<List<String>> zrangeByLex(String s, String s1, String s2) {
        return null;
    }

    @Override
    public Response<List<String>> zrangeByLex(String s, String s1, String s2, int i, int i1) {
        return null;
    }

    @Override
    public Response<List<String>> zrevrangeByLex(String s, String s1, String s2) {
        return null;
    }

    @Override
    public Response<List<String>> zrevrangeByLex(String s, String s1, String s2, int i, int i1) {
        return null;
    }

    @Override
    public Response<Long> zremrangeByLex(String s, String s1, String s2) {
        return null;
    }

    @Override
    public Response<ScanResult<Tuple>> zscan(String s, String s1, ScanParams scanParams) {
        return null;
    }

    @Override
    public Response<KeyedZSetElement> bzpopmax(double v, String... strings) {
        return null;
    }

    @Override
    public Response<KeyedZSetElement> bzpopmin(double v, String... strings) {
        return null;
    }

    @Override
    public Response<Set<String>> zdiff(String... strings) {
        return null;
    }

    @Override
    public Response<Set<Tuple>> zdiffWithScores(String... strings) {
        return null;
    }

    @Override
    public Response<Long> zdiffStore(String s, String... strings) {
        return null;
    }

    @Override
    public Response<Long> zinterstore(String s, String... strings) {
        return null;
    }

    @Override
    public Response<Long> zinterstore(String s, ZParams zParams, String... strings) {
        return null;
    }

    @Override
    public Response<Set<String>> zinter(ZParams zParams, String... strings) {
        return null;
    }

    @Override
    public Response<Set<Tuple>> zinterWithScores(ZParams zParams, String... strings) {
        return null;
    }

    @Override
    public Response<Long> zintercard(String... strings) {
        return null;
    }

    @Override
    public Response<Long> zintercard(long l, String... strings) {
        return null;
    }

    @Override
    public Response<Set<String>> zunion(ZParams zParams, String... strings) {
        return null;
    }

    @Override
    public Response<Set<Tuple>> zunionWithScores(ZParams zParams, String... strings) {
        return null;
    }

    @Override
    public Response<Long> zunionstore(String s, String... strings) {
        return null;
    }

    @Override
    public Response<Long> zunionstore(String s, ZParams zParams, String... strings) {
        return null;
    }

    @Override
    public Response<KeyValue<String, List<Tuple>>> zmpop(SortedSetOption sortedSetOption, String... strings) {
        return null;
    }

    @Override
    public Response<KeyValue<String, List<Tuple>>> zmpop(SortedSetOption sortedSetOption, int i, String... strings) {
        return null;
    }

    @Override
    public Response<KeyValue<String, List<Tuple>>> bzmpop(long l, SortedSetOption sortedSetOption, String... strings) {
        return null;
    }

    @Override
    public Response<KeyValue<String, List<Tuple>>> bzmpop(long l, SortedSetOption sortedSetOption, int i, String... strings) {
        return null;
    }

    @Override
    public Response<StreamEntryID> xadd(String s, StreamEntryID streamEntryID, Map<String, String> map) {
        return null;
    }

    @Override
    public Response<StreamEntryID> xadd(String s, XAddParams xAddParams, Map<String, String> map) {
        return null;
    }

    @Override
    public Response<Long> xlen(String s) {
        return null;
    }

    @Override
    public Response<List<StreamEntry>> xrange(String s, StreamEntryID streamEntryID, StreamEntryID streamEntryID1) {
        return null;
    }

    @Override
    public Response<List<StreamEntry>> xrange(String s, StreamEntryID streamEntryID, StreamEntryID streamEntryID1, int i) {
        return null;
    }

    @Override
    public Response<List<StreamEntry>> xrevrange(String s, StreamEntryID streamEntryID, StreamEntryID streamEntryID1) {
        return null;
    }

    @Override
    public Response<List<StreamEntry>> xrevrange(String s, StreamEntryID streamEntryID, StreamEntryID streamEntryID1, int i) {
        return null;
    }

    @Override
    public Response<List<StreamEntry>> xrange(String s, String s1, String s2) {
        return null;
    }

    @Override
    public Response<List<StreamEntry>> xrange(String s, String s1, String s2, int i) {
        return null;
    }

    @Override
    public Response<List<StreamEntry>> xrevrange(String s, String s1, String s2) {
        return null;
    }

    @Override
    public Response<List<StreamEntry>> xrevrange(String s, String s1, String s2, int i) {
        return null;
    }

    @Override
    public Response<Long> xack(String s, String s1, StreamEntryID... streamEntryIDS) {
        return null;
    }

    @Override
    public Response<String> xgroupCreate(String s, String s1, StreamEntryID streamEntryID, boolean b) {
        return null;
    }

    @Override
    public Response<String> xgroupSetID(String s, String s1, StreamEntryID streamEntryID) {
        return null;
    }

    @Override
    public Response<Long> xgroupDestroy(String s, String s1) {
        return null;
    }

    @Override
    public Response<Boolean> xgroupCreateConsumer(String s, String s1, String s2) {
        return null;
    }

    @Override
    public Response<Long> xgroupDelConsumer(String s, String s1, String s2) {
        return null;
    }

    @Override
    public Response<StreamPendingSummary> xpending(String s, String s1) {
        return null;
    }

    @Override
    public Response<List<StreamPendingEntry>> xpending(String s, String s1, StreamEntryID streamEntryID, StreamEntryID streamEntryID1, int i, String s2) {
        return null;
    }

    @Override
    public Response<List<StreamPendingEntry>> xpending(String s, String s1, XPendingParams xPendingParams) {
        return null;
    }

    @Override
    public Response<Long> xdel(String s, StreamEntryID... streamEntryIDS) {
        return null;
    }

    @Override
    public Response<Long> xtrim(String s, long l, boolean b) {
        return null;
    }

    @Override
    public Response<Long> xtrim(String s, XTrimParams xTrimParams) {
        return null;
    }

    @Override
    public Response<List<StreamEntry>> xclaim(String s, String s1, String s2, long l, XClaimParams xClaimParams, StreamEntryID... streamEntryIDS) {
        return null;
    }

    @Override
    public Response<List<StreamEntryID>> xclaimJustId(String s, String s1, String s2, long l, XClaimParams xClaimParams, StreamEntryID... streamEntryIDS) {
        return null;
    }

    @Override
    public Response<Map.Entry<StreamEntryID, List<StreamEntry>>> xautoclaim(String s, String s1, String s2, long l, StreamEntryID streamEntryID, XAutoClaimParams xAutoClaimParams) {
        return null;
    }

    @Override
    public Response<Map.Entry<StreamEntryID, List<StreamEntryID>>> xautoclaimJustId(String s, String s1, String s2, long l, StreamEntryID streamEntryID, XAutoClaimParams xAutoClaimParams) {
        return null;
    }

    @Override
    public Response<StreamInfo> xinfoStream(String s) {
        return null;
    }

    @Override
    public Response<StreamFullInfo> xinfoStreamFull(String s) {
        return null;
    }

    @Override
    public Response<StreamFullInfo> xinfoStreamFull(String s, int i) {
        return null;
    }

    @Override
    public Response<List<StreamGroupInfo>> xinfoGroup(String s) {
        return null;
    }

    @Override
    public Response<List<StreamGroupInfo>> xinfoGroups(String s) {
        return null;
    }

    @Override
    public Response<List<StreamConsumersInfo>> xinfoConsumers(String s, String s1) {
        return null;
    }

    @Override
    public Response<List<Map.Entry<String, List<StreamEntry>>>> xread(XReadParams xReadParams, Map<String, StreamEntryID> map) {
        return null;
    }

    @Override
    public Response<List<Map.Entry<String, List<StreamEntry>>>> xreadGroup(String s, String s1, XReadGroupParams xReadGroupParams, Map<String, StreamEntryID> map) {
        return null;
    }

    @Override
    public Response<String> set(String s, String s1) {
        return pipelineCommands.set(s, s1);
    }

    @Override
    public Response<String> set(String s, String s1, SetParams setParams) {
        return null;
    }

    @Override
    public Response<String> get(String s) {
        return null;
    }

    @Override
    public Response<String> setGet(String s, String s1, SetParams setParams) {
        return null;
    }

    @Override
    public Response<String> getDel(String s) {
        return null;
    }

    @Override
    public Response<String> getEx(String s, GetExParams getExParams) {
        return null;
    }

    @Override
    public Response<Boolean> setbit(String s, long l, boolean b) {
        return null;
    }

    @Override
    public Response<Boolean> getbit(String s, long l) {
        return null;
    }

    @Override
    public Response<Long> setrange(String s, long l, String s1) {
        return null;
    }

    @Override
    public Response<String> getrange(String s, long l, long l1) {
        return null;
    }

    @Override
    public Response<String> getSet(String s, String s1) {
        return null;
    }

    @Override
    public Response<Long> setnx(String s, String s1) {
        return null;
    }

    @Override
    public Response<String> setex(String s, long l, String s1) {
        return null;
    }

    @Override
    public Response<String> psetex(String s, long l, String s1) {
        return null;
    }

    @Override
    public Response<List<String>> mget(String... strings) {
        return null;
    }

    @Override
    public Response<String> mset(String... strings) {
        return null;
    }

    @Override
    public Response<Long> msetnx(String... strings) {
        return null;
    }

    @Override
    public Response<Long> incr(String s) {
        return null;
    }

    @Override
    public Response<Long> incrBy(String s, long l) {
        return null;
    }

    @Override
    public Response<Double> incrByFloat(String s, double v) {
        return null;
    }

    @Override
    public Response<Long> decr(String s) {
        return null;
    }

    @Override
    public Response<Long> decrBy(String s, long l) {
        return null;
    }

    @Override
    public Response<Long> append(String s, String s1) {
        return null;
    }

    @Override
    public Response<String> substr(String s, int i, int i1) {
        return null;
    }

    @Override
    public Response<Long> strlen(String s) {
        return null;
    }

    @Override
    public Response<Long> bitcount(String s) {
        return null;
    }

    @Override
    public Response<Long> bitcount(String s, long l, long l1) {
        return null;
    }

    @Override
    public Response<Long> bitcount(String s, long l, long l1, BitCountOption bitCountOption) {
        return null;
    }

    @Override
    public Response<Long> bitpos(String s, boolean b) {
        return null;
    }

    @Override
    public Response<Long> bitpos(String s, boolean b, BitPosParams bitPosParams) {
        return null;
    }

    @Override
    public Response<List<Long>> bitfield(String s, String... strings) {
        return null;
    }

    @Override
    public Response<List<Long>> bitfieldReadonly(String s, String... strings) {
        return null;
    }

    @Override
    public Response<Long> bitop(BitOP bitOP, String s, String... strings) {
        return null;
    }

    @Override
    public Response<LCSMatchResult> strAlgoLCSKeys(String s, String s1, StrAlgoLCSParams strAlgoLCSParams) {
        return null;
    }

    @Override
    public Response<LCSMatchResult> lcs(String s, String s1, LCSParams lcsParams) {
        return null;
    }
}
