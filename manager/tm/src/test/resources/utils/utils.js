/**
 * @author lg<lirufei0808@gmail.com>
 * @date 2020/11/28 3:19 下午
 * @description
 */


db.mdb_cluster.find({status: {$ne: 'Deleted'}}, {
	_id: 1,
	status: 1,
	name: 1,
	createAt: 1,
	createUser: 1,
	createBy: 1,
	spec: 1
}).toArray().map(r => {

	if (r.spec.type === 'ShardedCluster') {
		let s = r.spec.sharding;
		r.specStr = `分片集群，版本 ${r.spec.version}; ${s.mongosCount} mongos （${s.mongosOsSpec.cpu}C${s.mongosOsSpec.memory}G ${s.mongosOsSpec.storage}G存储），${s.shardCount} shard （${s.mongodOsSpec.cpu}C${s.mongodOsSpec.memory}G ${s.mongodOsSpec.storage}G存储, ${s.mongodsPerShardCount}节点/分片）；${s.configServerCount} config server（${s.configServerSpec.cpu}C${s.configServerSpec.memory}G ${s.configServerSpec.storage}G存储）；`;
	} else if(r.spec.type === 'ReplicaSet') {
		let s = r.spec.osSpec;
		r.specStr = `${r.spec.replication.members} 节点副本集${r.spec.replication.readOnlyMembers > 0 ? '（'+r.spec.replication.readOnlyMembers+'只读）' : ''}，版本 ${r.spec.version}; ${s.cpu}C${s.memory}G ${s.storage}G存储；备份 ${r.spec.backupStorage} G`;
	} else if(r.spec.type === 'Standalone') {
		let s = r.spec.osSpec;
		r.specStr = `单例，版本 ${r.spec.version}; ${s.cpu}C${s.memory}G ${s.storage}G存储；`;
	}

	delete r.spec;

	return r;
}).map(r => `${r._id.str}\t${r.name}\t${r.status}\t${r.createAt}\t${r.createBy}\t${r.createUser}\t${r.specStr}`).join('\n')

