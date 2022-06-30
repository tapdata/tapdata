/**
 * @author lg<lirufei0808@gmail.com>
 * @date 2020/9/15 10:53 上午
 * @description
 */

/**
 * 订购订单
 * @type {{chargeType: string, measureUnit: string, availableArea: string, spec: {replication: {readOnlyMembers: number, members: number}, engine: string, osSpec: {memory: number, cpu: number, storage: number}, type: string, version: string, sharding: {mongosOsSpec: {memory: number, cpu: number, storage: number}, configServerCount: number, shardCount: number, mongosCount: number, mongodsPerShardCount: number, mongodOsSpec: {memory: number, cpu: number, storage: number}}}, createAt: Date, customerType: string, createBy: string, lastUpdAt: Date, customerId: string, _id: string, lastUpdBy: string, region: string, status: string, measureType: string}}
 */
let tm_order = {
	_id: "",			// 主键
	customerType: "",	// 客户类型: 集团用户、政企用户、互联网用户
	customerId: "",		// 客户ID

	type: "proactivePurchase",			// 订单类型：proactivePurchase-主动购买订单，changeOrder-变更订单
	status: "",			// 订单状态：等待支付-waitingForPayment; 等待审核-waitingForReview; 等待同步-waitingForSync；等待创建资源-waitingForAllocateResource；订单完成-done

	chargeType: "",		// 付费方式: prepaid：预付费; postpaid：后付费
	measureType: "",	// 计量方式：duration：按时长; usageAmount：按使用量；线性阶梯：Linear ladder
	measureUnit: "",	// 计量单位：month-月，year-年

	region: "",			// 地域，使用移动云地域编码
	availableArea: "",	// 可用区，使用移动云可用区编码

	spec: {	// 产品（mongodb）规格
		version: "4.0.0",	// 版本
		engine: "wiredTiger",	// 固定值
		type: "Standalone",		// 实例类型: Standalone-单例，ReplicaSet-副本集，ShardedCluster-分片集群

		osSpec: {	// type = Standalone || ReplicaSet 时有效
			cpu: 4,	// CPU 核心数
			memory: 8,		// 内存大小，单位G
			storage: 100,	//存储
		},

		replication: {		// type = ReplicaSet 时有效
			members: 3,			// 副本集节点数
			readOnlyMembers: 1,	// 只读节点数，type = ReplicaSet时有效
		},

		sharding: {
			mongosCount: 2,		// mongos 节点个数
			mongosOsSpec: {
				cpu: 4,			// CPU 核心数
				memory: 8,			// 内存大小，单位G
				storage: 100,	//存储
			},

			mongodsPerShardCount: 3,
			mongodOsSpec: {
				cpu: 4,			// CPU 核心数
				memory: 8,			// 内存大小，单位G
				storage: 100,	//存储
			},

			shardCount: 2,			// Shard节点数

			configServerCount: 3,	// config server 节点数
		}
	},

	createAt: new Date(),	// 创建时间
	lastUpdAt: new Date(),	// 最后更新时间
	createBy: "",			// 创建用户ID
	lastUpdBy: ""			// 最后更新用户ID

}

/**
 * 变更订单
 * @type {{}}
 */
let tm_order_change = {
	_id: "",
	order_id: "",			// 原订单id

	type: "changeOrder",	// 订单类型：proactivePurchase-主动购买订单，changeOrder-变更订单
	status: "",				// 实例变更状态, 等待支付-waitingForPayment; 等待审核-waitingForReview; 等待同步-waitingForSync；等待创建资源-waitingForAllocateResource；订单完成-done

	spec: {	// 产品（mongodb）变更后规格
		// version: "4.0.0",	// 版本不支持变更
		engine: "wiredTiger",	// 固定值不支持变更
		type: "Standalone",		// 实例类型: Standalone-单例，ReplicaSet-副本集，ShardedCluster-分片集群；待定是否支持变更

		osSpec: {	// type = Standalone || ReplicaSet 时有效
			cpu: 4,	// CPU 核心数
			memory: 8,		// 内存大小，单位G
			storage: 100,	//存储
		},

		replication: {		// type = ReplicaSet 时有效
			members: 3,			// 副本集节点数
			readOnlyMembers: 1,	// 只读节点数，type = ReplicaSet时有效
		},

		sharding: {
			mongosCount: 2,		// mongos 节点个数
			mongosOsSpec: {
				cpu: 4,			// CPU 核心数
				memory: 8,			// 内存大小，单位G
				storage: 100,	//存储
			},

			mongodsPerShardCount: 3,
			mongodOsSpec: {
				cpu: 4,			// CPU 核心数
				memory: 8,			// 内存大小，单位G
				storage: 100,	//存储
			},

			shardCount: 2,			// Shard节点数

			configServerCount: 3,	// config server 节点数
		}
	},

	createAt: new Date(),	// 创建时间
	lastUpdAt: new Date(),	// 最后更新时间
	createBy: "",			// 创建用户ID
	lastUpdBy: ""			// 最后更新用户ID
}

