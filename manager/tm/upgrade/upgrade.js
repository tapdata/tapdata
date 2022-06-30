/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2022/1/10 下午8:45
 *
 * Usage:
 *   mongo 'mongodb://127.0.0.1:27017/dfs_tm' upgrade.js
 *
 * Description:
 *   1. user.phoneNumber -> user.phone
 *   2. Jobs add customId
 *   3. DataFlowInsight add customId
 *   4. Logs add user_id、customId
 *   5. MetadataInstances add user_id、customId
 *   6. Worker add customId
 *   7. Unset Connections.schema when it is empty string.
 *   8. JobDDLHistories add user_id
 *
 */

(function(){

    /**
     * batch update function
     *
     * @param collection
     * @param operatorFn
     * @param opts {query: {}, projection {}, batchSize: 5000}
     */
    const batchUpdate = function(collection, operatorFn, opts = {}){
        if (!collection) {
            print('[ERROR] collection can not be empty.');
            return;
        }
        if (!operatorFn) {
            print('[ERROR] operator function can not be empty.');
            return;
        }
        let query = opts.query || {},
            projection = opts.projection || {},
            batchSize = opts.batchSize || 5000;

        let total = collection.count(query);
        if (total === 0) {
            print(`${collection} not found document to be update, break.`);
            return;
        }

        print(`${collection} found ${total} documents to be update.`);

        let bulk = collection.initializeUnorderedBulkOp();
        let count = 0;
        let updated = 0;
        let skipped = 0;
        collection.find(query, projection).batchSize(batchSize).forEach((doc) => {

            count++;
            let skip = operatorFn(bulk, doc);
            if (skip !== false) updated++
            else skipped++;
            if (updated > 0 && updated % batchSize === 0) {
                bulk.execute();
                print(`fetch ${count} document, updated ${updated} documents, skipped ${skipped} documents.`);
                if (count !== total) {
                    bulk = collection.initializeUnorderedBulkOp();
                }
            }
        });
        if (updated > 0 && updated % batchSize > 0) {
            bulk.execute();
        }
        print(`done, update ${updated} documents, skipped ${skipped} documents.`);
    }

    /**
     * Update user
     */
    const updateUser = function() {
        print();
        print('#1. user.phoneNumber -> user.phone');
        batchUpdate(db.user, (bulk, doc) => {
            if (doc.phoneNumber) {
                bulk.find({_id: doc._id}).updateOne({$set: {
                        phone: doc.phoneNumber
                    }});
            } else {
                return false;
            }
        }, {
            query: {
                phoneNumber: {$exists: true}, phone: {$exists: false}
            }
        });
    }

    /**
     * Update Connections
     */
    const updateConnections = function(){
        print();
        print('#7. Unset Connections.schema when it is empty string.');
        let result = db.Connections.updateMany({schema: ''}, { $unset: {schema: 1}});
        print(JSON.stringify(result));
    }

    let userCustomIdCache = {};
    let externalUserIdCustomIdCache = {};
    db.user.find({}, {_id:1, customId: 1, userId: 1}).forEach((doc) => {
        userCustomIdCache[doc._id.str] = doc.customId;
        if (doc.userId)
            externalUserIdCustomIdCache[doc.userId] = doc;
    });

    /**
     * Update JobDDLHistories
     */
    const updateJobDDLHistories = function() {
        print();
        print('8. JobDDLHistories add user_id');
        let counter = 0;
        let skipped = 0;
        let updateOperations = [];
        db.JobDDLHistories.aggregate([
            {$match: { user_id : {$exists: false}}},
            {$project: {_id: 1, jobId: 1, sourceConnId: 1}},
            {$addFields: {_jobId: {$toObjectId: "$jobId"}}},
            {$lookup: {
                from: 'Jobs',
                localField: '_jobId',
                foreignField: '_id',
                as: 'job' }},
            {$project: {_id: 1, jobId: 1, sourceConnId: 1, _jobId: 1, 'job._id': 1, 'job.name': 1, 'job.user_id': 1}},
            ]).forEach(doc => {
                counter++;
                let job = doc.job && doc.job[0] || {};
                if (job.user_id) {
                    updateOperations.push({
                        updateOne: {
                            filter: {_id: doc._id},
                            update: {$set: { user_id: job.user_id}}
                        }
                    });
                    if (updateOperations.length >= 1000) {
                        db.JobDDLHistories.bulkWrite(updateOperations);
                        updateOperations = [];
                    }
                } else {
                    skipped++;
                }
                if (counter % 1000 === 0) {
                    print(`fetch ${counter} document, updated ${counter - skipped} document, skipped ${skipped} documents.`)
                }
        });
        if (updateOperations.length > 0) {
            db.JobDDLHistories.bulkWrite(updateOperations);
            updateOperations = [];
        }
        print(`fetch ${counter} document, updated ${counter - skipped} document, skipped ${skipped} documents.`)
    }

    /**
     * Add customId field to all document in the specified collection.
     * @param collection
     */
    const addCustomId = function(collection){
        if (!collection) return;
        print();
        print(`#2. ${collection} add customId`);
        batchUpdate(collection, (bulk, doc) => {
            let update;
            let user_id = doc.user_id || doc.last_user_id;
            if (userCustomIdCache[user_id]) {
                update = {$set: { customId: userCustomIdCache[user_id]}};
                if (!doc.user_id) {
                    update.$set.user_id = user_id;
                }
            } else if (externalUserIdCustomIdCache[user_id]) {
                // Worker 等表 user_id 记录的是外部用户id
                let _user = externalUserIdCustomIdCache[user_id];
                update = {$set: { customId: _user.customId, user_id: _user._id.str}};
            }
            if (update) {
                bulk.find({_id: doc._id}).updateOne(update);
            } else {
                return false;
            }
        }, {
            query: {
                $or: [{last_user_id: {$exists: true}}, {user_id: {$exists: true}}], customId: {$exists: false}
            },
            projection: {_id: 1, user_id: 1, customId: 1, last_user_id: 1}
        });
    }

    updateUser();

    updateConnections();

    updateJobDDLHistories();

    let collections = ['Jobs', 'DataFlowInsight', 'Logs', 'MetadataInstances', 'Workers', 'JobDDLHistories'];
    collections.forEach(collectionName => addCustomId(db[collectionName]) );

})();

