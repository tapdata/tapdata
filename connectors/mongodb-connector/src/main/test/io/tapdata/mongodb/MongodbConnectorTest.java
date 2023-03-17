package io.tapdata.mongodb;


public class MongodbConnectorTest {

  public static void main(String[] args) {

    System.out.println(MongodbUtil.parseCommand("db.test.find({}).sort({_id:1}).limit(1);"));
    System.out.println(MongodbUtil.parseCommand("db.test.find({aa:1}).sort({_id:1}).limit(1);"));
    System.out.println(MongodbUtil.parseCommand("db.test.find({aa:1},{aa:0}).sort({_id:1}).limit(1);"));
    System.out.println(MongodbUtil.parseCommand("db.test.find({aa:1},{aa:0}).sort({_id:1}).limit(1).skip(2);"));
    System.out.println(MongodbUtil.parseCommand("db.test.find({aa.b.c:1},{aa:0}).sort({_id:1}).limit(1).skip(2);"));
    System.out.println(MongodbUtil.parseCommand("db .test.find({}).sort({_id:1}).limit(1);"));
    System.out.println(MongodbUtil.parseCommand("db . test . find({}). sort({_id:1}) .limit(1);"));

    System.out.println(MongodbUtil.parseCommand("db.getCollection('test').find({}).sort({_id:1}).limit(1);"));
    System.out.println(MongodbUtil.parseCommand("db.getCollection('test').aggregate([])"));





  }

}