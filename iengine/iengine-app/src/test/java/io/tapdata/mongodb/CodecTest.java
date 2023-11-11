package io.tapdata.mongodb;

import io.tapdata.base.BaseTest;

public class CodecTest extends BaseTest {

//    @Before
//    public void init() throws IOException, URISyntaxException {
//        super.init();
//    }
//
//    @Test
//    public void customCodecTest(){
//
//        BigInteger bigInteger = new BigInteger("123");
//
//        BigDecimal bigDecimal = new BigDecimal("54321");
//        CodecRegistry defaultCodecRegistry = MongoClient.getDefaultCodecRegistry();
//
//        Map<BsonType, Class<?>> replacements = new HashMap<>();
//        replacements.put(BsonType.DECIMAL128, BigDecimal.class);
//        BsonTypeClassMap bsonTypeClassMap = new BsonTypeClassMap(replacements);
//
//        CodecRegistry customRegistry = CodecRegistries.fromCodecs(new BigIntegerCodec(), new BigDecimalCodec());
//
//        DocumentCodecProvider documentCodecProvider = new DocumentCodecProvider(bsonTypeClassMap);
//        CodecRegistry registry = CodecRegistries.fromRegistries(
//                customRegistry,
//                CodecRegistries.fromProviders(documentCodecProvider),
//                defaultCodecRegistry
//        );
//
//
//        MongoClientOptions.Builder builder = MongoClientOptions.builder().codecRegistry(registry);
//        MongoClientURI uri = new MongoClientURI("mongodb://localhost:12345/test", builder);
//        MongoClient client = new MongoClient(uri);
//
//        MongoCollection<Document> collection = client.getDatabase("test").getCollection("codec");
//        collection.insertOne(
//                new Document("bigInteger", bigInteger)
//                        .append("bigDecimal", bigDecimal)
//        );
//        System.out.println("Insert big integer done");
//
//        System.out.println("Insert docs ");
//        MongoCursor<Document> cursor = collection.find().iterator();
//        while (cursor.hasNext()) {
//            System.out.println("--------------------");
//            Document next = cursor.next();
//            Assert.assertEquals("BigInteger custom mongodb driver codec fail ", "java.lang.String", next.get("bigInteger").getClass().getTypeName());
//            Assert.assertEquals("BigDecimal custom mongodb driver codec fail ", "java.math.BigDecimal", next.get("bigDecimal").getClass().getTypeName());
//            System.out.println("--------------------");
//        }
//
//    }

}
