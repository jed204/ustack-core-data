package com.untzuntz.coredata;

import com.mongodb.MongoClient;
import com.untzuntz.coredata.anno.DBPrimaryKey;
import com.untzuntz.coredata.anno.DBTableMap;
import com.untzuntz.ustack.data.MongoDB;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;

@RunWith(PowerMockRunner.class)
@PrepareForTest({MongoDB.class})
public class DataMgrTest {

    private static MongoClient mongoClient;
    private static MongodExecutable mongodExecutable;


    static {

        MongodStarter starter = MongodStarter.getDefaultInstance();

        String bindIp = "localhost";
        int port = 12345;

        try {
            IMongodConfig mongodConfig = new MongodConfigBuilder()
                    .version(Version.Main.PRODUCTION)
                    .net(new Net(bindIp, port, Network.localhostIsIPv6()))
                    .build();

            mongodExecutable = starter.prepare(mongodConfig);
            MongodProcess mongod = mongodExecutable.start();

            mongoClient = new MongoClient(bindIp, port);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> mongoClient.close()));

            Runtime.getRuntime().addShutdownHook(new Thread(() -> 	{
                if (mongodExecutable != null) {
                    mongodExecutable.stop();
                }
            }));
            MongoDB.setMongo(mongoClient);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testGettingPuttingClassFromDb() throws Exception {

        ChildBaseTestPojo childBaseTestPojo = new ChildBaseTestPojo();
        childBaseTestPojo.setMyField("myField");
        childBaseTestPojo.setOtherField("otherField");

        DataMgr.saveOrUpdate(childBaseTestPojo);

        SearchFilters searchFilters = new SearchFilters();
        searchFilters.add("_id", SearchFilters.FilterType.Equals, childBaseTestPojo.getId());

        ChildBaseTestPojo childBaseTestPojo1 = MongoQueryRunner.runQuery(ChildBaseTestPojo.class, searchFilters);

        assertEquals("myField", childBaseTestPojo1.getMyField());
        assertEquals("otherField", childBaseTestPojo1.getOtherField());
    }

    public static class BaseTestPojo extends MongoBaseData {
        private String myField;

        public String getMyField() {
            return myField;
        }

        public void setMyField(String myField) {
            this.myField = myField;
        }
    }

    @DBTableMap( dbDatabase = "test", dbTable = "testTable", dbMongo = true, includeParent = true)
    public static class ChildBaseTestPojo extends BaseTestPojo {

        @DBPrimaryKey( dbColumn = "_id" )
        private ObjectId id;

        private String otherField;

        public ObjectId getId() {
            return id;
        }

        public void setId(ObjectId id) {
            this.id = id;
        }

        public String getOtherField() {
            return otherField;
        }

        public void setOtherField(String otherField) {
            this.otherField = otherField;
        }
    }

}