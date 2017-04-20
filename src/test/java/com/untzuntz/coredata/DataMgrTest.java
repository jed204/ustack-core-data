package com.untzuntz.coredata;

import com.github.fakemongo.Fongo;
import com.mongodb.DBCollection;
import com.untzuntz.coredata.anno.DBPrimaryKey;
import com.untzuntz.coredata.anno.DBTableMap;
import com.untzuntz.ustack.data.MongoDB;
import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;

@RunWith(PowerMockRunner.class)
@PrepareForTest({MongoDB.class})
public class DataMgrTest {

    private Fongo fongo;

    @Before
    public void setUp() throws Exception {
        fongo = new Fongo("mock server 1");
        PowerMockito.mockStatic(MongoDB.class);

        DBCollection collection = fongo.getDB("test").getCollection("testTable");
        Mockito.when(MongoDB.getMongo()).thenReturn(fongo.getMongo());
        Mockito.when(MongoDB.getCollection("test", "testTable")).thenReturn(collection);
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