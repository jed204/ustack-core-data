package com.untzuntz.coredata.tests;

import com.mongodb.BasicDBObject;
import com.untzuntz.coredata.MongoQueryRunner;
import com.untzuntz.coredata.PagingSupport;
import com.untzuntz.ustack.main.UOpts;
import org.apache.log4j.*;
import org.junit.Test;

import java.util.UUID;

import static junit.framework.Assert.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CachePlanTest {

    @Test public void testCache() throws Exception
    {
        Appender console = new ConsoleAppender(new PatternLayout("[%d{ISO8601}] %-5p - %t - %-16c - %m %n"));
        BasicConfigurator.configure(console);
        Logger.getRootLogger().setLevel(Level.INFO);

        UOpts.setCacheFlag(true);
        System.setProperty("UAppCfg.CacheHost", "localhost:11211");

        MongoQueryRunner.CachePlan plan = new MongoQueryRunner.CachePlan("testcase1", 300, 900);
        plan.setSearch(new BasicDBObject("simple", "search-" + UUID.randomUUID().toString()));
        plan.setSort(new BasicDBObject("test", "sort"));
        plan.setPaging(new PagingSupport(1, 10));

        assertNull(plan.getCacheCount());
        plan.setCacheCount(100L);
        assertEquals(new Long(100), plan.getCacheCount());

    }

    @Test public void testCacheAdvanced() throws Exception
    {
        Appender console = new ConsoleAppender(new PatternLayout("[%d{ISO8601}] %-5p - %t - %-16c - %m %n"));
        BasicConfigurator.configure(console);
        Logger.getRootLogger().setLevel(Level.INFO);

        UOpts.setCacheFlag(true);
        System.setProperty("UAppCfg.CacheHost", "localhost:11211");

        MongoQueryRunner.CachePlan plan = new MongoQueryRunner.CachePlan("testcase1", 300, 900);
        plan.setSearch(new BasicDBObject("simple", "search-" + UUID.randomUUID().toString()));
        plan.setSort(new BasicDBObject("test", "sort"));
        plan.setPaging(new PagingSupport(1, 10));

        assertNull(plan.getCacheCount());
        plan.setCacheCount(100L);
        assertEquals(new Long(100), plan.getCacheCount());

        System.err.println("Query UID: " + plan.getQueryUid());


        MongoQueryRunner.CachePlan plan2 = new MongoQueryRunner.CachePlan("testcase1", 300, 900);
        plan2.setSearch(new BasicDBObject("simple", "search-" + UUID.randomUUID().toString()));
        plan2.setSort(new BasicDBObject("test", "sort"));
        plan2.setPaging(new PagingSupport(1, 10));

        assertNull(plan2.getCacheCount());
        plan2.setCacheCount(105L);
        assertEquals(new Long(105), plan2.getCacheCount());

        assertTrue(!plan.getQueryUid().equals(plan2.getQueryUid()));

        System.err.println("Query UID 2: " + plan2.getQueryUid());
    }

}
