package com.untzuntz.coredata;

import com.Ostermiller.util.MD5;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.untzuntz.coredata.anno.DBTableMap;
import com.untzuntz.coredata.exceptions.FailedRequestException;
import com.untzuntz.coredata.exceptions.FieldSetException;
import com.untzuntz.coredata.exceptions.UnknownPrimaryKeyException;
import com.untzuntz.ustack.data.MongoDB;
import com.untzuntz.ustack.data.UDataCache;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.UnhandledException;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class MongoQueryRunner {

    static Logger logger = Logger.getLogger(MongoQueryRunner.class);

	public static <T> List<T> runListQuery(Class<T> clazz, SearchFilters filters, OrderBy orderBy, PagingSupport paging) throws FailedRequestException, UnknownPrimaryKeyException, UnhandledException
	{
		return runListQuery(clazz, filters, orderBy, paging, null);
	}	

	public static <T> List<T> runListQuery(Class<T> clazz, SearchFilters filters, OrderBy orderBy, PagingSupport paging, DBObject additionalSearch) throws FailedRequestException, UnknownPrimaryKeyException, UnhandledException
	{
		return runListQuery(clazz, filters, orderBy, paging, additionalSearch, null, null, null);
	}
	
	public static <T> int count(Class<T> clazz, SearchFilters filters, DBObject additionalSearch, Integer maxTimeSeconds) throws FailedRequestException
	{
		DBTableMap tbl = clazz.getAnnotation(DBTableMap.class);
		if (tbl == null)
			throw new FailedRequestException("Cannot persist or grab class from data source : " + clazz.getName());

		// search parameters
		DBObject searchObj = null;
		if (filters != null)
			searchObj = filters.getMongoSearchObject();
		else
			searchObj = new BasicDBObject();
		
		if (additionalSearch != null)
			searchObj.putAll(additionalSearch);

		String db = DataMgr.getDb(tbl);
		String colName = tbl.dbTable();
		DBCollection col = MongoDB.getCollection(db, colName);
		DBCursor cur = col.find(searchObj);
		if (maxTimeSeconds != null) {
			cur.maxTime(maxTimeSeconds, TimeUnit.SECONDS);
		}
		int count = cur.count();
		
		logger.info(String.format("[%s.%s] => %s | Count: %d", db, colName, searchObj, count));
		
		return count;
	}

	public static class CachePlan {
		private Integer resultCacheTime;
		private Integer countCacheTime;
		private String key;

		private boolean countFromCache;
		private boolean resultsFromCache;

		private DBObject summary = new BasicDBObject();
		private String queryUid;

		public CachePlan(String key, Integer resultCacheTime, Integer countCacheTime) {
			this.key = key;
			this.resultCacheTime = resultCacheTime;
			this.countCacheTime = countCacheTime;
		}

		public boolean hasCountCache() {
			return key != null && countCacheTime != null;
		}

		public boolean hasResultCache() {
			return key != null && resultCacheTime != null;
		}

		public void setSearch(DBObject search) {
			summary.put("search", search);
		}

		public void setSort(DBObject sort) {
			summary.put("sort", sort);
		}

		public void setPaging(PagingSupport paging) {
			summary.put("page", paging.getPage());
			summary.put("limit", paging.getItemsPerPage());
		}

		private void calculateQueryUid() {
			queryUid = String.format("mcq_%s_%s", key, DigestUtils.md5Hex(summary.toString()));
		}

		public void setCacheCount(Long value) {

			if (value == null || !hasCountCache()) {
				logger.info("Skipping Cache Count");
				return;
			}

			logger.info(String.format("Setting Cache [%s] => %d", queryUid, value));
			if (UOpts.getCacheEnabled()) {
				UDataCache.getInstance().set(queryUid, countCacheTime, value);
			}
		}

		public Long getCacheCount() {

			if (!hasCountCache()) {
				return null;
			}

			calculateQueryUid();

			if (UOpts.getCacheEnabled()) {
				return (Long)UDataCache.getInstance().get(queryUid);
			}
			return null;
		}

		public String getQueryUid() {
			return queryUid;
		}

		public boolean isCountFromCache() {
			return countFromCache;
		}

		public void setCountFromCache(boolean countFromCache) {
			this.countFromCache = countFromCache;
		}

		public boolean isResultsFromCache() {
			return resultsFromCache;
		}

		public void setResultsFromCache(boolean resultsFromCache) {
			this.resultsFromCache = resultsFromCache;
		}
	}

	public static <T> List<T> runListQuery(Class<T> clazz, SearchFilters filters, OrderBy orderBy, PagingSupport paging, DBObject additionalSearch, ExportFormat exportFormat, Integer maxTimeSeconds, CachePlan cache) throws FailedRequestException, UnknownPrimaryKeyException, UnhandledException {

		DBTableMap tbl = clazz.getAnnotation(DBTableMap.class);
		if (tbl == null)
			throw new FailedRequestException("Cannot persist or grab class from data source : " + clazz.getName());

		DBCollection col = MongoDB.getCollection(DataMgr.getDb(tbl), tbl.dbTable());
		return runListQuery(col, clazz, filters, orderBy, paging, additionalSearch, exportFormat, maxTimeSeconds, cache);
	}

	/**
	 * Executes a query against the MongoDB database for the request class and collection
	 *
	 * @throws FailedRequestException
	 * @throws UnknownPrimaryKeyException
	 * @throws UnhandledException
	 * @throws SecurityException
	 */
	public static <T> List<T> runListQuery(DBCollection col, Class<T> clazz, SearchFilters filters, OrderBy orderBy, PagingSupport paging, DBObject additionalSearch, ExportFormat exportFormat, Integer maxTimeSeconds, CachePlan cache) throws FailedRequestException, UnknownPrimaryKeyException, UnhandledException
	{
		List<T> ret = new ArrayList<T>();
		
		// search parameters
		DBObject searchObj = null;
		if (filters != null)
			searchObj = filters.getMongoSearchObject();
		else
			searchObj = new BasicDBObject();
		
		if (additionalSearch != null) {
			searchObj.putAll(additionalSearch);
		}

		// paging and limits
		int skip = 0;
		int limit = -1;
		if (paging != null)
		{
			skip = (paging.getPage() - 1) * paging.getItemsPerPage();
			limit = paging.getItemsPerPage(); 
		}

		// sorting parameters
		DBObject orderByObj = null;
		if (orderBy != null) {
			orderByObj = new BasicDBObject();
			orderByObj.put(orderBy.getFieldName(), orderBy.getDirection().getOrderInt());
		}

		long start = System.currentTimeMillis();
		
		// run the actual query
		DBCursor cur = col.find(searchObj);

		if (maxTimeSeconds != null) {
			cur.maxTime(maxTimeSeconds, TimeUnit.SECONDS);
		}

		cur.sort(orderByObj);
		cur.skip(skip);
		if (limit != -1)
			cur.limit(limit);

		// setup result paging
		long pagingStart = System.currentTimeMillis();
		boolean nocount = false;
		if (paging != null && !paging.isNoCount()) {
			Long total = null;
			if (cache != null) {
				cache.setSort(orderByObj);
				cache.setPaging(paging);
				cache.setSearch(searchObj);
				total = cache.getCacheCount();
			}
			if (total == null) {
				total = new Long(cur.count());
				if (cache != null) {
					cache.setCacheCount(total);
				}
			}
			paging.setTotal(total);
		} else {
			nocount = true;
		}
		long pagingEnd = System.currentTimeMillis();
		
		long iterateStart = System.currentTimeMillis();
		if (exportFormat != null)
			exportFormat.output(cur);
		else
		{
			while (cur.hasNext()) {
				ret.add(DataMgr.getObjectFromDBObject(clazz, cur.next()));
			}
		}
		long iterateEnd = System.currentTimeMillis();

		long totalTime = (System.currentTimeMillis() - start);
		logger.info(String.format("%s | Search [%s] | Sort [%s] | Skip %d | Limit %d => PagingTime %d / IterationTime %d / TotalTime %d CountEnabled: (nocount: %s)%s",
				clazz.getSimpleName(), searchObj, orderByObj, skip, limit,
				(pagingEnd - pagingStart), (iterateEnd - iterateStart), totalTime, nocount, totalTime > 5000 ? " | SLOWQUERY" : ""));

		return ret;
	}
	
	/**
	 * Searches for a single object based on the provided search filters
	 * 
	 * @throws FailedRequestException
	 * @throws UnknownPrimaryKeyException
	 * @throws SecurityException
	 */
	public static <T> T runQuery(Class<T> clazz, SearchFilters filters) throws FailedRequestException, UnknownPrimaryKeyException
	{
		DBTableMap tbl = clazz.getAnnotation(DBTableMap.class);
		if (tbl == null)
			throw new FailedRequestException("Cannot persist or grab class from data source : " + clazz.getName());

		// search parameters
		DBObject searchObj = null;
		if (filters != null)
			searchObj = filters.getMongoSearchObject();
		else
			searchObj = new BasicDBObject();

		// run the actual query
		DBCollection col = MongoDB.getCollection(DataMgr.getDb(tbl), tbl.dbTable());
		DBObject ret = col.findOne(searchObj);
		return DataMgr.getObjectFromDBObject(clazz, ret);
	}
	
	
}
