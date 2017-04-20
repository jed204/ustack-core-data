package com.untzuntz.coredata;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import javax.mail.internet.InternetAddress;
import javax.sql.DataSource;

import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.apache.commons.dbutils.handlers.ScalarHandler;
import org.apache.log4j.Logger;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.WriteResult;
import com.untzuntz.coredata.ORMBeanProcessor.FieldMap;
import com.untzuntz.coredata.anno.DBFieldMap;
import com.untzuntz.coredata.anno.DBPrimaryKey;
import com.untzuntz.coredata.anno.DBTableMap;
import com.untzuntz.coredata.exceptions.FailedRequestException;
import com.untzuntz.coredata.exceptions.FieldSetException;
import com.untzuntz.coredata.exceptions.UnknownPrimaryKeyException;
import com.untzuntz.ustack.data.MongoDB;

/**
 * Basic Database-related utility methods
 * 
 * @author jdanner
 *
 */
public class DataMgr {

    static Logger           			logger          									= Logger.getLogger(DataMgr.class);
    private static final Hashtable<String,ComboPooledDataSource>	data 					= new Hashtable<String,ComboPooledDataSource>();
    public static final String IGNORE = "IGNORE_COL_NOW";
	private static final String FROM = " FROM ";
	private static final String AS = " AS ";
	private static final String SET = " SET ";
	private static final String INSERT_INTO = "INSERT INTO ";
	private static final String WHERE = " WHERE ";
	private static final String SELECT = "SELECT ";
	private static final String UPDATE = "UPDATE ";
	private static final String DELETE_FROM = "DELETE FROM ";
	private static final String VAL_SET = " = ?";
	
    public static final HashMap<Class,String> tblPrefixCache = new HashMap<Class,String>();
    public static final HashMap<Class,String> pkFieldCache = new HashMap<Class,String>();
    public static final HashMap<Class,List<FieldMap>> fieldToSetCache = new HashMap<Class,List<FieldMap>>();
	
	static 
	{
		dbToJavaMaps = new HashMap<Class<?>,String>();
		objectGetMap = new HashMap<Class<?>,String>();
	}
	
	public static ComboPooledDataSource getDataSource() { 
		return data.get("core");
	}
	
	public static ComboPooledDataSource getDataSource(String name) {
		return data.get(name);
	}
	
	public static void setupDatasource(String name, String url, String user, String pass)
	{
		ComboPooledDataSource cpds = new ComboPooledDataSource();
		cpds.setJdbcUrl(url);
		cpds.setUser(user);                                  
		cpds.setPassword(pass);                                  
		cpds.setMaxIdleTime(3000);
		cpds.setMinPoolSize(5);                                     
		cpds.setAcquireIncrement(5);
		cpds.setMaxPoolSize(20);
		data.put(name, cpds);
	}
	
	/**
	 * Returns which database to connect to
	 * 
	 * @param map
	 * @return
	 */
	public static String getDb(DBTableMap map)
	{
		if ("true".equalsIgnoreCase(System.getProperty("TestCase")))
			return "testCase";
		return map.dbDatabase();
	}
	
	/**
	 * Deletes an object from the database
	 * 
	 * @param obj
	 * @throws SQLException
	 * @throws UnknownPrimaryKeyException
	 */
	public static int delete(Object obj) throws SQLException, UnknownPrimaryKeyException
	{
		DBTableMap tbl = obj.getClass().getAnnotation(DBTableMap.class);
		
		PrimaryKeyData pk = PrimaryKeyData.getInstance(tbl, obj);

		if (pk.getValue() == null)
			return 0;

		if (tbl.dbMongo())
		{
			DBCollection col = MongoDB.getCollection(getDb(tbl), tbl.dbTable());
			DBObject del = new BasicDBObject();
			del.put(pk.getColumnName(), pk.getValue());
			WriteResult res = col.remove(del);
			return res.getN();
		}
		else
		{
			List<Object> fieldVals = new ArrayList<Object>();
			
			StringBuffer sql = new StringBuffer();
			sql.append(DELETE_FROM);
			sql.append( tbl.dbTable() );
			sql.append(WHERE);
			if (pk.isMulti())
			{
				String[] cols = pk.getColumnNames();
				for (int i = 0; i < cols.length; i++)
				{
					Field f = DataMgr.getFieldByName(obj, cols[i]);
					if (f == null)
						throw new IllegalArgumentException("Could not find field for col '" + cols[i] + "'");
					
					String colName = getColumnByField(f);

					sql.append(colName).append(VAL_SET);
					if ((i + 1) < cols.length)
						sql.append(" AND ");
				}
				
				Object[] vals = pk.getValues(obj);
				for (Object o : vals) 
					fieldVals.add(o);
			}
			else
			{
				sql.append(pk.getColumnName()).append(VAL_SET);
				fieldVals.add(pk.getValue());
			}
	
			//logger.info("delete sql = " + sql);
			//for (Object o : fieldVals)
			//	logger.info("\t => " + o);
			
			QueryRunner run = new QueryRunner(DataMgr.getDataSource());
			return run.update(sql.toString(), fieldVals.toArray());
		}
	}
	
	/**
	 * Creates an Index in MongoDB
	 * @param clazz
	 * @param index
	 */
	public static void ensureIndex(String indexName, Class<?> clazz, DBObject index)
	{
		if (index == null)
			return;
	
		DBTableMap tbl = clazz.getAnnotation(DBTableMap.class);

		DBCollection col = MongoDB.getCollection(getDb(tbl), tbl.dbTable());
		col.ensureIndex(index, indexName);
	}

	/**
	 * Returns an object from the database provided a primary key value and a sql string
	 * 
	 * @param inClass
	 * @param pkValue
	 * @param sqlStr
	 * @return
	 * @throws SQLException
	 * @throws UnknownPrimaryKeyException
	 */
	@SuppressWarnings("unchecked")
	public static <T> T get(Class<T> inClass, Object pkValue, String sqlStr) throws SQLException, UnknownPrimaryKeyException
	{
		return (T)get(DataMgr.getDataSource(), inClass, pkValue, sqlStr);
	}
	
	/**
	 * Returns an object from the database provided a primary key value
	 * 
	 * @param inClass
	 * @param pkValue
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <T> T get(Class<T> inClass, Object pkValue) throws SQLException, UnknownPrimaryKeyException
	{
		return (T)get(DataMgr.getDataSource(), inClass, pkValue);
	}
	
	/**
	 * Returns an object from the mongodb based on the provided search
	 * @param inClass
	 * @param search
	 * @return
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws UnknownPrimaryKeyException
	 * @throws FieldSetException
	 * @throws SecurityException
	 * @throws NoSuchFieldException
	 * @throws FailedRequestException 
	 */
	public static <T> T getFromMongo(Class<T> inClass, DBObject search) throws FailedRequestException, UnknownPrimaryKeyException
	{
		DBTableMap tbl = inClass.getAnnotation(DBTableMap.class);
		if (tbl == null)
			throw new FailedRequestException("Cannot persist or grab class from data source : " + inClass.getName());
		
		logger.info(String.format("Searching '%s.%s' for => %s", getDb(tbl), tbl.dbTable(), search));
		
		DBCollection col = MongoDB.getCollection(getDb(tbl), tbl.dbTable());
		DBObject ret = col.findOne(search);
		if (ret == null)
			return null;
		
//		logger.info("RAW OBJECT:" + ret);
		
		return getObjectFromDBObject(inClass, ret);
	}

	/**
	 * Returns an object from the mongodb based on the provided search
	 * 
	 * @param inClass
	 * @param orderBy
	 * @param paging
	 * @param filters
	 * @return
	 * @throws FailedRequestException 
	 * @throws UnknownPrimaryKeyException 
	 */
	public static <T> List<T> getListFromMongo(Class<T> inClass, DBObject extraSearch, OrderBy orderBy, PagingSupport paging, SearchFilters filters) throws UnknownPrimaryKeyException, FailedRequestException
	{
		DBObject searchObj = null;
		if (filters != null)
			searchObj = filters.getMongoSearchObject();
		else
			searchObj = new BasicDBObject();
		
		if (extraSearch != null)
			searchObj.putAll(extraSearch);
		
		int skip = (paging.getPage() - 1) * paging.getItemsPerPage();
		int limit = paging.getItemsPerPage(); 
		DBObject orderByObj = null;
		if (orderBy != null)
		{
			orderByObj = new BasicDBObject();
			orderByObj.put(orderBy.getFieldName(), orderBy.getDirection().getOrderInt());
		}

		logger.info("SEARCH => " + searchObj);
		
		return getListFromMongo(inClass, searchObj, skip, limit, paging);
	}
	
	/**
	 * Returns an object from the mongodb based on the provided search
	 * @param inClass
	 * @param search
	 * @return
	 * @throws UnknownPrimaryKeyException
	 * @throws FailedRequestException 
	 */
	public static <T> List<T> getListFromMongo(Class<T> inClass, DBObject search, Integer skip, Integer limit, PagingSupport paging) throws FailedRequestException, UnknownPrimaryKeyException
	{
		DBTableMap tbl = inClass.getAnnotation(DBTableMap.class);
		if (tbl == null)
			throw new FailedRequestException("Cannot persist or grab class from data source : " + inClass.getName());
		
		logger.info(String.format("Searching '%s.%s' for => %s", getDb(tbl), tbl.dbTable(), search));
		
		DBCollection col = MongoDB.getCollection(getDb(tbl), tbl.dbTable());
		DBCursor cur = col.find(search);
		if (paging != null)
			paging.setTotal(new Long(cur.count()));
		
		if (skip != null)
			cur.skip(skip);
		if (limit != null)
			cur.limit(limit);
		
		List<T> ret = new ArrayList<T>();
		
		while (cur.hasNext())
		{
			DBObject obj = cur.next();
			T retObj = getObjectFromDBObject(inClass, obj);
			ret.add(retObj);
		}
		
		return ret;
	}
	
	/**
	 * Returns the DBCollection for the requested object
	 * 
	 * @param obj
	 * @return
	 * @throws FailedRequestException
	 */
	public static DBCollection getDBCollection(Class inClass) throws FailedRequestException
	{
		@SuppressWarnings("unchecked")
		DBTableMap tbl = (DBTableMap)inClass.getAnnotation(DBTableMap.class);
		if (tbl == null)
			throw new FailedRequestException("Cannot persist or grab class from data source : " + inClass.getName());
		
		return MongoDB.getCollection(getDb(tbl), tbl.dbTable());
	}

	/**
	 * Returns an item from the Mongo database
	 * @param inClass
	 * @param pkValue
	 * @return
	 * @throws UnknownPrimaryKeyException 
	 * @throws FailedRequestException 
	 */
	public static <T> T getFromMongo(Class<T> inClass, Object pkValue) throws FailedRequestException, UnknownPrimaryKeyException
	{
		PrimaryKeyData pk = PrimaryKeyData.getPrimaryKeyMap(inClass, pkValue);

		if (pk == null)
			return null;

		DBTableMap tbl = inClass.getAnnotation(DBTableMap.class);
		
		DBCollection col = MongoDB.getCollection(getDb(tbl), tbl.dbTable());
		DBObject ret = col.findOne(new BasicDBObject(pk.getColumnName(), pk.getValue()));
		if (ret == null)
			return null;
		
		return getObjectFromDBObject(inClass, ret);
	}
	
	public static <T> T getObjectFromBSONObject(Class<T> inClass, BasicBSONObject ret) throws FailedRequestException, UnknownPrimaryKeyException
	{
		return getObjectFromDBObject(inClass, new BasicDBObject(ret));
	}
	
	public static <T> T getObjectFromDBObject(Class<T> inClass, DBObject ret) throws FailedRequestException, UnknownPrimaryKeyException
	{
		if (ret == null)
			return null;
		
		String fieldName = null;
		try {
			T instance = (T)inClass.newInstance();
		
			Iterator<String> pulledFields = ret.keySet().iterator();
			while (pulledFields.hasNext())
			{
				fieldName = pulledFields.next();
				try {
					callSetter(instance, fieldName, ret.get(fieldName));
				} catch (NoSuchFieldException e) {
					if (instance instanceof MongoBaseData)
					{
						//System.err.println("inClass = " + inClass.getName() + " fieldName = " + fieldName);
						MongoBaseData mbd = (MongoBaseData)instance;
						mbd.getExtraFields().put(fieldName, ret.get(fieldName));
					}
					else
						throw new FailedRequestException("Invalid Object Requested : Field [" + fieldName + "]");
				}
			}

			return instance;

		} catch (InstantiationException e) {
			logger.warn("Unable to create class of type [" + inClass.getName() + "]", e);
			throw new FailedRequestException("Failed to build Object");
		} catch (IllegalAccessException e) {
			throw new FailedRequestException("Invalid Object Requested");
		} catch (UnknownPrimaryKeyException e) {
			throw e;
		} catch (SecurityException e) {
			throw new FailedRequestException("Invalid Object Requested");
		} catch (FieldSetException e) {
			throw new FailedRequestException("Invalid Object Requested");
		}
	}
	
    @SuppressWarnings("unchecked")
	public static void callSetter(Object target, String fieldName, Object value) throws UnknownPrimaryKeyException, FailedRequestException, SecurityException, NoSuchFieldException, FieldSetException {

    	Field tField = getFieldByColumn(target, fieldName);
    	if (tField != null)
    		fieldName = tField.getName();
    	
    	String setterName = "set" + fieldName.substring(0,  1).toUpperCase() + fieldName.substring(1);

    	Class paramType = null;
    	Object checkType = null;
    	if (tField != null)
    	{
	    	try {
	    		checkType = tField.getType().newInstance();
	    	} catch (Exception e) {}
    	}
    	
//    	if (checkType != null)
//    		System.err.println(fieldName + " => checkType = " + checkType.getClass().getSimpleName() + " | " + value);
    	
    	if (checkType instanceof BaseData)
    	{
    		paramType = checkType.getClass();
    		value = DataMgr.getObjectFromDBObject(paramType, (DBObject)value);
    	}
    	else if (value instanceof BasicDBList)
    	{
    		Class cls = target.getClass();
    		//logger.debug(String.format("Getting '%s' from class '%s'", fieldName, target.getClass().getName()));
    		Field f = cls.getDeclaredField(fieldName);
    		if (f == null)
    			return;

    		List objectList = new ArrayList();
    		
    		if (f.getGenericType().getClass().equals(Class.class))
    		{
    			// we have a generic array
    			
    		}
    		
    		//logger.info(String.format("Getting '%s' from class '%s' => '%s'", fieldName, target.getClass().getName(), f.getGenericType().getClass().getName()));
    		
			ParameterizedType listTypeParam = (ParameterizedType)f.getGenericType();
		    Class<?> listType = (Class<?>)listTypeParam.getActualTypeArguments()[0];

    		BasicDBList list = (BasicDBList)value;
    		for (int i = 0; i < list.size(); i++)
    		{
    			if (list.get(i) instanceof DBObject)
    			{
	    			DBObject listObj = (DBObject)list.get(i);
	    			//logger.debug("listObj => " + listObj + " for type '" + f.getGenericType() + "'");
	    			Object newObj = null;
	    			
	    			if (listType.equals(BasicDBObject.class))
	    				newObj = listObj;
	    			else
	    				newObj = getObjectFromDBObject(listType, listObj);
	    			
	    			objectList.add(newObj);
    			}
    			else
	    			objectList.add(list.get(i));
    		}
    		
    		paramType = List.class;
    		value = objectList;
    		//logger.debug(list.size() + " items in the created list for " + fieldName);
    	}
    	else if (value instanceof DBObject)
    	{
    		Field f = target.getClass().getDeclaredField(fieldName);
    		if (f == null)
    			return;
    		
    		if (!(value instanceof DBObject))
    			value = getObjectFromDBObject(f.getType(), (DBObject)value);

    		paramType = DBObject.class;
    	}
    	else if (value instanceof Number)
    	{
    		// handle Long type safely - Long's aren't handled by MongoDB driver quite right
    		try {
    			target.getClass().getDeclaredMethod(setterName, Long.class);
    			value = ((Number)value).longValue();
    		} catch (NoSuchMethodException nse) {}
    	}
    	
    	if (value == null)
    	{
    		Field f = target.getClass().getDeclaredField(fieldName);
    		paramType = f.getType();
    	}
    	
    	if (paramType == null)
    		paramType = value.getClass();
    	
        Method setter = null;
		try {
			setter = ReflectionUtil.getMethod(setterName, paramType, target.getClass());
		} catch (NoSuchMethodException nse) {
			if (target instanceof MongoBaseData)
			{
				MongoBaseData mbd = (MongoBaseData)target;
		    	if (value == null)
		    		mbd.getExtraFields().removeField(fieldName);
		    	else
		    		mbd.getExtraFields().put(fieldName, value);
				return;
			}
		} catch (Exception e) {
			logger.error(String.format("Failed to find setter named %s", setterName), e);
		}

        if (setter == null)
        	return;

        try {
        	setter.invoke(target, new Object[]{value});
		} catch (Exception e) {
            throw new FieldSetException(setterName, e);
		}
    }
    
	/**
	 * Resolves a SQL field name based on a class and a class field
	 * 
	 * @param typeClass
	 * @param fieldName
	 * @return
	 */
	public static String resolveSQLField(Class<?> typeClass, String fieldName)
	{
		DBTableMap tbl = typeClass.getAnnotation(DBTableMap.class);
	
		Field f;
		try {
			f = typeClass.getDeclaredField(fieldName);
		} catch (SecurityException e) {
			return null;
		} catch (NoSuchFieldException e) {
			return null;
		}
		
		f.setAccessible(true);
		
		String sqlFieldName = null;
		DBFieldMap map = f.getAnnotation(DBFieldMap.class);
		if (map == null)
		{
			DBPrimaryKey pk = f.getAnnotation(DBPrimaryKey.class);
			if (pk == null)
				sqlFieldName = f.getName();
			else
				sqlFieldName = pk.dbColumn();
		}
		else
			sqlFieldName = map.dbColumn();
		
		return tbl.dbTableAlias() + "." + sqlFieldName;
	}
	
	/**
	 * Returns an object from the database provided a primary key value
	 * 
	 * @param ds
	 * @param inClass
	 * @param pkValue
	 * @return
	 * @throws SQLException
	 * @throws UnknownPrimaryKeyException
	 */
	public static <T> Object get(DataSource ds, Class<T> inClass, Object pkValue) throws SQLException, UnknownPrimaryKeyException
	{
		String sqlStr = objectGetMap.get(inClass);
		if (sqlStr == null)
		{
			DBTableMap tbl = inClass.getAnnotation(DBTableMap.class);
			PrimaryKeyData pk = PrimaryKeyData.getPrimaryKeyMap(inClass);
	
			StringBuffer sql = new StringBuffer();
			sql.append(SELECT);
			sql.append(DataMgr.getDBtoJavaMap(inClass));
			sql.append(FROM);
			sql.append(tbl.dbTable()).append(AS).append(tbl.dbTableAlias());
			sql.append(WHERE);
			sql.append(pk.getColumnName());
			sql.append(VAL_SET);
			sqlStr = sql.toString();
			
			logger.info("Setup Object Get Map [" + inClass.getName() + "] => " + sqlStr);
			objectGetMap.put(inClass, sqlStr);
		}
		
		return get(ds, inClass, pkValue, sqlStr);
	}
	
	/**
	 * Returns an object from the database provided a primary key value and supporting SQL
	 * 
	 * @param ds
	 * @param inClass
	 * @param pkValue
	 * @param sqlStr
	 * @param qId
	 * @return
	 * @throws SQLException
	 * @throws UnknownPrimaryKeyException
	 */
	public static <T> Object get(DataSource ds, Class<T> inClass, Object pkValue, String sqlStr) throws SQLException, UnknownPrimaryKeyException
	{
		QueryRunner run = new QueryRunner(ds);
		ResultSetHandler<T> h = new BeanHandler<T>(inClass, new ORMBeanRowProcessor());
		return run.query(sqlStr, h, pkValue);
	}

	/**
	 * Returns a count result query for Paging Support
	 * 
	 * @param sqlStr
	 * @param paging
	 * @return
	 * @throws SQLException
	 */
	public static PagingSupport count(String sqlStr, PagingSupport paging, Object... params)
	{
		try {
			return count(DataMgr.getDataSource(), sqlStr, paging, params);
		} catch (SQLException sqlE) {
			
			if (paging == null)
				paging = new PagingSupport();

			return paging;
		}
	}
	
	/**
	 * Returns a count result query for Paging Support
	 * 
	 * @param ds
	 * @param sqlStr
	 * @param paging
	 * @return
	 * @throws SQLException
	 */
	public static PagingSupport count(DataSource ds, String sqlStr, PagingSupport paging, Object... params)  throws SQLException
	{
		QueryRunner run = new QueryRunner(ds);
		Long total = run.query(sqlStr, new ScalarHandler<Long>(), params);
		
		if (paging == null)
			paging = new PagingSupport();
		
		paging.setTotal(total);
		
		return paging;
	}
	
	/**
	 * Saves or updates an existing object
	 * 
	 * @param obj
	 * @throws SQLException
	 * @throws UnknownPrimaryKeyException
	 */
	public static void saveOrUpdate(Object obj) throws SQLException, UnknownPrimaryKeyException
	{
		try {
			DBTableMap tbl = obj.getClass().getAnnotation(DBTableMap.class);

			if (tbl.dbMongo())
				saveOrUpdateMongo(tbl, obj);
			else
				saveOrUpdateSQL(tbl, obj);
			
		} catch (IllegalArgumentException iae) {
			logger.warn("Invalid Arguments", iae);
		} catch (IllegalAccessException iae) {
			logger.warn("Invalid Access", iae);
		}
	}
	
	private static String getColumnByField(Field f){
		String colName = null;
		DBFieldMap map = f.getAnnotation(DBFieldMap.class);
		if (map == null)
		{
			DBPrimaryKey pkMap = f.getAnnotation(DBPrimaryKey.class);
			if (pkMap != null && pkMap.dbColumn() != null)
				colName = pkMap.dbColumn();
			else
				colName = f.getName();
		}
		else if (!map.dbLoadListFromQuery() && !map.dbIgnore())
			colName = map.dbColumn();
		
		return colName;
	}
	
	private static Field getFieldByName(Object obj, String fieldName) 
	{
		final List<Field> fields = ReflectionUtil.getFields(obj.getClass());

		for (final Field f : fields) {
			f.setAccessible(true);

			if (f.getName().equals(fieldName))
				return f;
		}
		return null;
	}

	private static Field getFieldByColumn(Object obj, String inColName) throws UnknownPrimaryKeyException
	{
		PrimaryKeyData pk = PrimaryKeyData.getInstance(null, obj);
		final List<Field> fields = ReflectionUtil.getFields(obj.getClass());

		for (final Field f : fields) {
			f.setAccessible(true);

			String colName = null;
			DBFieldMap map = f.getAnnotation(DBFieldMap.class);
			if (map == null)
				colName = f.getName();
			else if (!map.dbLoadListFromQuery() && !map.dbIgnore())
				colName = map.dbColumn();

			if (pk != null && f.equals(pk.getField())) // don't set the primary key value
				colName = pk.getColumnName();

			if (inColName.equals(colName))
				return f;
		}
		return null;
	}
	
	public static void addItem(BasicDBList list, Object o) {
		
		DBObject no = new BasicDBObject();
		
		if (o instanceof BaseData)
		{
			no = ((BaseData)o).toDBObject(true, false);
			list.add(no);
		}
		if (o instanceof MongoBaseData && ((MongoBaseData)o).getExtraFields() != null)
			no.putAll((BSONObject)((MongoBaseData)o).getExtraFields());
		if (o instanceof String)
			list.add((String)o);
		
	}
	
	/**
	 * Saves or updates an item in MongoDB
	 * 
	 * @param tbl
	 * @param obj
	 * @throws UnknownPrimaryKeyException
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 */
	public static void saveOrUpdateMongo(DBTableMap tbl, Object obj) throws UnknownPrimaryKeyException, IllegalArgumentException, IllegalAccessException
	{
		PrimaryKeyData pk = PrimaryKeyData.getInstance(tbl, obj);
		final List<Field> fields = ReflectionUtil.getFields(obj.getClass());

		DBObject toSave = new BasicDBObject();
		
		for (Field f : fields)
		{
			f.setAccessible(true);

			String colName = null;
			DBFieldMap map = f.getAnnotation(DBFieldMap.class);
			if (map == null)
				colName = f.getName();
			else if (!map.dbLoadListFromQuery() && !map.dbIgnore())
				colName = map.dbColumn();
			
			if (pk != null && f.equals(pk.getField())) // don't set the primary key value
				colName = pk.getColumnName();
			
			if (colName != null)
			{
				Object bd = f.get(obj);
				if (bd instanceof List)
				{
					BasicDBList list = new BasicDBList();
					List l = (List)bd;
					for (int j = 0; j < l.size(); j++)
					{
						Object o = l.get(j);
						addItem(list, o);
					}
					
					toSave.put(colName, list);
				}
				else if (bd instanceof Set)
				{
					BasicDBList list = new BasicDBList();
					Set l = (Set)bd;
					Iterator it = l.iterator();
					while (it.hasNext()) {
						
						Object o = it.next();
						addItem(list, o);
					}
					
					toSave.put(colName, list);
				}
				else if (bd instanceof BaseData)
				{
					DBObject tObj = ((BaseData)bd).toDBObject(true, false);
					toSave.put(colName, tObj);
				}
				else if (bd instanceof InternetAddress)
					toSave.put(colName, ((InternetAddress)bd).toString());
				else if (bd != null && bd.getClass().isEnum())
				{
					Method m = null;
					try {
						m = bd.getClass().getDeclaredMethod("getMongoValue", (Class<?>)null);
					} catch (SecurityException e) {
					} catch (NoSuchMethodException e) {}
					
					if (m == null)	
					{
						toSave.put(colName, bd.toString() );
					}
					else
						try {
							toSave.put(colName, m.invoke(bd, (Object)null));
						} catch (InvocationTargetException e) {}
				}
				else
				{
					if (bd == null)
						toSave.removeField(colName);
					else
						toSave.put(colName, bd);
				}
					
			}
		}
		
		if (obj instanceof MongoBaseData && ((MongoBaseData)obj).getExtraFields() != null)
		{
			toSave.putAll((BSONObject)((MongoBaseData)obj).getExtraFields());
		}
		
		//logger.info("SAVING OBJECT: " + toSave);

		DBCollection col = MongoDB.getCollection(getDb(tbl), tbl.dbTable());
		col.save(toSave);
		
		//logger.debug("Saved object => " + toSave);
		
		try {
			callSetter(obj, pk.getField().getName(), toSave.get("_id"));
		} catch (Exception e) {}
		
	}

	/**
	 * Saves or updates data in a SQL database
	 * 
	 * @param tbl
	 * @param obj
	 * @throws UnknownPrimaryKeyException
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 * @throws SQLException
	 */
	public static void saveOrUpdateSQL(DBTableMap tbl, Object obj) throws UnknownPrimaryKeyException, IllegalArgumentException, IllegalAccessException, SQLException
	{
		List<Object> fieldVals = new ArrayList<Object>();
		PrimaryKeyData pk = PrimaryKeyData.getInstance(tbl, obj);
		List<Field> fields = ReflectionUtil.getFields(tbl, obj);

		boolean sqlUpdate = pk != null && pk.hasValue();
		if (sqlUpdate && obj instanceof MultiKey)
		{
			if (((MultiKey)obj).isNew())
				sqlUpdate = false;
		}


		/*
		 * Setup
		 */
		StringBuffer sql = new StringBuffer();
		if (sqlUpdate)
			sql.append(UPDATE).append( tbl.dbTable() ).append(SET);
		else
			sql.append(INSERT_INTO).append( tbl.dbTable() ).append(" (");
			
		StringBuffer updateSet = new StringBuffer();
		StringBuffer fieldCsv = null;
		StringBuffer questionCsv = null;
		
		if (!sqlUpdate) {
			fieldCsv = new StringBuffer();
			questionCsv = new StringBuffer();
		}
		
		/*
		 * Set Fields or Values
		 */
		int size = fields.size();
		for (int i = 0; i < size; i++) 
		{
			final Field f = fields.get(i);
			if (pk != null && !pk.isMulti() && f.equals(pk.getField())) // don't set the primary key value
				continue;
			
			f.setAccessible(true);

			String colName = getColumnByField(f);
			
			if (colName != null)
			{
				// We have a PK, so prep the update sql
				if (sqlUpdate)
				{
					if (updateSet.length() > 0)
						updateSet.append(",");
					updateSet.append(colName).append(VAL_SET);
				}
				else
				{
					// no pk, so setup the insert sql
					if (questionCsv.length() > 0)
						questionCsv.append(",");
					if (fieldCsv.length() > 0)
						fieldCsv.append(",");
					
					questionCsv.append("?");
					fieldCsv.append(colName);
				}
				fieldVals.add(f.get(obj));
			}
		}

		if (sqlUpdate)
		{
			sql.append(updateSet.toString());
			sql.append(WHERE);
			if (pk.isMulti())
			{
				String[] cols = pk.getColumnNames();
				for (int i = 0; i < cols.length; i++)
				{
					sql.append(cols[i]).append(VAL_SET);
					if ((i + 1) < cols.length)
						sql.append(" AND ");
				}
				
				Object[] vals = pk.getValues(obj);
				for (Object o : vals) 
					fieldVals.add(o);
			}
			else
			{
				sql.append(pk.getColumnName()).append(VAL_SET);
				fieldVals.add(pk.getValue());
			}
		}
		else
			sql.append(fieldCsv.toString()).append(") VALUES (").append(questionCsv.toString()).append(")");
		
		//logger.info("SQL: " + sql.toString() + " [pk = " + pk + "]");
		
		QueryRunner run = new QueryRunner(DataMgr.getDataSource());
		if (sqlUpdate)
			run.update(sql.toString(), fieldVals.toArray());
		else
		{
			//logger.info("RET AUTOINC: " + pk.isAutoInc());
			if (pk != null && pk.isAutoInc())
			{
				ResultSetHandler<Long> rsh = new ResultSetHandler<Long>() {
				    public Long handle(ResultSet rs) throws SQLException {
				        if (!rs.next()) {
				            return null;
				        }
				    
				        return rs.getLong(1);
				    }
				};
				Long pkVal = run.insert(sql.toString(), rsh, fieldVals.toArray());
				if (pk != null)
					pk.setValue(pkVal);
			}
			else
				run.update(sql.toString(), fieldVals.toArray());
		}
		
		if (obj instanceof MultiKey)
			((MultiKey)obj).markSaved();
	}

	private static HashMap<Class<?>,String> objectGetMap;
	private static HashMap<Class<?>,String> dbToJavaMaps;
	
	public static String getDBtoJavaMap(Class<?> cls) {
		
		String ret = dbToJavaMaps.get(cls);
		if (ret != null)
			return ret;
		
		DBTableMap tblMap = cls.getAnnotation(DBTableMap.class);
		String qPrefix = "";
		String rPrefix = "";
		if (tblMap != null)
		{
			qPrefix = tblMap.dbTableAlias() + ".";
			rPrefix = tblMap.dbTableAlias() + "000";
		}

		StringBuffer buf = new StringBuffer();
		List<Field> fields = ReflectionUtil.getFields(tblMap, cls);
		
		boolean added = false;
		for (Field f : fields) {
			DBFieldMap map = f.getAnnotation(DBFieldMap.class);
			if (map == null)
			{
				if (added)
					buf.append(",");
				
				DBPrimaryKey pk = f.getAnnotation(DBPrimaryKey.class);
				if (pk != null)
					buf.append(qPrefix).append(pk.dbColumn()).append(" AS ").append(rPrefix).append(f.getName());
				else
					buf.append(qPrefix).append(f.getName()).append(" AS ").append(rPrefix).append(f.getName());
				added = true;
			}
			else if (!map.dbLoadListFromQuery() && !map.dbIgnore())
			{
				if (added)
					buf.append(",");
				
				buf.append(qPrefix).append(map.dbColumn()).append(" AS ").append(rPrefix).append(f.getName());
				added = true;
			}
		}
		
		ret = buf.toString();
		dbToJavaMaps.put(cls, ret);
		
		logger.info("Setup DB to Java Map [" + cls.getName() + "] => " + buf.toString());
		
		return ret;
	}


}
