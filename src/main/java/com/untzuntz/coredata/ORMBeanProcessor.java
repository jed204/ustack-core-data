package com.untzuntz.coredata;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

import com.untzuntz.coredata.anno.DBFieldMap;
import com.untzuntz.coredata.anno.DBTableMap;
import com.untzuntz.coredata.exceptions.UnknownPrimaryKeyException;

/**
 * Processes a SQL ResultSet and converts it to Java Beans.
 * 
 * This class supports converting multiple objects out of the ResultSet and nested Beans
 * 
 * Your query should return all the proper columns for an object and it will be automatically converted
 * 
 * @author jdanner
 *
 */
public class ORMBeanProcessor {

	static 		Logger           	logger                  = Logger.getLogger(ORMBeanProcessor.class);

	// this is for optimization checks
	private static final boolean DisableCache = false;
	// flag logging to save time!
    private static final boolean VerboseLogging = false;
    
    /**
     * Do the actual conversion of a ResultSet to a Bean of the provided type.
     * 
     * Note: this method can be recursively called from within the createBean call
     * 
     * @param rs
     * @param type
     * @param fieldToLoadCache
     * @return
     * @throws SQLException
     */
    public <T> T toBean(ResultSet rs, Class<T> type, HashMap<Class,List<Field>> fieldToLoadCache, String parentPk, String parentPkField) throws SQLException {
    	T ret = null;
    	try {
    		ret = createBean(rs, type, fieldToLoadCache, parentPk, parentPkField);
		} catch (Exception e) {
			logger.warn(String.format("Failed to create bean for type [%s]", type), e);
		}
        return ret;
    }

    /**
     * Convert the ResultSet to a list of Beans
     * 
     * @param rs
     * @param type
     * @param fieldToLoadCache
     * @return
     * @throws SQLException
     */
    public <T> List<T> toBeanList(ResultSet rs, Class<T> type, HashMap<Class,List<Field>> fieldToLoadCache) throws SQLException {
        List<T> results = new ArrayList<T>();

        if (!rs.next()) {
            return results;
        }

        try {
	        do {
				results.add(createBean(rs, type, fieldToLoadCache, null, null));
	        } while (rs.next());
		} catch (Exception e) {
			logger.warn(String.format("Failed to create bean list for type [%s]", type), e);
		}

        return results;
    }

    /**
     * Call the Bean's 'set' method for the given field
     * 
     * @param target
     * @param field
     * @param value
     * @throws SQLException
     */
    private void callSetter(Object target, FieldMap field, Object value) throws SQLException {
    	
        Method setter = null;
		try {
			setter = target.getClass().getMethod(field.setter, field.type);
		} catch (Exception e) {}

        if (setter == null)
            return;

        Class<?>[] params = setter.getParameterTypes();
        try {
            // convert types for some popular ones
            if (value instanceof java.util.Date) {
            	
                final String targetType = params[0].getName();
                if ("java.sql.Date".equals(targetType))
                    value = new java.sql.Date(((java.util.Date) value).getTime());
                else if ("java.sql.Time".equals(targetType))
                    value = new java.sql.Time(((java.util.Date) value).getTime());
                else if ("java.sql.Timestamp".equals(targetType))
                    value = new java.sql.Timestamp(((java.util.Date) value).getTime());
                
            }
            
            // Don't call setter if the value object isn't the right type
            if (isCompatibleType(value, params[0]))
                setter.invoke(target, new Object[]{value});
            else
              throw new SQLException(String.format("incompatible types, cannot convert %s to %s", value.getClass().getName(), params[0].getName()));

        } catch (Exception e) {
            throw new SQLException(String.format("Cannot set %s: %s", field.f.getName(), e.getMessage()));
        }
    }
    
    /**
     * Handle the lookup of the table prefix and cache it if necessary
     * @param type
     * @return
     */
    private String getTablePrefix(Class<?> type)
    {
    	String rPrefix = DataMgr.tblPrefixCache.get(type);
		if (rPrefix == null)
		{
			DBTableMap tblMap = type.getAnnotation(DBTableMap.class);
			if (tblMap != null)
				rPrefix = tblMap.dbTableAlias() + "000";
			else
				rPrefix = "";
			
			if (!DisableCache)
				DataMgr.tblPrefixCache.put(type, rPrefix);
		}
		return rPrefix;
    }
    
    /**
     * Returns a list of 'sub' fields for this class type (one to many)
     * @param type
     * @param fieldToLoadCache
     * @return
     */
    private List<Field> getFieldsToLoad(Class<?> type, HashMap<Class,List<Field>> fieldToLoadCache)
    {
		List<Field> fieldsToLoad = fieldToLoadCache.get(type);
		if (fieldsToLoad == null)
		{
			fieldsToLoad = new ArrayList<Field>();

			List<Field> fields = DataMgr.getFields(null, type);
			for (Field f : fields)
			{
				f.setAccessible(true);
				
				DBFieldMap map = f.getAnnotation(DBFieldMap.class);
				if (map != null && map.dbLoadListFromQuery() && f.getType().equals(List.class))
					fieldsToLoad.add(f);
			}
			if (!DisableCache)
				fieldToLoadCache.put(type, fieldsToLoad);
		}
		
		return fieldsToLoad;
    }
    
    /**
     * Returns a list of FieldMap objects for the fields we should set when creating this Bean type
     * @param type
     * @param rPrefix
     * @return
     */
    private List<FieldMap> getFieldsToSet(Class<?> type, String rPrefix)
    {
		List<FieldMap> fieldToSet = DataMgr.fieldToSetCache.get(type);
		if (fieldToSet == null)
		{
			fieldToSet = new ArrayList<FieldMap>();
			
			List<Field> fields = DataMgr.getFields(null, type);
			for (Field f : fields)
			{
				f.setAccessible(true);

				DBFieldMap map = f.getAnnotation(DBFieldMap.class);

				if (map == null || (!map.dbIgnore() && !map.dbLoadListFromQuery()))
				{
			    	String fieldCamelCase = f.getName();
			    	fieldCamelCase = fieldCamelCase.substring(0,  1).toUpperCase() + fieldCamelCase.substring(1);
			    	
					FieldMap fm = new FieldMap();
					fm.f = f;
					fm.sqlField = rPrefix + f.getName();
			    	fm.setter = "set" + fieldCamelCase;
			    	fm.type = f.getType();
	
			    	fieldToSet.add(fm);
				}
			}

			if (!DisableCache)
				DataMgr.fieldToSetCache.put(type, fieldToSet);
		}

		return fieldToSet;
    }

    /**
     * Determine the Primary Key SQL field based on the type and prefix
     * 
     * @param type
     * @param rPrefix
     * @return
     */
    private PkFieldInfo getPKSQLField(Class<?> type, String rPrefix, ResultSet rs)
    {
    	// Determine the SQL FieldName
        String pkSQLField = DataMgr.pkFieldCache.get(type);
        if (pkSQLField == null)
        {
        	String pkField = PrimaryKeyData.getPKField(type);
        	if (pkField != null)
        	{
				pkSQLField = rPrefix + pkField;

				if (!DisableCache)
					DataMgr.pkFieldCache.put(type, pkSQLField);
        	}
        }
        
        if (pkSQLField == null)
        	return null;
        
        // Determine the current primary key (if it's available)
        String origPk = null;
        int pkIdx = -1;
        try {
	        ResultSetMetaData rsmd = rs.getMetaData();
	        int numberOfColumns = rsmd.getColumnCount();
	        for (int i = 1; pkIdx == -1 && i <= numberOfColumns; i++)
	        {
	        	if (pkSQLField.equals( rsmd.getColumnLabel(i) ))
	        		pkIdx = i;
	        }
        } catch (SQLException se) {}
        
        if (pkIdx > -1)
        {
	        try {
				origPk = rs.getString(pkIdx);
	        } catch (SQLException se) {}
        }
        
        // Setup return info
    	PkFieldInfo ret = new PkFieldInfo();
        ret.pkSQLField = pkSQLField;
        ret.origPk = origPk;
        return ret;
    }
    
    /**
     * Does the hard work of converting the ResultSet to the bean
     * 
     * @param rs
     * @param type
     * @param fieldToLoadCache
     * @return
     * @throws SQLException
     * @throws UnknownPrimaryKeyException 
     * @throws IllegalAccessException 
     * @throws IllegalArgumentException 
     */
	public <T> T createBean(ResultSet rs, Class<T> type, HashMap<Class,List<Field>> fieldToLoadCache, String parentPk, String parentPkField) throws SQLException, UnknownPrimaryKeyException, IllegalArgumentException, IllegalAccessException {

		// The prefix to look for based on the class type
		String tblPrefix = getTablePrefix(type);

		PkFieldInfo pk = getPKSQLField(type, tblPrefix, rs);
		if (pk != null && pk.origPk == null)
			return null;

		// this HashMap holds the 'sub' fields to load. It is kept witin this instance to avoid 
		// the negative cache tactic it uses on queries that don't contain the subfields
    	if (fieldToLoadCache == null)
    		fieldToLoadCache = new HashMap<Class,List<Field>>();

    	// Create the actual field
        T bean = null;
		try {
			bean = (T)type.newInstance();
		} catch (Exception e) {}

		// The list of 'sub' fields to load for this type AND query
		List<Field> fieldsToLoad = getFieldsToLoad(type, fieldToLoadCache);
		// The list of fields to set for this type
		List<FieldMap> fieldToSet = getFieldsToSet(type, tblPrefix);

		// Set the actual fields
		for (FieldMap f : fieldToSet) 
		{
            Object value = procesTColumn(rs, f.sqlField, f.type);
			callSetter(bean, f, value);
		}

		// get out of here now if we don't have any fields to load
		if (fieldsToLoad.size() == 0)
			return bean;
		
		// Loop through the results until we have a different primary key than the current one
		// When the PK is different we are onto another 'object'
        HashMap<Class,List<Object>> pkMap = new HashMap<Class,List<Object>>();
        do
        {
        	if (parentPk != null)
        	{
        		if (!parentPk.equals( rs.getString(parentPkField) ))
        		{
            		if (VerboseLogging)
            			logger.info(String.format(type + " - Parent PK different noted - going to previous record ['%s' vs '%s']", rs.getString(parentPkField), parentPk));
            		
            		rs.previous();
            		return bean;
        		}
        	}
        	
        	String thisPk = null;
        	if (pk != null)
        	{
        		thisPk = rs.getString(pk.pkSQLField);
	        	if (!pk.origPk.equals(thisPk))
	        	{
	        		if (VerboseLogging)
	        			logger.info(String.format(type + " - PK different noted - going to previous record ['%s' vs '%s']", pk.origPk, thisPk));
	        		
	        		rs.previous();
	        		return bean;
	        	}
        	}
        	
        	// for each 'sub' field to load
        	for (int i = 0; i < fieldsToLoad.size(); i++)
        	{
        		Field f = fieldsToLoad.get(i);
        		
				ParameterizedType listTypeParam = (ParameterizedType)f.getGenericType();
			    Class<?> listType = (Class<?>)listTypeParam.getActualTypeArguments()[0];
			    
			    // create the bean for the sub field list
			    Object subFieldObjectEntry = toBean(rs, listType, fieldToLoadCache, thisPk, pk.pkSQLField);
			    if (subFieldObjectEntry == null)
			    {
			    	if (VerboseLogging)
			    		System.out.println(type + " - No primary key for " + f.getName());
			    	
	    			fieldsToLoad.remove(i);
	    			i--;
	    			continue;
			    }
			    
			    // track PK info and add to proper list on parent bean
			    handlePkTracking(pkMap, listType, f, bean, subFieldObjectEntry);
        	}
        	
        } while (rs.next());
        
        return bean;
    }
	
	/**
	 * Track what PK's we have used or not. Only add to the list objects with PK's we haven't seen already
	 * 
	 * @param pkMap
	 * @param listType
	 * @param f
	 * @param parentBean
	 * @param o
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 * @throws UnknownPrimaryKeyException
	 */
	@SuppressWarnings("unchecked")
	private void handlePkTracking(HashMap<Class,List<Object>> pkMap, Class<?> listType, Field f, Object parentBean, Object o) throws IllegalArgumentException, IllegalAccessException, UnknownPrimaryKeyException
	{
		// get the primary key info from the created bean
		PrimaryKeyData lPk = PrimaryKeyData.getInstance(null, o);
		
		// setup the PK mapping based on this type
		List<Object> pks = pkMap.get(listType);
		if (pks == null)
		{
			pks = new ArrayList<Object>();
			pkMap.put(listType, pks);
		}

		boolean addToList = false;
		// check for existance of PK
		if (!pks.contains( lPk.getValue() ))
		{	
			// we don't have the PK yet - so let's add it
			pks.add(lPk.getValue());
			addToList = true;
		}
		else if (o instanceof MultiKeyBase) {
			addToList = true;
		}

		if (addToList) {
		    List inList = (List)f.get(parentBean);
		    if (inList == null)
		    {
		    	// the list hasn't been initialized yet - so let's do that
		    	inList = new ArrayList();
		    	f.set(parentBean, inList);
		    }

		    inList.add(o);
		}
	}

	/**
	 * Determine if the value provided by the database matches up with the data in our class
	 * 
	 * @param value
	 * @param type
	 * @return
	 */
    private boolean isCompatibleType(Object value, Class<?> type) {
    	
        // Do object check first, then primitives
        if (value == null || type.isInstance(value)) {
            return true;

        } else if (type.equals(Integer.TYPE) && Integer.class.isInstance(value)) {
            return true;

        } else if (type.equals(Long.TYPE) && Long.class.isInstance(value)) {
            return true;

        } else if (type.equals(Double.TYPE) && Double.class.isInstance(value)) {
            return true;

        } else if (type.equals(Float.TYPE) && Float.class.isInstance(value)) {
            return true;

        } else if (type.equals(Short.TYPE) && Short.class.isInstance(value)) {
            return true;

        } else if (type.equals(Byte.TYPE) && Byte.class.isInstance(value)) {
            return true;

        } else if (type.equals(Character.TYPE) && Character.class.isInstance(value)) {
            return true;

        } else if (type.equals(Boolean.TYPE) && Boolean.class.isInstance(value)) {
            return true;

        }
        return false;

    }

    /**
     * Convert a ResultSet column to the proper object type
     * @param rs
     * @param fieldSqlName
     * @param propType
     * @return
     * @throws SQLException
     */
    protected Object procesTColumn(ResultSet rs, String fieldSqlName, Class<?> propType) throws SQLException {

    	try {
    		rs.findColumn(fieldSqlName);
    	} catch (SQLException e) {
    		return null;
    	}
    	
        if ( !propType.isPrimitive() && rs.getObject(fieldSqlName) == null ) {
            return null;
        }

        if (propType.equals(String.class)) {
            return rs.getString(fieldSqlName);

        } else if (
            propType.equals(Integer.TYPE) || propType.equals(Integer.class)) {
            return Integer.valueOf(rs.getInt(fieldSqlName));

        } else if (
            propType.equals(Boolean.TYPE) || propType.equals(Boolean.class)) {
            return Boolean.valueOf(rs.getBoolean(fieldSqlName));

        } else if (propType.equals(Long.TYPE) || propType.equals(Long.class)) {
            return Long.valueOf(rs.getLong(fieldSqlName));

        } else if (
            propType.equals(Double.TYPE) || propType.equals(Double.class)) {
            return Double.valueOf(rs.getDouble(fieldSqlName));

        } else if (
            propType.equals(Float.TYPE) || propType.equals(Float.class)) {
            return Float.valueOf(rs.getFloat(fieldSqlName));

        } else if (
            propType.equals(Short.TYPE) || propType.equals(Short.class)) {
            return Short.valueOf(rs.getShort(fieldSqlName));

        } else if (propType.equals(Byte.TYPE) || propType.equals(Byte.class)) {
            return Byte.valueOf(rs.getByte(fieldSqlName));

        } else if (propType.equals(Timestamp.class)) {
            return rs.getTimestamp(fieldSqlName);

        } else if (propType.equals(SQLXML.class)) {
            return rs.getSQLXML(fieldSqlName);

        } else {
            return rs.getObject(fieldSqlName);
        }

    }
    
    /**
     * Basic mapping class for caching data
     * 
     * @author jdanner
     */
    public class FieldMap {
    	public Field f;
    	public String sqlField;
    	public String setter;
    	public Class type;
    }

    /**
     * Return package
     * 
     * @author jdanner
     */
    private class PkFieldInfo
    {
    	public String pkSQLField;
    	public String origPk;
    }


}
