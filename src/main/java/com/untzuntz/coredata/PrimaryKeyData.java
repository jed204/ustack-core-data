package com.untzuntz.coredata;

import com.untzuntz.coredata.anno.DBPrimaryKey;
import com.untzuntz.coredata.anno.DBTableMap;
import com.untzuntz.coredata.exceptions.UnknownPrimaryKeyException;

import java.lang.reflect.Field;
import java.util.List;

/**
 * Primary Key helper
 * 
 * @author jdanner
 *
 */
public class PrimaryKeyData {
	
	private Object pkVal;
	private Object[] pkVals;
	private Field pkField;
	private String pkColumn;
	private String[] pkColumns;
	private Object sourceObject; 
	private boolean pkAuto;
	
	/**
	 * Gets the actual value of the primary key. This may be null!
	 * @return
	 */
	public Object getValue() {
		return pkVal;
	}
	
	/**
	 * Gets the multi-key values
	 * @return
	 */
	public Object[] getValues(Object src) {
		
		pkVals = new Object[pkColumns.length];
		// set pk values
		for (int pkColIdx = 0; pkColIdx < pkColumns.length; pkColIdx++)
		{
			String col = pkColumns[pkColIdx];
			try {
				pkVals[pkColIdx] = getFieldValue(src, col);
			} catch (Exception e) {}
		}

		return pkVals;
	}

	/**
	 * Sets the value of the object
	 * @param val
	 */
	public void setValue(Object val) {
		try {
			getField().set(sourceObject, val);
		} catch (IllegalArgumentException e) {
		} catch (IllegalAccessException e) {
		}
	}
	
	/**
	 * If true the value is not null
	 * @return
	 */
	public boolean hasValue() {
		return pkVal != null;
	}

	/**
	 * Gets the field from the Object
	 * @return
	 */
	public Field getField() {
		return pkField;
	}
	
	public boolean isAutoInc() {
		return pkAuto;
	}
	
	public boolean isMulti() {
		return pkColumns != null;
	}
	
	/**
	 * Gets the database column name of this primary key
	 * @return
	 */
	public String getColumnName() {
		return pkColumn;
	}
	
	public String[] getColumnNames() {
		return pkColumns;
	}
	
	public static PrimaryKeyData getPrimaryKeyMap(Class<?> inClass)
	{
		return getPrimaryKeyMap(inClass, null);
	}

	public static PrimaryKeyData getPrimaryKeyMap(Class<?> inClass, Object pkValue)
	{
		final Field[] fields = inClass.getDeclaredFields();
		
		/*
		 * Get the primary key (if it's set)
		 */
		for (int i = 0; i < fields.length; i++) 
		{
			final Field f = fields[i];
			f.setAccessible(true);

			DBPrimaryKey pki = f.getAnnotation(DBPrimaryKey.class);
			if (pki != null)
			{
				PrimaryKeyData pk = new PrimaryKeyData();
				pk.pkVal = pkValue;
				pk.pkField = f;
				pk.pkColumn = pki.dbColumn();
				if (!DataMgr.IGNORE.equals(pki.dbMultiKeyColumn()))
					pk.pkColumns = pki.dbMultiKeyColumn().split(",");
				pk.pkAuto = pki.dbAutoInc();
				return pk;
			}
		}
		return null;

	}
	
	public static Object getFieldValue(Object o, String fieldName) throws SecurityException, NoSuchFieldException {
		
		final Field f = o.getClass().getDeclaredField(fieldName);
		f.setAccessible(true);
		
		if (f.getName().equals(fieldName)) {
			try {
				return f.get(o);
			} catch (Exception e) {}
		}
		
		return null;
	}
	
	
	public static String getPKField(Class<?> type) 
	{
		final Field[] fields = type.getDeclaredFields();
		
		/*
		 * Get the primary key (if it's set)
		 */
		for (int i = 0; i < fields.length; i++) 
		{
			final Field f = fields[i];
			f.setAccessible(true);

			DBPrimaryKey pki = f.getAnnotation(DBPrimaryKey.class);
			if (pki != null)
				return f.getName();
		}
	
		return null;
	}
	
	public static PrimaryKeyData getInstance(DBTableMap tbl, Object obj) throws UnknownPrimaryKeyException
	{
		List<Field> fields = ReflectionUtil.getFields(tbl, obj);
		
		/*
		 * Get the primary key (if it's set)
		 */
		int size = fields.size();
		for (int i = 0; i < size; i++) 
		{
			final Field f = fields.get(i);
			f.setAccessible(true);

			DBPrimaryKey pki = f.getAnnotation(DBPrimaryKey.class);
			if (pki != null)
			{
				PrimaryKeyData pk = new PrimaryKeyData();
				try {
					pk.pkVal = f.get(obj);
				} catch (Exception e) {
					// this really shouldn't happen, but just in case
					throw new UnknownPrimaryKeyException(obj.getClass());
				}
				pk.pkField = f;
				pk.pkColumn = pki.dbColumn();
				if (!DataMgr.IGNORE.equals(pki.dbMultiKeyColumn()))
					pk.pkColumns = pki.dbMultiKeyColumn().split(",");
				pk.sourceObject = obj;
				pk.pkAuto = pki.dbAutoInc();
				return pk;
			}
		}
		
		return null; // some tables don't have a primary key
	}
	

	
}
