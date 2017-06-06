package com.untzuntz.ustackserverapi;

import java.lang.reflect.Field;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Core Exception of the API
 * @author jdanner
 *
 */
abstract public class APIException extends Exception {

	private static final long serialVersionUID = 1L;
	@SuppressWarnings("unused")
	private long raisedTime;

	public APIException() { 
		super();
		setTime();
	}
	
	public APIException(String msg) {
		super(msg);
		setTime();
	}
	
	private void setTime() {
		raisedTime = System.currentTimeMillis();
	}
	
	public DBObject toDBObject()
	{
		DBObject obj = new BasicDBObject();
		
		obj.put("type", this.getClass().getSimpleName());

		final Field[] fields = getClass().getDeclaredFields();
		for (int i = 0; i < fields.length; i++) {
			final Field f = fields[i];
			String fieldName = f.getName();
			if (!"serialVersionUID".equals(fieldName))
			{
				f.setAccessible(true);
				try {
					Object o = f.get(this);
					if (!(o instanceof Exception))
						obj.put(fieldName, o);
				} catch (IllegalArgumentException e) {
				} catch (IllegalAccessException e) {
				}
			}
		}
		
		return obj;
	}
	
}
