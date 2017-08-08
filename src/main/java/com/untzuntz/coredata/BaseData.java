package com.untzuntz.coredata;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;

import org.bson.BasicBSONObject;
import org.bson.types.ObjectId;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.untzuntz.coredata.anno.DBFieldMap;

/**
 * A core element to support the API responses such as JSON output
 * 
 * @author jdanner
 *
 */
abstract public class BaseData {

	@DBFieldMap ( dbIgnore = true )
	public static final DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
	static {
		df.setTimeZone(TimeZone.getTimeZone("UTC"));
	}
	
	@DBFieldMap ( dbIgnore = true )
	public static final Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").create();

	public DBObject toDBObject()
	{
		return toDBObject(true, true);
	}
	
	public DBObject toDBObject(boolean dumpSubObjects, boolean convertDates)
	{
		return toDBObjectStatic(this, dumpSubObjects, convertDates);
	}
	
	@SuppressWarnings("unchecked")
	public static DBObject toDBObjectStatic(Object sourceObject, boolean dumpSubObjects, boolean convertDates)
	{
		DBObject obj = new BasicDBObject();

		final List<Field> fields = ReflectionUtil.getFields(sourceObject.getClass());
		for (final Field f : fields) {
			String fieldName = f.getName();
			f.setAccessible(true);

			Object object = null;

			DBFieldMap map = f.getAnnotation(DBFieldMap.class);
			if (map != null) {
				if (map.dbIgnore()) {
					continue;
				}
			}

			if (f.getType().equals(List.class)) {
				if (!dumpSubObjects)
					continue;

				List<Object> subVals = null;
				try {
					subVals = (List<Object>) f.get(sourceObject);
				} catch (IllegalArgumentException e) {
					// stub - we have marked the field accessible above
				} catch (IllegalAccessException e) {
					// stub - we have marked the field accessible above
				}
				if (subVals != null) {
					BasicDBList list = new BasicDBList();
					for (Object o : subVals) {
						if (o instanceof BaseData) {
							BaseData bd = (BaseData) o;
							list.add(bd.toDBObject());
						} else if (o instanceof BasicDBObject)
							list.add((DBObject) o);
						else {
							list.add(o);
						}
					}
					object = list;
				}
			} else {
				try {
					object = f.get(sourceObject);
				} catch (IllegalArgumentException e1) {
					// stub - we have marked the field accessible above
				} catch (IllegalAccessException e1) {
					// stub - we have marked the field accessible above
				}
			}

			if (object == null)
				continue;

			if (object instanceof ObjectId) {
				obj.put(fieldName, object.toString());
			} else if (object instanceof Timestamp) {
				Timestamp ts = (Timestamp) object;
				if (convertDates)
					obj.put(fieldName, df.format(ts));
				else
					obj.put(fieldName, ts);
			} else if (object instanceof BaseData) {
				obj.put(fieldName, ((BaseData) object).toDBObject());
			} else if (object instanceof BasicDBList) {
				obj.put(fieldName, object);
			} else if (object instanceof Date) {
				Date ts = (Date) object;
				if (convertDates)
					obj.put(fieldName, df.format(ts));
				else
					obj.put(fieldName, ts);
			} else if (object != null && object.getClass().isEnum()) {
				Method m = null;
				try {
					m = object.getClass().getDeclaredMethod("getMongoValue");
				} catch (SecurityException e) {
				} catch (NoSuchMethodException e) {
				}

				if (m == null) {
					obj.put(fieldName, object.toString());
				} else {
					m.setAccessible(true);
					try {
						obj.put(fieldName, m.invoke(object));
					} catch (InvocationTargetException e) {
						//System.err.println("IV E");
						e.printStackTrace();
					} catch (IllegalArgumentException e) {
						// stub - we have marked the method accessible above
					} catch (IllegalAccessException e) {
						// stub - we have marked the method accessible above
					}
				}
			} else {
				obj.put(fieldName, object);
			}
		}
		
		if (sourceObject instanceof MongoBaseData)
		{
			// load in any other fields the object has
			//(BSONObject)()
			BasicBSONObject extras = ((MongoBaseData)sourceObject).getExtraFields();
			Iterator<String> keys = extras.keySet().iterator();
			while (keys.hasNext())
			{
				String key = keys.next();
				//System.err.println(key + " => " + extras.get(key));
				if (convertDates && (extras.get(key) instanceof Date || extras.get(key) instanceof Timestamp))
					obj.put(key, df.format(extras.get(key)));
				else
					obj.put(key, extras.get(key));
			}
		}
		
		if (sourceObject instanceof ToDBObject)
		{
			// use the object's DBObject representation if it has one
			((ToDBObject)sourceObject).toDBObject(obj);
		}
		
		return obj;
	}
	
	public void toDBObject(DBObject o) {
		// stub - do nothing
	}
	
}
