package com.untzuntz.coredata.json;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.untzuntz.coredata.exceptions.UnknownFilterKey;

import net.minidev.json.JSONArray;

public class JsonQuery {

    static Logger           		logger               	= Logger.getLogger(JsonQuery.class);

	private BasicDBList filters;
	private FilterMode mode;
	private Object document;

	public JsonQuery(BasicDBList obj, FilterMode mode) {
		this(obj);
		this.mode = mode;
	}
	
	public JsonQuery(BasicDBList obj) {
		this.mode = FilterMode.AND;
		this.filters = obj;
	}

	public void setDocument(Object doc) {
		document = doc;
	}
	
	public boolean runCheck(DBObject doc) throws UnknownFilterKey
	{
		if (document == null)
			document = Configuration.defaultConfiguration().jsonProvider().parse(doc.toString());
		
		int passed = 0;
		for (int i = 0; i < filters.size(); i++)
		{
			Object obj = filters.get(i);
			if (obj instanceof BasicDBList)
			{
				// more sub queries
				JsonQuery inQuery = new JsonQuery((BasicDBList)obj);
				inQuery.setDocument(document);
				if (inQuery.runCheck(doc))
					passed++;
				
				logger.debug(passed + " - " + mode + " | Sub-Filter: " + obj.toString());
			}
			else
			{
				// Actual Query
				DBObject filter = (DBObject)obj;

				if (filterCheck(filter, null))
					passed++;
				
				logger.debug(passed + " - " + mode + " | Filter: " + filter);
			}
			
		}
		
		
		if (mode.equals(FilterMode.AND))
		{
			// AND query
			return passed == filters.size();
		}
		else
		{
			// OR query
			return passed > 0;
		}
		
	}
	
	private boolean filterCheck(DBObject doc, String inboundKey) throws UnknownFilterKey
	{
		Set<String> keys = doc.keySet();
		Iterator<String> it = keys.iterator();
		int passed = 0;
		while (it.hasNext())
		{
			String key = it.next();
			
			Object obj = doc.get(key);
			if (key.startsWith("$"))
			{
				Object filter = doc.get(key);
				if ("$or".equals(key))
				{
					JsonQuery inQuery = new JsonQuery((BasicDBList)filter, FilterMode.OR);
					inQuery.setDocument(document);
					logger.debug(mode + " | Sub-Filter: " + filter.toString());
					if (inQuery.runCheck(doc))
						passed++;
				}
				else if ("$elemMatch".equals(key))
				{
					//logger.info("Inbound Key: " + inboundKey + " => " + obj.toString());
					List<Map<String, Object>> items = JsonPath.read(document, "$." + inboundKey); 
					for (Map<String, Object> item : items)
					{
						DBObject oItem = new BasicDBObject();
						Iterator<String> iIt = item.keySet().iterator();
						while (iIt.hasNext()) {
							String iKey = iIt.next();
							oItem.put(iKey, item.get(iKey));
						}
						
						BasicDBList elemMatchFilter = new BasicDBList();
						elemMatchFilter.add((DBObject)obj);
						
						//logger.info("Checking ElemMatch => " + elemMatchFilter + " against document: " + oItem);
						JsonQuery jq = new JsonQuery(elemMatchFilter);
						if (jq.runCheck(oItem))
						{
							//logger.info("ElemMatch Filter Passed => " + oItem);
							passed++;
						}
					}
				}
				else if ("$exists".equals(key))
				{
					Boolean filterValue = (Boolean)filter;
					
					boolean hasValue = false;
					if (inboundKey.indexOf("..") > -1)
					{
						List<Object> values = JsonPath.read(document, "$." + inboundKey);
						if (values.size() > 0)
							hasValue = true;
					}
					else
					{
						Object checkVal = null;
						try {
							checkVal = JsonPath.read(document, "$." + inboundKey);
						} catch (PathNotFoundException pne) {}
	
						if (checkVal != null)
							hasValue = true;
					}
					
					if (hasValue == filterValue)
						passed++;					
				}
				else if ("$contains".equals(key) || "$doesNotContain".equals(key))
				{
					boolean checkPassed = false;
					String strVal = filter.toString();
					if (inboundKey.indexOf("..") > -1)
					{
						List<Object> values = JsonPath.read(document, "$." + inboundKey);
						for (Object v : values) {
							if ("$doesNotContain".equals(key))
							{
								if (v == null || !v.toString().contains(strVal))
									checkPassed = true;
							}
							else
							{
								if (v != null && v.toString().contains(strVal))
									checkPassed = true;
							}
						}
					}
					else
					{
						Object checkVal = null;
						try {
							checkVal = JsonPath.read(document, "$." + inboundKey);
						} catch (PathNotFoundException pne) {}
						
						if ("$doesNotContain".equals(key))
						{
							if (checkVal == null || !checkVal.toString().contains(strVal))
								checkPassed = true;
						}
						else
						{
							if (checkVal != null && checkVal.toString().contains(strVal))
								checkPassed = true;
						}
					}
					if (checkPassed)
					{
						passed++;
					}
				}
				else if ("$lte".equals(key) || "$lt".equals(key) || "$gte".equals(key) || "$gt".equals(key))
				{
					Number filterValue = (Number)filter;
					if (inboundKey.indexOf("..") > -1)
					{
						List<Number> values = JsonPath.read(document, "$." + inboundKey);
						
						boolean hasValue = false;
						for (Number objectValue : values)
						{
							@SuppressWarnings("unused")
							boolean thisRun = false;
							double filterInt = filterValue.doubleValue();
							double objectInt = objectValue.doubleValue();
							if ("$lte".equals(key) && filterInt <= objectInt) {
								hasValue = true;
								thisRun = true;
							}
							else if ("$lt".equals(key) && filterInt < objectInt) {
								hasValue = true;
								thisRun = true;
							}
							else if ("$gte".equals(key) && objectInt >= filterInt) {
								hasValue = true;
								thisRun = true;
							}
							else if ("$gt".equals(key) && objectInt > filterInt) {
								hasValue = true;
								thisRun = true;
							}
							
							//logger.info(key + " => " + filterInt + " vs. " + objectInt + " [" + thisRun + "]");
						}
						
						
						if (hasValue)
							passed++;
					}
					else
					{
						Number checkVal = null;
						try {
							checkVal = JsonPath.read(document, "$." + inboundKey);
						} catch (PathNotFoundException pne) {}
	
						if (checkVal != null)
						{
							boolean hasValue = false;
							double filterInt = filterValue.doubleValue();
							double objectInt = checkVal.doubleValue();
							if ("$lte".equals(key) && filterInt <= objectInt)
								hasValue = true;
							else if ("$lt".equals(key) && filterInt < objectInt)
								hasValue = true;
							else if ("$gte".equals(key) && filterInt >= objectInt)
								hasValue = true;
							else if ("$gt".equals(key) && filterInt > objectInt)
								hasValue = true;
							
							if (hasValue) 
								passed++;
						}
						else
							logger.warn("Could not find check value @ " + inboundKey);
					}
				}
				else if ("$neq".equals(key))
				{
					boolean checkPassed = false;
					String strVal = filter.toString();
					if (inboundKey.indexOf("..") > -1)
					{
						List<Object> values = JsonPath.read(document, "$." + inboundKey);
						for (Object v : values) {
							if (!strVal.equals(v.toString()))
								checkPassed = true;
						}
					}
					else
					{
						Object checkVal = null;
						try {
							checkVal = JsonPath.read(document, "$." + inboundKey);
						} catch (PathNotFoundException pne) {}
						
						if (checkVal == null || !strVal.equals(checkVal.toString()))
							checkPassed = true;
					}
					if (checkPassed)
					{
						passed++;
					}
				}
				else if ("$isUpdated".equals(key))
				{
					if (updateCheck(inboundKey))
					{
						passed++;
					}
				}
				else
					throw new UnknownFilterKey(key);
			}
			else if (obj instanceof String)
			{
				boolean checkPassed = false;
				String strVal = (String)obj;
				if (key.indexOf("..") > -1)
				{
					List<Object> values = JsonPath.read(document, "$." + key);
					boolean hasValue = false;
					for (Object v : values) {
						if (strVal.equals(v.toString()))
							hasValue = true;
					}
					
					if (hasValue)
						checkPassed = true;
				}
				else
				{
					Object checkVal = null;
					try {
						checkVal = JsonPath.read(document, "$." + key);
					} catch (PathNotFoundException pne) {}
					
					if (checkVal != null && strVal.equals(checkVal.toString()))
						checkPassed = true;
				}
				if (checkPassed)
				{
					passed++;
				}
				//logger.debug("\t[" + checkPassed + "] Value Check | " + key + " => " + strVal);
				
			}
			else if (obj instanceof DBObject)
			{
				DBObject filter = (DBObject)doc.get(key);
				if (filterCheck(filter, key))
					passed++;
			}
			else
				logger.warn("Unknown object type [" + key + "]: " + obj.getClass().getName());
		}
		
		if (mode.equals(FilterMode.AND))
		{
			// AND query
			return passed == keys.size();
		}
		else
		{
			// OR query
			return passed > 0;
		}
	}
	
	private boolean updateCheck(String inboundKey){
		boolean checkPassed = false;

		Object checkVal = null;
		try {
			checkVal = JsonPath.read(document, "$.fieldChangeList[*]." + inboundKey);
		} catch (PathNotFoundException pne) {}
				
		if (checkVal != null && checkVal instanceof JSONArray && !((JSONArray)checkVal).isEmpty()){
			checkPassed = true;
		}
		
		return checkPassed;
	}
	
	private static enum FilterMode {
		AND,
		OR;
	}
	
}
