package com.untzuntz.coredata.json;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
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

    private List<String> reasons;
	private BasicDBList filters;
	private FilterMode mode;
	private Object document;
	private DecimalFormat df = new DecimalFormat("0.0");
	
	public List<String> getReasons() {
		return reasons;
	}

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
	
	public void addReasons(List<String> re) {
		if (re == null)
			return;
		
		if (reasons == null)
			reasons = new ArrayList<String>();
		
		reasons.addAll(re);
	}
	
	public void addReason(String r) {
		
		if (r == null)
			return;
		
		if (reasons == null)
			reasons = new ArrayList<String>();

		reasons.add(r);
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
				{
					passed++;
					addReasons(inQuery.getReasons());
				}
				
				logger.debug(passed + " - " + mode + " | Sub-Filter: " + obj.toString());
			}
			else
			{
				// Actual Query
				DBObject filter = (DBObject)obj;
				
				if (filterCheck(filter, null, mode))
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
	
	private boolean filterCheck(DBObject doc, String inboundKey, FilterMode mode) throws UnknownFilterKey
	{
		String reason = null;
		DecimalFormat ldf = df;
		
		Map<String,String> reasonMap = new HashMap<String,String>();
		Set<String> keys = doc.keySet();
		Iterator<String> it = keys.iterator();
		int passed = 0;
		int expectedPass = keys.size();
		
		if (doc.get("$formatNumber") != null) {
			ldf = new DecimalFormat((String)doc.get("$formatNumber"));
		}

		while (it.hasNext())
		{
			String key = it.next();
			
			Object obj = doc.get(key);
			if (key.startsWith("$"))
			{
				Object filter = doc.get(key);
				if ("$reason".equals(key)) {
					reason = filter.toString();
					expectedPass--;
				}
				else if ("$formatNumber".equals(key))
				{
					expectedPass--;
				}
				else if ("$or".equals(key))
				{
					JsonQuery inQuery = new JsonQuery((BasicDBList)filter, FilterMode.OR);
					inQuery.setDocument(document);
					logger.debug(mode + " | Sub-Filter: " + filter.toString());
					if (inQuery.runCheck(doc))
					{
						passed++;
						addReasons(inQuery.getReasons());
					}
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
							addReasons(jq.getReasons());
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
								if (v == null || !v.toString().toLowerCase().contains(strVal.toLowerCase()))
									checkPassed = true;
							}
							else
							{
								if (v != null && v.toString().toLowerCase().contains(strVal.toLowerCase()))
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
							if (checkVal == null || !checkVal.toString().toLowerCase().contains(strVal.toLowerCase()))
								checkPassed = true;
						}
						else
						{
							if (checkVal != null && checkVal.toString().toLowerCase().contains(strVal.toLowerCase()))
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
							if (("$lte".equals(key) && objectInt <= filterInt) ||
									("$lt".equals(key) && objectInt < filterInt) ||
									("$gte".equals(key) && objectInt >= filterInt) ||
									("$gt".equals(key) && objectInt > filterInt)) {
								hasValue = true;
								thisRun = true;
								reasonMap.put(key, ldf.format(filterInt));
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
							if (("$lte".equals(key) && objectInt <= filterInt) ||
									("$lt".equals(key) && objectInt < filterInt) ||
									("$gte".equals(key) && objectInt >= filterInt) ||
									("$gt".equals(key) && objectInt > filterInt))
							{
								hasValue = true;
								reasonMap.put(key, ldf.format(filterInt));
							}

							if (hasValue) 
								passed++;
						}
//						else
//							logger.warn("Could not find check value @ " + inboundKey);
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
				if (filterCheck(filter, key, FilterMode.AND))
					passed++;
			}
			else
				logger.warn("Unknown object type [" + key + "]: " + obj.getClass().getName());
		}
		
		boolean ret = false;
		if (mode.equals(FilterMode.AND))
		{
			// AND query
			ret = passed == expectedPass;
		}
		else
		{
			// OR query
			ret = passed > 0;
		}
		if (ret && reason != null)
		{
			// process reason
			StringBuffer buf = new StringBuffer();
			int startIdx = 0;
			Iterator<String> rit = reasonMap.keySet().iterator();
			while (rit.hasNext()) {
				String key = rit.next();
				
				int keyIdx = reason.indexOf(key);
				if (keyIdx > -1) {

					buf.append(reason.substring(startIdx, keyIdx));
					
					// resolve key
					String kv = reasonMap.get(key);
					buf.append(kv);
					
					startIdx = keyIdx + key.length();
				}
				
			}
			
			if (startIdx < reason.length())
				buf.append(reason.substring(startIdx));
			
			addReason(buf.toString());
		}
		return ret;
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
