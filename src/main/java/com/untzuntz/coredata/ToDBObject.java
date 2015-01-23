package com.untzuntz.coredata;

import com.mongodb.DBObject;

/**
 * Optional interface. If you data class implements this you will be able to adjust/extend/reduce the JSON sent to a client
 * 
 * @author jdanner
 *
 */
public interface ToDBObject {

	public void toDBObject(DBObject o);
	
}
