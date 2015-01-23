package com.untzuntz.coredata;

import org.bson.BasicBSONObject;

/**
 * This class will hold onto extra fields that may be in the mongoDB Object that the class has not specifically extended
 * 
 * @author jdanner
 *
 */
abstract public class MongoBaseData extends BaseData {

	private BasicBSONObject extraFields;
	
	public BasicBSONObject getExtraFields() {
		if (extraFields == null)
			extraFields = new BasicBSONObject();
		
		return extraFields;
	}
	
	public void setExtraFields(BasicBSONObject extras) {
		extraFields = extras;
	}
	
}
