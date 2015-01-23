package com.untzuntz.coredata;

import com.untzuntz.coredata.anno.DBFieldMap;

public class MultiKeyBase {

	@DBFieldMap(dbIgnore=true)
	private boolean newFlag;
	
	public void markNew() {
		newFlag = true;
	}
	public boolean isNew() {
		return newFlag;
	}
	public void markSaved() {
		newFlag = false;
	}
	
}
