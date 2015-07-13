package com.untzuntz.coredata.tests;

import com.untzuntz.coredata.anno.DBTableMap;

@DBTableMap ( dbTable = "TEST_TABLE", dbTableAlias = "testExt", includeParent = true )
public class ExtendedObjectTest extends BaseObjectTest {
	String mom;

	public String getMom() {
		return mom;
	}

	public void setMom(String mom) {
		this.mom = mom;
	}
	
}
