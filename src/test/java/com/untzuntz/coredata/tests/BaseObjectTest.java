package com.untzuntz.coredata.tests;

import com.untzuntz.coredata.anno.DBPrimaryKey;
import com.untzuntz.coredata.anno.DBTableMap;

@DBTableMap ( dbTable = "TEST_TABLE", dbTableAlias = "testBase" )
public class BaseObjectTest {
	@DBPrimaryKey ( dbColumn = "STUDY_ID" )
	Long id;
	String hi;
	public Long getId() {
		return id;
	}
	public void setId(Long id) {
		this.id = id;
	}
	public String getHi() {
		return hi;
	}
	public void setHi(String hi) {
		this.hi = hi;
	}
	
}
