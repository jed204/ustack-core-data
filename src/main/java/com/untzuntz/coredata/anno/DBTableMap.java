package com.untzuntz.coredata.anno;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.untzuntz.coredata.DataMgr;

/**
 * This interface is used to capture annotations on a data class. 
 * 
 * It allows the user to define a database table name and an alias for a particular class
 * 
 * @author jdanner
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface DBTableMap {

	String dbDatabase() default DataMgr.IGNORE;
	String dbTable();
	String dbTableAlias() default DataMgr.IGNORE;
	boolean dbMongo() default false;
	boolean includeParent() default false;

}
