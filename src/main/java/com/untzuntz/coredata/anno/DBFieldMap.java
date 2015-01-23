package com.untzuntz.coredata.anno;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.untzuntz.coredata.DataMgr;

/**
 * This interface is used to capture annotations on a data class. 
 * 
 * It allows the user to define a database field name for a particular field otherwise the field name is used
 * 
 * @author jdanner
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface DBFieldMap {

	/**
	 * @return
	 */
	String dbColumn() default DataMgr.IGNORE;
	boolean dbIgnore() default false;
	boolean dbLoadListFromQuery() default false;
	
}
