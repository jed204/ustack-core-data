package com.untzuntz.coredata.anno;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.untzuntz.coredata.DataMgr;

/**
 * Denotes the primary key on a class/table
 * 
 * @author jdanner
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface DBPrimaryKey {

	/**
	 * @return
	 */
	String dbColumn();
	String dbMultiKeyColumn() default DataMgr.IGNORE;
	boolean dbAutoInc() default true;

}
