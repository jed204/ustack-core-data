package com.untzuntz.coredata.anno;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.untzuntz.coredata.DataMgr;

/**
 * Indicates the method on the class to call when a study has been added
 * 
 * Method definitions:
 * 
 * addItemMethodName -> method(String actor, Object item)
 * removeItemMethodName -> method(String actor, Integer itemIndex)
 * replaceItemMethodName -> method(String actor, Integer itemIdex, Object item)
 * 
 * Note: Object can be the generic class from the List
 * 
 * @author jdanner
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface CrudActions {

	String addItemMethodName() default DataMgr.IGNORE;
	String removeItemMethodName() default DataMgr.IGNORE;
	String replaceItemMethodName() default DataMgr.IGNORE;
	
}
