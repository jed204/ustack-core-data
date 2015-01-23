package com.untzuntz.coredata.exceptions;

import com.untzuntz.ustackserverapi.APIExceptionDocumentation;

/**
 * We have been provided a class that doesn't appear to have a @DBPrimaryKey field
 * @author jdanner
 *
 */
public class UnknownPrimaryKeyException extends APIDatabaseException implements APIExceptionDocumentation {

	private static final long serialVersionUID = 1L;

	private String className;
	
	public UnknownPrimaryKeyException(Class<?> inClass)
	{
		super("Cannot find primary key for " + inClass.getName());
		className = inClass.getName();
	}

	public String getClassName() {
		return className;
	}	

	public String getReason() {
		return "The internal code is not properly setup for this call.";
	}

}
