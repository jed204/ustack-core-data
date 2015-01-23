package com.untzuntz.coredata.exceptions;

import com.untzuntz.ustackserverapi.APIExceptionDocumentation;

/**
 * The caller passed in an invalid value for a lookup
 * 
 * @author jdanner
 *
 */
public class InvalidObjectIdException extends APIDatabaseException implements APIExceptionDocumentation {

	private static final long serialVersionUID = 1L;

	public InvalidObjectIdException(boolean testMode)
	{
		super("Invalid Object ID", testMode);
	}

	public InvalidObjectIdException()
	{
		super("Invalid Object ID");
	}
	
	public String getReason() {
		return "The provided Object ID is invalid or not provided.";
	}

}
