package com.untzuntz.coredata.exceptions;

import java.sql.SQLException;

import com.untzuntz.coredata.LoggingUtil;
import com.untzuntz.ustackserverapi.APIException;
import com.untzuntz.ustackserverapi.APIExceptionDocumentation;

/**
 * Captures Database-related exceptions
 * 
 * @author jdanner
 *
 */
public class APIDatabaseException extends APIException implements APIExceptionDocumentation {

	private static final long serialVersionUID = 1L;
	private SQLException sqlExcep;
	private String message;
	
	/**
	 * Do not call this one - call the constructor with just the message or provide the SQL 
	 * @param msg
	 * @param testMode
	 */
	public APIDatabaseException(String msg, boolean testMode) {
		super(msg);
		message = "Database Error";
		
		if (!testMode)
			LoggingUtil.sqlError(msg, this);
	}
	
	public APIDatabaseException(String msg) {
		super(msg);
		message = "Database Error";
		
		LoggingUtil.sqlError(msg, this);
	}
	
	public APIDatabaseException(SQLException e) {
		super("Database Error");
		sqlExcep = e;
		message = "Database Error";

		// log this error
		LoggingUtil.sqlError(e);
	}
	
	public String getMessage() {
		return message;
	}
	
	public SQLException getSQLException() {
		return sqlExcep;
	}
	
	public String getReason() {
		return "A general database error has occurred.";
	}

}
