package com.untzuntz.coredata;

import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.untzuntz.coredata.exceptions.APIDatabaseException;

/**
 * Some centralized logging calls
 * 
 * @author jdanner
 *
 */
public class LoggingUtil {

	private static Logger logger = Logger.getLogger(LoggingUtil.class);

	/**
	 * Centralized SQL Exception logging (called from APIDatabaseException)
	 * 
	 * @param er
	 */
	public static void sqlError(SQLException er)
	{
		logger.warn(String.format("SQL Exception encountered [%d] [State: %s]", er.getErrorCode(), er.getSQLState()), er);
	}
	
	/**
	 * Centralized SQL Exception logging (called from APIDatabaseException)
	 * 
	 * @param er
	 */
	public static void sqlError(String er, APIDatabaseException dbe)
	{
		logger.warn(String.format("SQL Exception encountered [Message: %s]", er), dbe);
	}
	
}
