package com.untzuntz.coredata;

import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.untzuntz.coredata.exceptions.FailedRequestException;

/**
 * Defines a place for query results to be exported to
 * 
 * (could be a CSV, could be a Excel, maybe not even a file)
 * 
 * @author jdanner
 *
 */
public interface ExportFormat {

	/** Should be called at the beginning of an export */
	public void start() throws FailedRequestException;
	/** Exports the results in a DBCursor object */
	public void output(DBCursor cur) throws FailedRequestException;
	/** Exports the results in a DBObject */
	public void output(DBObject dbObject) throws Exception;
	/** Should be called after all data has been exported */
	public void finished() throws FailedRequestException;
	
}
