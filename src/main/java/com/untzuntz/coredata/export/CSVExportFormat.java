package com.untzuntz.coredata.export;

import java.io.OutputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.untzuntz.coredata.ExportFormat;
import com.untzuntz.coredata.exceptions.FailedRequestException;
import com.untzuntz.ustack.main.UFile;

public class CSVExportFormat implements ExportFormat,OutputCallback {

	private UFile outputFile;
	private OutputStream out;
	private List<ExportFieldFormat> fields;
	private HashMap<String,DateFormat> formats;
	
	/**
	 * Exports the data to a CSV File
	 * 
	 * @param fileTarget
	 */
	public CSVExportFormat(UFile outputFile)
	{
		this.outputFile = outputFile;
		this.fields = new ArrayList<ExportFieldFormat>();
		this.formats = new HashMap<String,DateFormat>();
	}
	
	public void addFields(List<ExportFieldFormat> newFields) {
		if (newFields == null)
			return;
		this.fields.addAll(newFields);
	}
	
	public void addField(ExportFieldFormat field) {
		if (field == null)
			return;
		fields.add(field);
	}

	@Override
	public void start() throws FailedRequestException {
		try {
			// open the output file
			out = outputFile.getOutputStream();
	
			// write the CSV header
			for (int fieldIdx = 0; fieldIdx < fields.size(); fieldIdx++)
			{
				ExportFieldFormat eff = fields.get(fieldIdx);
				writeField(eff.getHeaderName(), null);
				nextField();
			}
			
			nextLine();

		} catch (Exception e) {
			throw new FailedRequestException(e);
		}
	}

	@Override
	public void output(DBCursor cur) throws FailedRequestException {
		
		if (cur == null)
			return;
		
		try {
			// grab each object from the DBCursor
			while (cur.hasNext())
			{
				DBObject dbObject = cur.next();
				// send it to the multiline writer - we get callback to do the actual writing (see nextField, writeField, and nextLine)
				output(dbObject);
			}
		} catch (Exception e) {
			ExportUtil.close(out);
			throw new FailedRequestException(e);
		}
	}

	@Override
	public void output(DBObject dbObject) throws Exception {
		ExportUtil.writeMultiLine(this, fields, dbObject);
	}

	@Override
	public void finished() throws FailedRequestException {
		// close the output file
		ExportUtil.close(out);
	}

	@Override
	public void nextField() throws Exception {
		out.write(ExportUtil.COMMA);
	}

	@Override
	public void writeField(Object fieldValue, ExportFieldFormat format) throws Exception {
		out.write(ExportUtil.QUOTE);
		if (fieldValue instanceof Date)
			out.write( getFormat(format.getFieldFormat()).format(((Date)fieldValue)).getBytes() );
		else
			out.write(fieldValue.toString().getBytes());
		out.write(ExportUtil.QUOTE);
	}

	/**
	 * Cache the date formats
	 * 
	 * @param format
	 * @return
	 */
	private DateFormat getFormat(String format)
	{
		DateFormat fmt = formats.get(format);
		if (fmt == null)
		{
			fmt = new SimpleDateFormat(format);
			formats.put(format, fmt);
		}
		
		return fmt;
	}
	
	@Override
	public void startLine() throws Exception {
		// stub
	}

	@Override
	public void nextLine() throws Exception {
		out.write(ExportUtil.NEWLINE);
	}
	
}
