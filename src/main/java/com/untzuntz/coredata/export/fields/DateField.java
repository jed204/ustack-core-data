package com.untzuntz.coredata.export.fields;

import com.mongodb.DBObject;
import com.untzuntz.coredata.export.ExportFieldFormat;

public class DateField implements ExportFieldFormat {

	private String fieldName;
	private String headerName;
	private String dateFormat;

	public DateField(String fieldName, String dateFormat) {
		this.fieldName = fieldName;
		this.headerName = "";
		this.dateFormat = dateFormat;
	}
	
	public DateField(String headerName, String fieldName, String dateFormat) {
		this.fieldName = fieldName;
		this.headerName = headerName;
		this.dateFormat = dateFormat;
	}

	public String getFieldName() {
		return fieldName;
	}

	public String getForegroundColor() {
		return null;
	}
	
	public String getBackgroundColor() {
		return null;
	}
	
	public String getFieldFormat() {
		return dateFormat;
	}
	
	public String getHeaderName() {
		return headerName;
	}
	
	public void calculateLineCount(Object data) {}
	
	public int getLineCount() {
		return 1;
	}
	
	@Override
	public Object getFieldValue(Object data, int outputLine) {
		
		if (data == null)
			return "";
		
		if (data instanceof DBObject)
		{
			Object obj = ((DBObject)data).get(fieldName);
			if (obj == null)
				return "";
			
			return obj;
		}
		
		return data;
	}

}
