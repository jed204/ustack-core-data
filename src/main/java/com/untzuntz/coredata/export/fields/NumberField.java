package com.untzuntz.coredata.export.fields;

import com.mongodb.DBObject;
import com.untzuntz.coredata.export.ExportFieldFormat;

public class NumberField implements ExportFieldFormat {

	private String fieldName;
	private String headerName;
	private String numberFormat;

	public NumberField(String fieldName, String numberFormat) {
		this.fieldName = fieldName;
		this.headerName = "";
		this.numberFormat = numberFormat;
	}
	
	public NumberField(String headerName, String fieldName, String numberFormat) {
		this.fieldName = fieldName;
		this.headerName = headerName;
		this.numberFormat = numberFormat;
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
		return numberFormat;
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
