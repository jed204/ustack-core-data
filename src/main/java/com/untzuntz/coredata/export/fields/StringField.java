package com.untzuntz.coredata.export.fields;

import com.mongodb.DBObject;
import com.untzuntz.coredata.export.ExportFieldFormat;

public class StringField implements ExportFieldFormat {

	private String fieldName;
	private String headerName;
	private String subField;

	public StringField(String fieldName) {
		this.fieldName = fieldName;
		this.headerName = "";
	}
	
	public StringField(String headerName, String fieldName) {
		this.fieldName = fieldName;
		this.headerName = headerName;
	}
	
	public StringField(String headerName, String fieldName, String subField) {
		this.fieldName = fieldName;
		this.headerName = headerName;
		this.subField = subField;
	}
	
	public String getSubField() {
		return subField;
	}

	public void setSubField(String subField) {
		this.subField = subField;
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
		return null;
	}
	
	public String getHeaderName() {
		return headerName;
	}
	
	public void calculateLineCount(Object data) {}
	
	public int getLineCount() {
		return 1;
	}
	
	@Override
	public String getFieldValue(Object data, int outputLine) {
		
		if (data == null)
			return "";
		
		if (data instanceof DBObject)
		{
			DBObject obj = (DBObject)data;
			if (obj != null)
			{
				if (this.subField != null)
				{
					DBObject sub = (DBObject)obj.get(fieldName);
					if (sub != null && sub.get(subField) != null)
						return sub.get(subField) + "";
					else
						data = "";
				}
				else if (obj.get(fieldName) != null)
					data = obj.get(fieldName) + "";
				else
					data = "";
			}
		}
		
		return data.toString();
	}

}
