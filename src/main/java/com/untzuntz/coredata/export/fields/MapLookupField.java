package com.untzuntz.coredata.export.fields;

import java.util.Map;

import com.mongodb.DBObject;
import com.untzuntz.coredata.BaseData;
import com.untzuntz.coredata.export.ExportFieldFormat;

public class MapLookupField implements ExportFieldFormat {

	private String lookupFieldName;
	private ExportFieldFormat format;
	private Map values;
	
	public MapLookupField(String lookupFieldName, ExportFieldFormat fmt, Map values) {
		this.lookupFieldName = lookupFieldName;
		this.format = fmt;
		this.values = values;
	}

	public String getForegroundColor() {
		return format.getForegroundColor();
	}
	
	public String getBackgroundColor() {
		return format.getBackgroundColor();
	}
	
	public String getFieldFormat() {
		return format.getFieldFormat();
	}
	
	public String getHeaderName() {
		return format.getHeaderName();
	}
	
	public void calculateLineCount(Object data) {
		format.calculateLineCount(data);
	}
	
	public int getLineCount() {
		return format.getLineCount();
	}
	
	@Override
	public String getFieldValue(Object data, int outputLine) {
		
		if (data == null)
			return "";

		if (data instanceof DBObject)
		{
			DBObject obj = (DBObject)data;
			String lookupValue = (String)obj.get(lookupFieldName);
			Object o = (Object)values.get(lookupValue);
			if (o != null)
				data = format.getFieldValue(((BaseData)o).toDBObject(), outputLine);
			else 
				data = "";
		}
		
		return data.toString();
	}

	@Override
	public String getFieldName() {
		return format.getFieldName();
	}
}
