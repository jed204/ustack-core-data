package com.untzuntz.coredata.export.fields;

import com.mongodb.BasicDBList;
import com.mongodb.DBObject;
import com.untzuntz.coredata.export.ExportFieldFormat;

public class DBListField implements ExportFieldFormat {

	private String fieldName;
	private String headerName;
	private ExportFieldFormat itemOutput;
	private int cachedLineCount;
	
	public DBListField(String headerName, String fieldName, ExportFieldFormat itemOutput) {
		this.fieldName = fieldName;
		this.headerName = headerName;
		this.itemOutput = itemOutput;
	}
	
	public String getFieldName() {
		return fieldName;
	}
	
	public String getHeaderName() {
		return headerName;
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
	
	public void calculateLineCount(Object data) {
		if (data instanceof DBObject)
		{
			DBObject pData = (DBObject)data;
			BasicDBList list = (BasicDBList)pData.get(fieldName);
			if (list != null)
				cachedLineCount = list.size();
		}
	}
	
	public int getLineCount() {
		return cachedLineCount;
	}
	
	@Override
	public Object getFieldValue(Object data, int outputLine) {
		
		if (data instanceof DBObject)
		{
			BasicDBList list = (BasicDBList)(((DBObject)data).get(fieldName));
			if (list != null && outputLine < list.size())
			{
				DBObject obj = (DBObject)list.get(outputLine);
				return itemOutput.getFieldValue(obj, outputLine);
			}
		}
		
		return "";
	}

}
