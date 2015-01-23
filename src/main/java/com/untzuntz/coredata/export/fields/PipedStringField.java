package com.untzuntz.coredata.export.fields;

import com.mongodb.BasicDBList;
import com.mongodb.DBObject;
import com.untzuntz.coredata.export.ExportFieldFormat;

public class PipedStringField implements ExportFieldFormat {

	private String fieldName;
	private String headerName;
	private String subField;

	public PipedStringField(String fieldName) {
		this.fieldName = fieldName;
		this.headerName = "";
	}
	
	public PipedStringField(String headerName, String fieldName) {
		this.fieldName = fieldName;
		this.headerName = headerName;
	}
	
	public PipedStringField(String headerName, String fieldName, String subField) {
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
		
		String[] vals = new String[0];
		BasicDBList list = null;
		if (data instanceof DBObject)
		{
			DBObject obj = (DBObject)data;
			if (obj != null)
			{
				if (this.subField != null)
				{
					DBObject sub = (DBObject)obj.get(fieldName);
					if (sub != null && sub.get(subField) != null)
						list = (BasicDBList)sub.get(subField);
				}
				else if (obj.get(fieldName) != null)
				{
					list = (BasicDBList)obj.get(fieldName);
				}
			}
		}
		
		StringBuffer ret = new StringBuffer();
		if (list != null)
		{
			for (int i = 0; i < list.size(); i++)
			{
				String v = (String)list.get(i);
				ret.append(v);
				if ((i + 1) < list.size())
					ret.append("|");
			}
		}
		else
		{
			for (int i = 0; i < vals.length; i++)
			{
				ret.append(vals[i]);
				if ((i + 1) < vals.length)
					ret.append("|");
			}
		}
		
		return ret.toString();
	}

}
