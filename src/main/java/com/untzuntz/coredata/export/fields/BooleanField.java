package com.untzuntz.coredata.export.fields;

import com.mongodb.DBObject;
import com.untzuntz.coredata.export.ExportFieldFormat;

public class BooleanField  implements ExportFieldFormat 
{
	private String fieldName;
	private String headerName;
	
	public BooleanField(String fieldName) 
	{
		this.fieldName = fieldName;
		this.headerName = "";
	}
	
	public BooleanField(String headerName, String fieldName) 
	{
		this.fieldName = fieldName;
		this.headerName = headerName;
	}

	public String getFieldName() {
		return fieldName;
	}
	
	@Override
	public String getHeaderName() 
	{
		return headerName;
	}

	@Override
	public String getFieldFormat() 
	{
		return null;
	}

	@Override
	public String getForegroundColor() 
	{
		return null;
	}

	@Override
	public String getBackgroundColor() 
	{
		return null;
	}

	@Override
	public void calculateLineCount(Object data) {}

	@Override
	public int getLineCount() 
	{
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
				return "No";
			else if (obj instanceof java.lang.Boolean)
			{
				if (((Boolean)obj))
					return "Yes";
				else
					return "No";
			}
			
			return obj.toString();
		}
		
		return data.toString();
	}
	
}
