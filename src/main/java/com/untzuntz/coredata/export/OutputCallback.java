package com.untzuntz.coredata.export;


public interface OutputCallback {
	public void startLine() throws Exception;
	public void nextField() throws Exception;
	public void writeField(Object fieldValue, ExportFieldFormat format) throws Exception;
	public void nextLine() throws Exception;
}
