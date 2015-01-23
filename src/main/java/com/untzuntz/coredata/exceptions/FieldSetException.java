package com.untzuntz.coredata.exceptions;

/**
 * Thrown when the ORM cannot set a field for some reason
 * 
 * @author jdanner
 *
 */
public class FieldSetException extends Exception {

	private static final long serialVersionUID = 6837261594058699706L;
	
	private String setterName;
	private Exception parentException;
	
	public FieldSetException(String setterName, Exception excep) {
		super(excep);
		
		this.parentException = excep;
		this.setterName = setterName;
	}

	public String getSetterName() {
		return setterName;
	}

	public Exception getParentException() {
		return parentException;
	}
	
}
