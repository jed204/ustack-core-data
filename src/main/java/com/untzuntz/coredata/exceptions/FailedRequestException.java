package com.untzuntz.coredata.exceptions;

import com.untzuntz.ustackserverapi.APIException;
import com.untzuntz.ustackserverapi.APIExceptionDocumentation;

public class FailedRequestException extends APIException implements APIExceptionDocumentation {
	
	private static final long serialVersionUID = 1L;
	private String message;
	private String exceptionType;
	
	public FailedRequestException(String message)
	{
		super(message);
		this.message = message;
	}

	public FailedRequestException(Exception e)
	{
		super(e.getMessage());
		
		exceptionType = e.getClass().getSimpleName();
		message = e.getMessage();
	}

	public String getMessage() {
		return message;
	}

	public String getExceptionType() {
		return exceptionType;
	}
	
	public String getReason() {
		return "Your request was invalid and cannot be processed.";
	}

}
