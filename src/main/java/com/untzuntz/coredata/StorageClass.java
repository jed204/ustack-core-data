package com.untzuntz.coredata;

/**
 * Mainly used for MogileFS - this is the 'storage class' for a file or piece of data
 * 
 * @author jdanner
 *
 */
public enum StorageClass {

	Dicom("dicom"),
	Files("files");
	
	private String storageClassName;
	private StorageClass(String storageClassName)
	{
		this.storageClassName = storageClassName;
	}
	
	@Override
	public String toString() {
		return storageClassName;
	}

	
}
