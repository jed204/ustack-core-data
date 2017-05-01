package com.untzuntz.coredata;

import org.apache.log4j.Logger;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Generic Paging Support and information related to SQL records
 * 
 * @author jdanner
 *
 */
public class PagingSupport {

	static 		Logger           	logger                  = Logger.getLogger(PagingSupport.class);
	
	private Long total;
	private Integer page;
	private Integer itemsPerPage;
	private boolean isNoCount;
	
	public PagingSupport() {
		this(null, null);
	}
	
	public PagingSupport(Integer page, Integer itemsPerPage)
	{
		if (this.page != null && this.page < 1)
			throw new IllegalArgumentException("Page must be greater than 0");
		
		this.page = page;
		this.itemsPerPage = itemsPerPage;
	}
	
	public boolean isNoCount() {
		return isNoCount;
	}
	
	public void setNoCount(boolean b) {
		isNoCount = b;
	}
	
	public Long getTotal() {
		return total;
	}
	
	public void setTotal(Long total) {
		this.total = total;
	}
	
	public void setItemsPerPage(Integer ipp) {
		itemsPerPage = ipp;
	}
	
	public Integer getItemsPerPage() {
		if (itemsPerPage == null)
			return 10;
		return itemsPerPage;
	}
	
	public Integer getPage() {
		if (page == null)
			return 1;
		return page;
	}
	
	public Integer getTotalPages() {
		if (itemsPerPage == null)
			return null;
		
		return (int)(total / itemsPerPage) + (total % itemsPerPage > 0 ? 1 : 0);
	}
	
	public DBObject toDBObject() {
		
		DBObject ret = new BasicDBObject();
		
		DBObject countInfo = new BasicDBObject();
		
		if (page != null)
			countInfo.put("page", page);

		if (total != null) {
			Integer totalPages = getTotalPages();
			countInfo.put("items", total);
			if (totalPages != null)
				countInfo.put("pages", totalPages);
		}
		
		ret.put("paging", countInfo);
		
		return ret;
		
	}
	
}
