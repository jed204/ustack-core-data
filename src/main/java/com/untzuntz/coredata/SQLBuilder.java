package com.untzuntz.coredata;

import java.util.ArrayList;
import java.util.List;

import com.untzuntz.coredata.anno.DBTableMap;
import com.untzuntz.coredata.OrderBy.Direction;

/**
 * This class is used to generate a MySQL statement
 * 
 * It's currently basic as it only allows for simple joins and mostly AND where parameters
 * 
 * @author jdanner
 *
 */
public class SQLBuilder {

	private List<Class> select;
	private List<Class> from;
	private List<String> where;
	private OrderBy orderBy;
	private String groupBy;
	private Integer page;
	private Integer itemsPerPage;
	
	public SQLBuilder()
	{
		select = new ArrayList<Class>();
		from = new ArrayList<Class>();
		where = new ArrayList<String>();
	}
	
	public SQLBuilder addSelect(Class tbl)
	{
		select.add(tbl);
		return this;
	}
	
	public SQLBuilder addFrom(Class tbl)
	{
		from.add(tbl);
		return this;
	}
	
	public SQLBuilder andWhere(String sql) {
		where.add(sql);
		return this;
	}
	
	public SQLBuilder orderBy(OrderBy o) {
		orderBy = o;
		return this;
	}
	
	public SQLBuilder groupBy(String g) { 
		groupBy = g;
		return this;
	}
	
	public SQLBuilder paging(PagingSupport paging) {
		if (paging == null)
			return this;
		
		this.page = paging.getPage();
		this.itemsPerPage = paging.getItemsPerPage();
		return this;
	}
	
	public SQLBuilder page(Integer pg) {
		this.page = pg;
		return this;
	}
	
	public SQLBuilder itemsPerPage(Integer ipp) {
		itemsPerPage = ipp;
		return this;
	}
	
	public String count() {
		return count(null);
	}
	
	public String count(String opt) {

		StringBuffer buf = new StringBuffer();
		
		buf.append("SELECT\n");
		
		buf.append("\tCOUNT(");
		if (opt == null || opt.length() == 0)
			buf.append("*");
		else
			buf.append(opt);
		buf.append(")");
		
		getFromWhere(buf);
		
		return buf.toString();
	}
	
	public String select() {
		
		StringBuffer buf = new StringBuffer();
		
		buf.append("SELECT\n");
		
		int selectSize = select.size();
		for (int i = 0; i < selectSize; i++)
		{
			buf.append("\t").append(DataMgr.getDBtoJavaMap( select.get(i) ) );
			if ((i + 1) < selectSize)
				buf.append(",\n");
		}
		
		getFromWhere(buf);

		if (groupBy != null)
		{
			buf.append("\nGROUP BY\n\t");
			buf.append(groupBy);
		}
		
		if (orderBy != null)
		{
			buf.append("\nORDER BY\n\t");
			buf.append(orderBy.getFieldName());
			if (orderBy.getDirection().equals(Direction.Desc))
				buf.append(" DESC");
			else
				buf.append(" ASC");
		}
		
		if (itemsPerPage != null)
		{
			buf.append("\nLIMIT\n\t");
			
			if (page != null && page >= 1)
				buf.append((page - 1) * itemsPerPage).append(",");
			
			buf.append(itemsPerPage);
		}

		return buf.toString();
	}
	
	private void getFromWhere(StringBuffer buf)
	{
		buf.append("\nFROM\n");
		
		int fromSize = from.size();
		for (int i = 0; i < fromSize; i++)
		{
			Class fromCls = from.get(i);
			@SuppressWarnings("unchecked")
			DBTableMap tbl = (DBTableMap)fromCls.getAnnotation(DBTableMap.class);
			
			buf.append("\t").append(tbl.dbTable());
			
			if (tbl != null)
			{
				buf.append(" AS ");
				buf.append(tbl.dbTableAlias());
			}

			if ((i + 1) < fromSize)
				buf.append(",\n");

		}
		
		int whereSize = where.size();
		if (whereSize == 0)
			return;
		
		buf.append("\nWHERE\n");
		
		for (int i = 0; i < whereSize; i++)
		{
			buf.append("\t").append(where.get(i));
			if ((i + 1) < whereSize)
				buf.append(" AND\n");
		}
		
	}
	
	public static SQLBuilder from(Class tbl)
	{
		SQLBuilder build = new SQLBuilder();
		build.addSelect(tbl);
		build.addFrom(tbl);
		return build;
	}
	
	public static SQLBuilder select(Class tbl)
	{
		SQLBuilder build = new SQLBuilder();
		build.addSelect(tbl);
		return build;
	}
	
}
