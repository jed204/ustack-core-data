package com.untzuntz.coredata;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.untzuntz.ustackserverapi.params.types.util.DateRange;

/**
 * A holding place for one or more additional search parameters
 * 
 * Use case:
 * 
 *    - you want to provide some optional filters on an existing query
 *    
 * 
 * @author jdanner
 *
 */
public class SearchFilters {

    static Logger           		logger               	= Logger.getLogger(SearchFilters.class);

	private List<FilterEntry> filters;
	
	public SearchFilters()
	{
		filters = new ArrayList<FilterEntry>();
	}
	
	public SearchFilters add(Class<?> typeClass, String fieldName, FilterType ft, Object value)
	{
		return add(DataMgr.resolveSQLField(typeClass, fieldName), ft, value);
	}
	
	public SearchFilters add(String sqlField, FilterType ft, Object value)
	{
		filters.add(new FilterEntry(sqlField, ft, value));
		return this;
	}
	
	public List<FilterEntry> getParams() {
		return filters;
	}
	
	/**
	 * Reset all filters
	 */
	public void clear()
	{
		filters.clear();
	}
	
	/**
	 * Add the search parameters to the SQL Builder and the list of parameters
	 * 
	 * @param sqlBuilder
	 * @param paramList
	 */
	public void process(SQLBuilder sqlBuilder, List<Object> paramList)
	{
		for (FilterEntry filter : filters)
		{
			StringBuffer sql = new StringBuffer();
			
			Object value = filter.getValue();
			if (value instanceof DateRange)
			{	
				DateRange dr = (DateRange)value;
				Date start = dr.getStart();
				Date end = dr.getEnd();
				if (start != null && end == null)
				{
					sql.append(filter.getSqlField());
					sql.append(FilterType.EqualsGreaterThan.getFilterOperator());			
					sql.append("?");
					sqlBuilder.andWhere( sql.toString() );
					paramList.add(start);
				}
				else if (start == null && end != null)
				{
					sql.append(filter.getSqlField());
					sql.append(FilterType.EqualsLessThan.getFilterOperator());			
					sql.append("?");
					sqlBuilder.andWhere( sql.toString() );
					paramList.add(start);
				}
				else if (start != null && end != null)
				{
					sql.append(filter.getSqlField());
					sql.append(FilterType.EqualsGreaterThan.getFilterOperator());			
					sql.append("?");
					sqlBuilder.andWhere( sql.toString() );
					paramList.add(start);

					sql = new StringBuffer();
					sql.append(filter.getSqlField());
					sql.append(FilterType.EqualsLessThan.getFilterOperator());			
					sql.append("?");
					sqlBuilder.andWhere( sql.toString() );
					paramList.add(end);
				}
			}
			else 
			{  
				
				sql.append(filter.getSqlField());
	
				sql.append(filter.getType().getFilterOperator());			
				
				if (!filter.getType().isNoValue())
				{
					if (filter.getType().isLike())
						filter.likeValue();
					
					if (filter.getType().isStartsWith())
						filter.startsWithValue();
					
					sql.append("?");
				}
				
				sqlBuilder.andWhere( sql.toString() );
				paramList.add(filter.getValue());
			}
		}
	}
	
	/**
	 * Returns a set of fields summarizing the values to be searched
	 * @return
	 */
	public BasicDBObject getSearchSummary()
	{
		BasicDBObject search = new BasicDBObject();
		
		for (FilterEntry filter : filters)
		{
			//logger.debug(String.format("%s [%s] %s", filter.getSqlField(), filter.getType().name(), filter.getValue()));
			Object value = filter.getValue();
			
			if (value instanceof DateRange)
				search.put(filter.getSqlField(), new BasicDBObject("type", "DateRange").append("val", ((DateRange)value).toString()));
			else if (value != null)
				search.put(filter.getSqlField(), new BasicDBObject("type", filter.getType().name()).append("val", value.toString()));
		}
		
		return search;
	}
	
	/**
	 * Returns the MongoDB Search object based on the filter parameters
	 * 
	 * @return
	 */
	public DBObject getMongoSearchObject()
	{
		DBObject search = new BasicDBObject();
		for (FilterEntry filter : filters)
		{
			logger.info(String.format("%s [%s] %s", filter.getSqlField(), filter.getType().name(), filter.getValue()));
			Object value = filter.getValue();
			
			if (value instanceof DateRange)
			{	
				DateRange dr = (DateRange)value;
				Date start = dr.getStart();
				Date end = dr.getEnd();
				if (start != null && end == null)
					search.put(filter.getSqlField(), new BasicDBObject("$gte", start));
				else if (start == null && end != null)
					search.put(filter.getSqlField(), new BasicDBObject("$lte", end));
				else if (start != null && end != null)
					search.put(filter.getSqlField(), new BasicDBObject("$gte", start).append("$lte", end));
			}
			else if (value != null)
			{
				String[] values = null;
				Pattern[] fValues = null;
				switch (filter.getType())
				{
					case Like:
						search.put(filter.getSqlField(), Pattern.compile(".*" + value.toString() + ".*", Pattern.CASE_INSENSITIVE));
						break;
					case LikeCaseSensitive:
						search.put(filter.getSqlField(), Pattern.compile(".*" + value.toString() + ".*"));
						break;
					case StartsWith:
						search.put(filter.getSqlField(), Pattern.compile("^" + value.toString()));
						break;
					case NotLike:
						search.put(filter.getSqlField(), new BasicDBObject("$not", Pattern.compile(".*" + value.toString() + ".*", Pattern.CASE_INSENSITIVE)));
						break;
					case In:
						search.put(filter.getSqlField(), new BasicDBObject("$in", value));
						break;
					case InLike:
						values = (String[])value;
						fValues = new Pattern[values.length];
						for (int q = 0; q < fValues.length; q++) {
							fValues[q] = Pattern.compile(".*" + values[q].trim() + ".*", Pattern.CASE_INSENSITIVE);
						}
						search.put(filter.getSqlField(), new BasicDBObject("$in", fValues));
						break;
					case All:
						search.put(filter.getSqlField(), new BasicDBObject("$all", value));
						break;
					case AllLike:
						values = (String[])value;
						fValues = new Pattern[values.length];
						for (int q = 0; q < fValues.length; q++) {
							fValues[q] = Pattern.compile(".*" + values[q].trim() + ".*", Pattern.CASE_INSENSITIVE);
						}
						search.put(filter.getSqlField(), new BasicDBObject("$all", fValues));
						break;
					case NotEquals:
						search.put(filter.getSqlField(), new BasicDBObject("$ne", value));
						break;
					case Equals:
						search.put(filter.getSqlField(), value);
						break;
					case EqualsLessThan:
						search.put(filter.getSqlField(), new BasicDBObject("$lte", value));
						break;
					case EqualsGreaterThan:
						search.put(filter.getSqlField(), new BasicDBObject("$gte", value));
						break;
					case LessThan:
						search.put(filter.getSqlField(), new BasicDBObject("$lt", value));
						break;
					case GreaterThan:
						search.put(filter.getSqlField(), new BasicDBObject("$gt", value));
						break;
					case Exists:
						search.put(filter.getSqlField(), new BasicDBObject("$exists", true));
						break;
					case NotExists:
						search.put(filter.getSqlField(), new BasicDBObject("$exists", false));
						break;
				}
			}
		}
		return search;
	}
	
	public class FilterEntry {
		
		private String sqlField;
		private FilterType type;
		private Object value;
		
		public FilterEntry(String sqlField, FilterType type, Object value)
		{
			this.sqlField = sqlField;
			this.type = type;
			this.value = value;
		}

		public String getSqlField() {
			return sqlField;
		}

		public FilterType getType() {
			return type;
		}

		public Object getValue() {
			return value;
		}
		
		public void startsWithValue() {
			if (value == null)
				value = "%";
			else
				value = value.toString() + "%";				
		}
		
		public void likeValue() {
			if (value == null)
				value = "%";
			else
				value = "%" + value.toString() + "%";
		}
	}
	
	public static enum FilterType {
		Like,
		LikeCaseSensitive,
		StartsWith,
		NotLike,
		NotEquals,
		Equals,
		EqualsLessThan,
		EqualsGreaterThan,
		LessThan,
		GreaterThan,
		In,
		InLike,
		All,
		AllLike,
		Exists,
		NotExists;
		
		public boolean isNoValue() {
			if (this.equals(Exists))
				return true;
			else if (this.equals(NotExists))
				return true;
			
			return false;
		}
		
		public boolean isStartsWith() {
			if (this.equals(StartsWith))
				return true;
			
			return false;
		}
		
		public boolean isLike()
		{
			if (this.equals(Like))
				return true;
			else if (this.equals(NotLike))
				return true;
			
			return false;
		}
		
		public String getFilterOperator()
		{
			switch (this)
			{
				case Equals:
					return " = ";
				case EqualsGreaterThan:
					return " >= ";
				case EqualsLessThan:
					return " <= ";
				case GreaterThan:
					return " > ";
				case LessThan:
					return " < ";
				case StartsWith:
					return " LIKE ";
				case Like:
					return " LIKE ";
				case LikeCaseSensitive:
					return " LIKE ";
				case NotLike:
					return " NOT LIKE ";
				case NotEquals:
					return " <> ";
				case Exists:
					return " NOT NULL ";
				case NotExists:
					return " IS NULL ";
				default:
					return null;
			}
		}
	}
}
