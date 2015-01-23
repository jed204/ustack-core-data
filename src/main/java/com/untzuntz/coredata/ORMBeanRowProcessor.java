package com.untzuntz.coredata;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.dbutils.BasicRowProcessor;

/**
 * Processes a row of results in a SQL ResultSet
 * 
 * @author jdanner
 *
 */
public class ORMBeanRowProcessor extends BasicRowProcessor {

    /**
     * The default BeanProcessor instance to use if not supplied in the
     * constructor.
     */
    private static final ORMBeanProcessor defaultConvert = new ORMBeanProcessor();
	
    private final ORMBeanProcessor convert;
    
    // Cache this info per instance of the RowProcessor to avoid invalid caching across different query types
    private HashMap<Class,List<Field>> fieldToLoadCache;
    
    public ORMBeanRowProcessor() {
        this(defaultConvert);
    }

    public ORMBeanRowProcessor(ORMBeanProcessor bp) {
        super();
        this.convert = bp;
        this.fieldToLoadCache = new HashMap<Class,List<Field>>();
    }

    /**
     * Convert a ResultSet to a Bean
     */
    public <T> T toBean(ResultSet rs, Class<T> type) throws SQLException {
    	return this.convert.toBean(rs, type, fieldToLoadCache, null, null);
    }

    /**
     * Convert a ResultSet to a Bean list
     */
	public <T> List<T> toBeanList(ResultSet rs, Class<T> type)
			throws SQLException {
		return this.convert.toBeanList(rs, type, fieldToLoadCache);
	}

    

}
