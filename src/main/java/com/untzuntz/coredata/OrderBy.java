package com.untzuntz.coredata;

/**
 * Representation of an ORDER BY statement in SQL
 * 
 * @author jdanner
 *
 */
public class OrderBy {
	
	private String fieldName;
	private Direction dir;
	
	public OrderBy() {
		// do nothing
	}
	
	public OrderBy(String fieldName) {
		this(fieldName, Direction.Asc);
	}
	
	public OrderBy(String fieldName, Direction d) {
		setFieldName(fieldName);
		setDirection(d);
	}
	
	public void setFieldName(String f) {
		fieldName = f;
	}
	
	public String getFieldName() {
		return fieldName;
	}
	
	public void setDirection(Direction d) {
		dir = d;
	}
	
	public Direction getDirection() {
		if (dir == null)
			return Direction.Asc;
		
		return dir;
	}
	
	public static enum Direction {
		
		Desc(-1),
		Asc(1);
	
		private int orderInt;
		private Direction(int o) {
			orderInt = o;
		}
		
		public int getOrderInt() {
			return orderInt;
		}
		
	}
	
	
}
