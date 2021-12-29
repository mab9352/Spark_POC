package com.virtualpairprogrammers;

public class IntegerWithSqaureRoots {

	private Integer value;
	
	private Double valueSqrRoot;
	
	
      
	public IntegerWithSqaureRoots(Integer value) {
		super();
		this.value = value;
		this.valueSqrRoot=Math.sqrt(value);
	}

	public Integer getValue() {
		return value;
	}

	public void setValue(Integer value) {
		this.value = value;
	}

	public Double getValueSqrRoot() {
		return valueSqrRoot;
	}

	public void setValueSqrRoot(Double valueSqrRoot) {
		this.valueSqrRoot = valueSqrRoot;
	}

	@Override
	public String toString() {
		return "IntegerWithSqaureRoots [value=" + value + ", valueSqrRoot=" + valueSqrRoot + "]";
	}
	
	
	

	
}
