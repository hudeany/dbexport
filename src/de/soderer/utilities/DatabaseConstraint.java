package de.soderer.utilities;

public class DatabaseConstraint {
	public enum ConstraintType {
		PrimaryKey,
		Unique,
		ForeignKey,
		Check,
		Relation;

		public static ConstraintType fromName(String constraintTypeName) throws Exception {
			for (ConstraintType constraintType : ConstraintType.values()) {
				if (constraintType.name().replace("_", "").replace(" ", "").replace("-", "").equalsIgnoreCase(constraintTypeName.replace("_", "").replace(" ", "").replace("-", ""))) {
					return constraintType;
				} else if (constraintTypeName.length() == 1 && constraintType.name().replace("_", "").replace(" ", "").replace("-", "").charAt(0) == constraintTypeName.toUpperCase().charAt(0)) {
					return constraintType;
				}
			}
			throw new Exception("Unknown contraint type: '" + constraintTypeName + "'");
		}
	}

	private String tableName;
	private String constraintName;
	private ConstraintType constraintType;
	private String columnName;
	private String condition;

	public DatabaseConstraint(final String tableName, final String constraintName, final ConstraintType constraintType, String columnName, String condition) {
		this.tableName = tableName;
		this.constraintName = constraintName;
		this.constraintType = constraintType;
		this.columnName = columnName;
		this.condition = condition;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(final String tableName) {
		this.tableName = tableName;
	}

	public String getConstraintName() {
		return constraintName;
	}

	public void setConstraintName(final String constraintName) {
		this.constraintName = constraintName;
	}

	public ConstraintType getConstraintType() {
		return constraintType;
	}

	public void setConstraintType(final ConstraintType constraintType) {
		this.constraintType = constraintType;
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public String getCondition() {
		return condition;
	}

	public void setCondition(String condition) {
		this.condition = condition;
	}
	
	@Override
	public String toString() {
		return tableName + " " + constraintName + " " + constraintType + (columnName == null ? "" : " " + columnName) + (condition == null ? "" : " " + condition);
	}
}
