package de.soderer.utilities;

import java.util.List;

public class DatabaseIndex {
	private String tableName;
	private String indexName;
	private List<String> indexedColumns;
	
	public DatabaseIndex(String tableName, String indexName, List<String> indexedColumns) {
		this.tableName = tableName;
		this.indexName = indexName;
		this.indexedColumns = indexedColumns;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getIndexName() {
		return indexName;
	}

	public void setIndexName(String indexName) {
		this.indexName = indexName;
	}

	public List<String> getIndexedColumns() {
		return indexedColumns;
	}

	public void setIndexedColumns(List<String> indexedColumns) {
		this.indexedColumns = indexedColumns;
	}
	
	@Override
	public String toString() {
		return tableName + " " + indexName + " (" + Utilities.join(indexedColumns, ", ") + ")";
	}
}
