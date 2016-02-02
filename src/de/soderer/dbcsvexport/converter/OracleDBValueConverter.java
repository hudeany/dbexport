package de.soderer.dbcsvexport.converter;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;

import de.soderer.utilities.DbUtilities;

public class OracleDBValueConverter extends DefaultDBValueConverter {
	public OracleDBValueConverter(boolean zip, boolean createBlobFiles, boolean createClobFiles, String fileExtension) {
		super(zip, createBlobFiles, createClobFiles, fileExtension);
	}
	
	@Override
	public Object convert(ResultSet resultSet, int columnIndex, String outputFilePath) throws Exception {
		ResultSetMetaData metaData = resultSet.getMetaData();
		Object value;
		if (metaData.getColumnType(columnIndex) == DbUtilities.ORACLE_TIMESTAMPTZ_TYPECODE
				|| metaData.getColumnType(columnIndex) == Types.TIMESTAMP) {
			value = resultSet.getTimestamp(columnIndex);
			if (resultSet.wasNull()) {
				value = null;
			}
		} else {
			value = super.convert(resultSet, columnIndex, outputFilePath);
		}
		return value;
	}
}
