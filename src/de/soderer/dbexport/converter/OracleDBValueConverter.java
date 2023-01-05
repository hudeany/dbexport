package de.soderer.dbexport.converter;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;

import de.soderer.utilities.DbUtilities;

public class OracleDBValueConverter extends DefaultDBValueConverter {
	public OracleDBValueConverter(final boolean zip, final char[] zipPassword, final boolean useZipCrypto, final boolean createBlobFiles, final boolean createClobFiles, final String fileExtension) {
		super(zip, zipPassword, useZipCrypto, createBlobFiles, createClobFiles, fileExtension);
	}

	@Override
	public Object convert(final ResultSetMetaData metaData, final ResultSet resultSet, final int columnIndex, final String exportFilePath) throws Exception {
		Object value;
		final int columnTypeCode = metaData.getColumnType(columnIndex);
		if (columnTypeCode == DbUtilities.ORACLE_TIMESTAMPTZ_TYPECODE
				|| columnTypeCode == Types.TIMESTAMP) {
			value = resultSet.getTimestamp(columnIndex);
			if (resultSet.wasNull()) {
				value = null;
			}
		} else {
			value = super.convert(metaData, resultSet, columnIndex, exportFilePath);
		}
		return value;
	}
}
