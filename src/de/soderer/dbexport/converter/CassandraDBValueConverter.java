package de.soderer.dbexport.converter;

import java.io.File;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.Base64;

import de.soderer.utilities.Utilities;

public class CassandraDBValueConverter extends DefaultDBValueConverter {
	public CassandraDBValueConverter(final boolean zip, final char[] zipPassword, final boolean useZipCrypto, final boolean createBlobFiles, final boolean createClobFiles, final String fileExtension) {
		super(zip, zipPassword, useZipCrypto, createBlobFiles, createClobFiles, fileExtension);
	}

	@Override
	public Object convert(final ResultSetMetaData metaData, final ResultSet resultSet, final int columnIndex, final String exportFilePath) throws Exception {
		Object value;
		final int columnTypeCode = metaData.getColumnType(columnIndex);
		if (columnTypeCode == Types.LONGVARBINARY) {
			final byte[] data = (byte[]) resultSet.getObject(columnIndex);
			if (resultSet.wasNull()) {
				value = null;
			} else if (createBlobFiles) {
				final File blobOutputFile = new File(getLobFilePath(exportFilePath, "blob"));
				try {
					OutputStream output = null;
					try {
						output = openLobOutputStream(blobOutputFile);
						output.write(data);
					} finally {
						checkAndCloseZipEntry(output, blobOutputFile);
						Utilities.closeQuietly(output);
					}
					value = blobOutputFile;
				} catch (final Exception e) {
					throw new Exception("Error creating blob file '" + blobOutputFile.getAbsolutePath() + "': " + e.getMessage());
				}
			} else {
				value = Base64.getEncoder().encodeToString(data);
			}
		} else {
			value = super.convert(metaData, resultSet, columnIndex, exportFilePath);
		}
		return value;
	}
}
