package de.soderer.dbcsvexport.converter;

import java.io.File;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.Base64;

import de.soderer.utilities.Utilities;

public class FirebirdDBValueConverter extends DefaultDBValueConverter {
	public FirebirdDBValueConverter(boolean zip, boolean createBlobFiles, boolean createClobFiles, String fileExtension) {
		super(zip, createBlobFiles, createClobFiles, fileExtension);
	}

	@Override
	public Object convert(ResultSet resultSet, int columnIndex, String outputFilePath) throws Exception {
		ResultSetMetaData metaData = resultSet.getMetaData();
		Object value;
		if (metaData.getColumnType(columnIndex) == Types.LONGVARBINARY) {
			byte[] data = (byte[]) resultSet.getObject(columnIndex);
			if (resultSet.wasNull()) {
				value = null;
			} else if (createBlobFiles) {
				File blobOutputFile = new File(getLobFilePath(outputFilePath, "blob"));
				try {
					OutputStream output = null;
					try {
						output = openLobOutputStream(blobOutputFile);
						output.write(data);
					} finally {
						checkAndCloseZipEntry(output);
						Utilities.closeQuietly(output);
					}
					value = blobOutputFile;
				} catch (Exception e) {
					throw new Exception("Error creating blob file '" + blobOutputFile.getAbsolutePath() + "': " + e.getMessage());
				}
			} else {
				value = Base64.getEncoder().encodeToString(data);
			}
		} else {
			value = super.convert(resultSet, columnIndex, outputFilePath);
		}
		return value;
	}
}
