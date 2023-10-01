package de.soderer.dbimport.dataprovider;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import de.soderer.dbimport.DbImportException;
import de.soderer.utilities.TarGzUtilities;
import de.soderer.utilities.Tuple;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.csv.CsvDataException;
import de.soderer.utilities.csv.CsvFormat;
import de.soderer.utilities.csv.CsvReader;
import de.soderer.utilities.csv.CsvWriter;
import de.soderer.utilities.db.DbColumnType;
import de.soderer.utilities.zip.Zip4jUtilities;
import de.soderer.utilities.zip.ZipUtilities;

public class CsvDataProvider extends DataProvider {
	// Default optional parameters
	private char separator = ';';
	private Character stringQuote = '"';
	private char escapeStringQuote = '"';
	private String nullValueText = null;
	private boolean allowUnderfilledLines = false;
	private boolean removeSurplusEmptyTrailingColumns = false;
	private boolean noHeaders = false;
	private boolean trimData = false;

	private CsvReader csvReader = null;
	private List<String> columnNames = null;
	private Map<String, DbColumnType> dataTypes = null;
	private Long itemsAmount = null;
	private String itemsUnitSign = null;

	private final Charset encoding = StandardCharsets.UTF_8;

	public CsvDataProvider(final boolean isInlineData, final String importFilePathOrData, final char[] zipPassword, final char separator, final Character stringQuote, final char escapeStringQuote, final boolean allowUnderfilledLines, final boolean removeSurplusEmptyTrailingColumns, final boolean noHeaders, final String nullValueText, final boolean trimData) {
		super(isInlineData, importFilePathOrData, zipPassword);
		this.separator = separator;
		this.stringQuote = stringQuote;
		this.escapeStringQuote = escapeStringQuote;
		this.allowUnderfilledLines = allowUnderfilledLines;
		this.removeSurplusEmptyTrailingColumns = removeSurplusEmptyTrailingColumns;
		this.noHeaders = noHeaders;
		this.nullValueText = nullValueText;
		this.trimData = trimData;
	}

	@Override
	public String getConfigurationLogString() {
		return super.getConfigurationLogString()
				+ "Format: CSV" + "\n"
				+ "Encoding: " + encoding + "\n"
				+ "Separator: " + separator + "\n"
				+ "StringQuote: " + stringQuote + "\n"
				+ "EscapeStringQuote: " + escapeStringQuote + "\n"
				+ "AllowUnderfilledLines: " + allowUnderfilledLines + "\n"
				+ "RemoveSurplusEmptyTrailingColumns: " + removeSurplusEmptyTrailingColumns + "\n"
				+ "TrimData: " + trimData + "\n"
				+ "Null value text: " + (nullValueText == null ? "none" : "\"" + nullValueText + "\"") + "\n";
	}

	@Override
	public Map<String, DbColumnType> scanDataPropertyTypes(final Map<String, Tuple<String, String>> mapping) throws Exception {
		if (dataTypes == null) {
			final CsvFormat csvFormat = new CsvFormat()
					.setSeparator(separator)
					.setStringQuote(stringQuote)
					.setStringQuoteEscapeCharacter(escapeStringQuote)
					.setFillMissingTrailingColumnsWithNull(allowUnderfilledLines)
					.setRemoveSurplusEmptyTrailingColumns(removeSurplusEmptyTrailingColumns)
					.setAlwaysTrim(trimData);

			try (CsvReader scanCsvReader = new CsvReader(getInputStream(), encoding, csvFormat)) {
				if (!noHeaders) {
					// Read headers from file
					columnNames = scanCsvReader.readNextCsvLine();
				}

				dataTypes = new HashMap<>();

				// Scan all data for maximum
				List<String> values;
				while ((values = scanCsvReader.readNextCsvLine()) != null) {
					for (int i = 0; i < values.size(); i++) {
						final String columnName = (columnNames == null || columnNames.size() <= i ? "column_" + Integer.toString(i + 1) : columnNames.get(i));
						final String currentValue = values.get(i);
						detectNextDataType(mapping, dataTypes, columnName, currentValue);
					}
				}
			} catch (final Exception e) {
				throw e;
			}
		}

		return dataTypes;
	}

	@Override
	public List<String> getAvailableDataPropertyNames() throws Exception {
		if (columnNames == null) {
			final CsvFormat csvFormat = new CsvFormat()
					.setSeparator(separator)
					.setStringQuote(stringQuote)
					.setStringQuoteEscapeCharacter(escapeStringQuote)
					.setFillMissingTrailingColumnsWithNull(allowUnderfilledLines)
					.setRemoveSurplusEmptyTrailingColumns(removeSurplusEmptyTrailingColumns)
					.setAlwaysTrim(true);

			try (CsvReader scanCsvReader = new CsvReader(getInputStream(), encoding, csvFormat)) {
				if (noHeaders) {
					final List<String> returnList = new ArrayList<>();
					if (allowUnderfilledLines) {
						// Scan all data for maximum
						List<String> values;
						int maxColumns = 0;
						while ((values = scanCsvReader.readNextCsvLine()) != null) {
							maxColumns = Math.max(maxColumns, values.size());
						}
						for (int i = 0; i < maxColumns; i++) {
							returnList.add(Integer.toString(i + 1));
						}
						columnNames = returnList;
					} else {
						// Only take first data as example for all other data
						final List<String> values = scanCsvReader.readNextCsvLine();
						for (int i = 0; i < values.size(); i++) {
							returnList.add("column_" + Integer.toString(i + 1));
						}
						columnNames = returnList;
					}
				} else {
					// Read headers from file
					columnNames = scanCsvReader.readNextCsvLine();
				}
			} catch (final Exception e) {
				throw e;
			}
		}

		return columnNames;
	}

	@Override
	public long getItemsAmountToImport() throws Exception {
		if (itemsAmount == null) {
			if (getImportDataAmount() < 1024 * 1024 * 1024) {
				final CsvFormat csvFormat = new CsvFormat()
						.setSeparator(separator)
						.setStringQuote(stringQuote)
						.setStringQuoteEscapeCharacter(escapeStringQuote)
						.setFillMissingTrailingColumnsWithNull(allowUnderfilledLines)
						.setRemoveSurplusEmptyTrailingColumns(removeSurplusEmptyTrailingColumns);

				try (CsvReader scanCsvReader = new CsvReader(getInputStream(), encoding, csvFormat)) {
					if (noHeaders) {
						itemsAmount = Long.valueOf(scanCsvReader.getCsvLineCount());
					} else {
						itemsAmount = Long.valueOf(scanCsvReader.getCsvLineCount() - 1);
					}
				} catch (final CsvDataException e) {
					throw new DbImportException(e.getMessage(), e);
				} catch (final Exception e) {
					throw e;
				}
			} else {
				itemsAmount = getImportDataAmount();
				itemsUnitSign = "B";
			}
		}

		return itemsAmount;
	}

	@Override
	public String getItemsUnitSign() throws Exception {
		return itemsUnitSign;
	}

	@Override
	public Map<String, Object> getNextItemData() throws Exception {
		if (csvReader == null) {
			openReader();
		}

		final List<String> values = csvReader.readNextCsvLine();
		if (values != null) {
			final Map<String, Object> returnMap = new HashMap<>();
			for (int i = 0; i < getAvailableDataPropertyNames().size(); i++) {
				final String columnName = getAvailableDataPropertyNames().get(i);
				if (values.size() > i) {
					if (nullValueText != null && nullValueText.equals(values.get(i))) {
						returnMap.put(columnName, null);
					} else {
						returnMap.put(columnName, values.get(i));
					}
				} else {
					returnMap.put(columnName, null);
				}
			}
			return returnMap;
		} else {
			return null;
		}
	}

	@Override
	public File filterDataItems(final List<Integer> indexList, final String fileSuffix) throws Exception {
		OutputStream outputStream = null;
		File tempFile = null;
		try {
			openReader();

			File filteredDataFile;
			if (Utilities.endsWithIgnoreCase(getImportFilePath(), ".zip")) {
				filteredDataFile = new File(getImportFilePath() + "." + fileSuffix + ".csv.zip");
				outputStream = ZipUtilities.openNewZipOutputStream(filteredDataFile, null);
				((ZipOutputStream) outputStream).putNextEntry(new ZipEntry(new File(getImportFilePath() + "." + fileSuffix + ".csv").getName()));
			} else if (Utilities.endsWithIgnoreCase(getImportFilePath(), ".tar.gz")) {
				filteredDataFile = new File(getImportFilePath() + "." + fileSuffix + ".csv.tar.gz");
				tempFile = File.createTempFile(new File(getImportFilePath()).getName(), fileSuffix);
				outputStream = new FileOutputStream(tempFile);
			} else if (Utilities.endsWithIgnoreCase(getImportFilePath(), ".tgz")) {
				filteredDataFile = new File(getImportFilePath() + "." + fileSuffix + ".csv.tgz");
				tempFile = File.createTempFile(new File(getImportFilePath()).getName(), fileSuffix);
				outputStream = new FileOutputStream(tempFile);
			} else if (Utilities.endsWithIgnoreCase(getImportFilePath(), ".gz")) {
				filteredDataFile = new File(getImportFilePath() + "." + fileSuffix + ".csv.gz");
				outputStream = new GZIPOutputStream(new FileOutputStream(filteredDataFile));
			} else {
				filteredDataFile = new File(getImportFilePath() + "." + fileSuffix + ".csv");
				outputStream = new FileOutputStream(filteredDataFile);
			}

			try (CsvWriter csvWriter = new CsvWriter(outputStream, encoding, new CsvFormat().setSeparator(separator).setStringQuote(stringQuote).setStringQuoteEscapeCharacter(escapeStringQuote))) {
				csvWriter.writeValues(columnNames);

				List<String> values;
				int itemIndex = 0;
				while ((values = csvReader.readNextCsvLine()) != null) {
					itemIndex++;
					if (indexList.contains(itemIndex)) {
						csvWriter.writeValues(values);
					}
				}
			}

			if (Utilities.endsWithIgnoreCase(getImportFilePath(), ".zip") && zipPassword != null) {
				Zip4jUtilities.createPasswordSecuredZipFile(filteredDataFile.getAbsolutePath(), zipPassword, false);
			} else if (Utilities.endsWithIgnoreCase(getImportFilePath(), ".tar.gz")) {
				TarGzUtilities.compress(filteredDataFile, tempFile, new File(getImportFilePath()).getName() + "." + fileSuffix);
			} else if (Utilities.endsWithIgnoreCase(getImportFilePath(), ".tgz")) {
				TarGzUtilities.compress(filteredDataFile, tempFile, new File(getImportFilePath()).getName() + "." + fileSuffix);
			}

			return filteredDataFile;
		} finally {
			close();
			Utilities.closeQuietly(outputStream);
			if (tempFile != null && tempFile.exists()) {
				tempFile.delete();
				tempFile = null;
			}
		}
	}

	private void openReader() throws Exception {
		if (csvReader != null) {
			throw new Exception("Reader was already opened before");
		}

		try {
			final CsvFormat csvFormat = new CsvFormat()
					.setSeparator(separator)
					.setStringQuote(stringQuote)
					.setStringQuoteEscapeCharacter(escapeStringQuote)
					.setFillMissingTrailingColumnsWithNull(allowUnderfilledLines)
					.setRemoveSurplusEmptyTrailingColumns(removeSurplusEmptyTrailingColumns)
					.setAlwaysTrim(trimData);

			csvReader = new CsvReader(getInputStream(), encoding, csvFormat);

			if (!noHeaders) {
				// Skip headers
				csvReader.readNextCsvLine();
			}
		} catch (final Exception e) {
			close();
			throw e;
		}
	}

	@Override
	public void close() {
		Utilities.closeQuietly(csvReader);
		csvReader = null;
		super.close();
	}
}
