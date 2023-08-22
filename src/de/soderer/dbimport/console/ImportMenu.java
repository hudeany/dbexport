package de.soderer.dbimport.console;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import de.soderer.dbimport.DbImport;
import de.soderer.dbimport.DbImportDefinition;
import de.soderer.dbimport.DbImportDefinition.DataType;
import de.soderer.dbimport.DbImportDefinition.DuplicateMode;
import de.soderer.dbimport.DbImportDefinition.ImportMode;
import de.soderer.dbimport.DbImportException;
import de.soderer.utilities.FileUtilities;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.console.ConsoleMenu;
import de.soderer.utilities.console.ConsoleUtilities;
import de.soderer.utilities.console.ConsoleUtilities.TextColor;
import de.soderer.utilities.console.FilepathConsoleInput;
import de.soderer.utilities.console.PasswordConsoleInput;
import de.soderer.utilities.console.SimpleConsoleInput;
import de.soderer.utilities.db.DbUtilities.DbVendor;

public class ImportMenu extends ConsoleMenu {
	private DbImportDefinition dbImportDefinition = new DbImportDefinition();

	public DbImportDefinition getDbImportDefinition() {
		return dbImportDefinition;
	}

	public void setDbImportDefinition(final DbImportDefinition dbImportDefinition) {
		this.dbImportDefinition = dbImportDefinition;
	}

	public ImportMenu(final ConsoleMenu parentMenu) throws Exception {
		super(parentMenu, "Import data");
	}

	@Override
	public int show() throws Exception {
		try {
			ConsoleUtilities.clearScreen();

			ConsoleUtilities.printBoxed(DbImport.APPLICATION_NAME + " (v" + DbImport.VERSION.toString() + ")");

			System.out.println();

			System.out.println("Import data");
			System.out.println();
			printMessages();

			while (true) {
				while (dbImportDefinition.getDbVendor() == null) {
					try {
						final String dbVendorString = askForSelection("Please choose DB vendor", Stream.of(DbVendor.values()).map(x -> x.name()).collect(Collectors.toList()));
						if (dbVendorString == null) {
							getParentMenu().getMessages().add("Canceled by user");
							return 0;
						} else {
							dbImportDefinition.setDbVendor(DbVendor.getDbVendorByName(dbVendorString));
						}
					} catch (final Exception e) {
						System.out.println(ConsoleUtilities.getAnsiColoredText(e.getMessage(), TextColor.Light_red));
					}
				}

				if (Utilities.isBlank(dbImportDefinition.getHostnameAndPort()) && dbImportDefinition.getDbVendor() != DbVendor.SQLite && dbImportDefinition.getDbVendor() != DbVendor.HSQL && dbImportDefinition.getDbVendor() != DbVendor.Derby) {
					System.out.println();
					System.out.println("Please enter db hostname and optional port separated by ':' (No port uses db vendors default port, Blank => Cancel)");
					String choice = new SimpleConsoleInput().setPrompt(" > ").readInput();
					choice = choice == null ? "" : choice.trim();
					if (Utilities.isBlank(choice)) {
						getParentMenu().getMessages().add("Canceled by user");
						return 0;
					} else {
						dbImportDefinition.setHostnameAndPort(choice);
					}
				}

				if (dbImportDefinition.getDbVendor() == DbVendor.SQLite || dbImportDefinition.getDbVendor() == DbVendor.Derby) {
					while (Utilities.isBlank(dbImportDefinition.getDbName())) {
						System.out.println();
						System.out.println("Please enter db filepath (Blank => Cancel)");
						String choice = new SimpleConsoleInput().setPrompt(" > ").readInput();
						choice = choice == null ? "" : choice.trim();
						if (Utilities.isBlank(choice)) {
							getParentMenu().getMessages().add("Canceled by user");
							return 0;
						} else if (!FileUtilities.isValidFilePath(choice)) {
							System.out.println(ConsoleUtilities.getAnsiColoredText("Not a valid filepath", TextColor.Light_red));
						} else if (!new File(choice).exists()) {
							System.out.println(ConsoleUtilities.getAnsiColoredText("Filepath does not exist", TextColor.Light_red));
						} else {
							dbImportDefinition.setDbName(choice);
						}
					}
				} else {
					while (Utilities.isBlank(dbImportDefinition.getDbName())) {
						System.out.println();
						System.out.println("Please enter db name (Blank => Cancel)");
						String choice = new SimpleConsoleInput().setPrompt(" > ").readInput();
						choice = choice == null ? "" : choice.trim();
						if (Utilities.isBlank(choice)) {
							getParentMenu().getMessages().add("Canceled by user");
							return 0;
						} else {
							dbImportDefinition.setDbName(choice);
						}
					}
				}

				if (Utilities.isBlank(dbImportDefinition.getUsername()) && dbImportDefinition.getDbVendor() != DbVendor.SQLite && dbImportDefinition.getDbVendor() != DbVendor.Derby) {
					System.out.println();
					System.out.println("Please enter db username (Blank => Cancel)");
					String choice = new SimpleConsoleInput().setPrompt(" > ").readInput();
					choice = choice == null ? "" : choice.trim();
					if (Utilities.isBlank(choice)) {
						if (dbImportDefinition.getDbVendor() == DbVendor.Cassandra) {
							dbImportDefinition.setUsername(null);
						} else {
							getParentMenu().getMessages().add("Canceled by user");
							return 0;
						}
					} else {
						dbImportDefinition.setUsername(choice);
					}
				}

				while (Utilities.isBlank(dbImportDefinition.getTableName())) {
					System.out.println();
					System.out.println("Please enter import destination table (Blank => Cancel)");
					String choice = new SimpleConsoleInput().setPrompt(" > ").readInput();
					choice = choice == null ? "" : choice.trim();
					if (Utilities.isBlank(choice)) {
						getParentMenu().getMessages().add("Canceled by user");
						return 0;
					} else {
						dbImportDefinition.setTableName(choice);
					}
				}

				if (Utilities.isBlank(dbImportDefinition.getPassword()) && dbImportDefinition.getDbVendor() != DbVendor.SQLite && dbImportDefinition.getDbVendor() != DbVendor.HSQL && dbImportDefinition.getDbVendor() != DbVendor.Derby && (dbImportDefinition.getDbVendor() != DbVendor.Cassandra || dbImportDefinition.getUsername() != null)) {
					System.out.println();
					System.out.println("Please enter db password (Blank => Cancel)");
					final char[] passwordArray = new PasswordConsoleInput().setPrompt(" > ").readInput();
					if (Utilities.isBlank(passwordArray)) {
						getParentMenu().getMessages().add("Canceled by user");
						return 0;
					}
					dbImportDefinition.setPassword(passwordArray);
				}

				while (Utilities.isBlank(dbImportDefinition.getTableName())) {
					System.out.println();
					System.out.println("Please enter import destination table (Blank => Cancel)");
					String choiceTable = new SimpleConsoleInput().setPrompt(" > ").readInput();
					choiceTable = choiceTable == null ? "" : choiceTable.trim();
					if (Utilities.isBlank(choiceTable)) {
						getParentMenu().getMessages().add("Canceled by user");
						return 0;
					} else {
						dbImportDefinition.setTableName(choiceTable);
					}
				}

				while (Utilities.isBlank(dbImportDefinition.getImportFilePathOrData())) {
					System.out.println();
					System.out.println("Please enter import filepath or data (Blank => Cancel)");
					String choice = new SimpleConsoleInput().setPrompt(" > ").readInput();
					choice = choice == null ? "" : choice.trim();
					if (Utilities.isBlank(choice)) {
						getParentMenu().getMessages().add("Canceled by user");
						return 0;
					} else {
						dbImportDefinition.setImportFilePathOrData(choice, true);
					}
				}

				ConsoleUtilities.clearScreen();

				ConsoleUtilities.printBoxed(DbImport.APPLICATION_NAME + " (v" + DbImport.VERSION.toString() + ")");

				System.out.println();

				System.out.println("Change parameters or start import");
				System.out.println();
				printMessages();

				final int bulletSize = 23;
				final int nameSize = 49;

				final List<String> autoCompletionStrings = new ArrayList<>();
				autoCompletionStrings.add("");

				System.out.println("  " + Utilities.rightPad("DbVendor:", bulletSize) + " " + dbImportDefinition.getDbVendor().name());
				if (dbImportDefinition.getDbVendor() != DbVendor.SQLite && dbImportDefinition.getDbVendor() != DbVendor.HSQL && dbImportDefinition.getDbVendor() != DbVendor.Derby) {
					System.out.println("  " + Utilities.rightPad("Hostname:", bulletSize) + " " + dbImportDefinition.getHostnameAndPort());
				}
				if (dbImportDefinition.getDbVendor() == DbVendor.SQLite || dbImportDefinition.getDbVendor() == DbVendor.Derby) {
					System.out.println("  " + Utilities.rightPad("Db filepath:", bulletSize) + " " + dbImportDefinition.getDbName());
				} else {
					System.out.println("  " + Utilities.rightPad("Db name:", bulletSize) + " " + dbImportDefinition.getDbName());
				}
				if (dbImportDefinition.getDbVendor() != DbVendor.SQLite && dbImportDefinition.getDbVendor() != DbVendor.Derby) {
					System.out.println("  " + Utilities.rightPad("Db username:", bulletSize) + " " + (dbImportDefinition.getUsername() == null ? "<empty>" : dbImportDefinition.getUsername()));
				}
				System.out.println("  " + Utilities.rightPad("Db password:", bulletSize) + " " + (dbImportDefinition.getPassword() == null ? "<empty>" : "***"));

				if (dbImportDefinition.getDbVendor() == DbVendor.Oracle || dbImportDefinition.getDbVendor() == DbVendor.MySQL || dbImportDefinition.getDbVendor() == DbVendor.MariaDB) {
					System.out.println("  " + Utilities.rightPad("Secure connection:", bulletSize) + " " + (dbImportDefinition.isSecureConnection() ? "yes" : "no"));
					System.out.println("  " + Utilities.rightPad("TrustStore filepath:", bulletSize) + " " + (dbImportDefinition.getTrustStoreFile() == null ? "<none>" : dbImportDefinition.getTrustStoreFile().getAbsolutePath()));
					System.out.println("  " + Utilities.rightPad("TrustStore password:", bulletSize) + " " + (dbImportDefinition.getTrustStorePassword() == null ? "<empty>" : "***"));
				}

				System.out.println();
				System.out.println("  " + Utilities.rightPad("reset)", bulletSize) + " " + "Reset basic db parameters");
				autoCompletionStrings.add("reset");
				System.out.println();

				if (dbImportDefinition.getDbVendor() == DbVendor.Oracle || dbImportDefinition.getDbVendor() == DbVendor.MySQL || dbImportDefinition.getDbVendor() == DbVendor.MariaDB) {
					System.out.println("  " + Utilities.rightPad("secure)", bulletSize) + " " + "Change setting for secure connection via SSL/TLS");
					autoCompletionStrings.add("secure");
					System.out.println("  " + Utilities.rightPad("truststore)", bulletSize) + " " + "Define TrustStore filepath (Optional)");
					autoCompletionStrings.add("truststore");
					System.out.println("  " + Utilities.rightPad("truststorepassword)", bulletSize) + " " + "Define TrustStore password");
					autoCompletionStrings.add("truststorepassword");
					System.out.println();
				}

				System.out.println();
				System.out.println("  " + Utilities.rightPad("table)", bulletSize) + " " + Utilities.rightPad("Import table:", nameSize) + dbImportDefinition.getTableName());
				autoCompletionStrings.add("table");
				System.out.println("  " + Utilities.rightPad("import)", bulletSize) + " " + Utilities.rightPad("Input filepath or data:", nameSize) + dbImportDefinition.getImportFilePathOrData());
				autoCompletionStrings.add("import");
				System.out.println();

				System.out.println("  " + Utilities.rightPad("data)", bulletSize) + " " + Utilities.rightPad("Use inline data:", nameSize) + dbImportDefinition.isInlineData());
				autoCompletionStrings.add("data");
				System.out.println("  " + Utilities.rightPad("x)", bulletSize) + " " + Utilities.rightPad("Importformat:", nameSize) + dbImportDefinition.getDataType().name());
				autoCompletionStrings.add("x");
				if (dbImportDefinition.getDataType() == DataType.JSON || dbImportDefinition.getDataType() == DataType.XML || dbImportDefinition.getDataType() == DataType.EXCEL || dbImportDefinition.getDataType() == DataType.ODS || dbImportDefinition.getDataType() == DataType.VCF) {
					System.out.println("  " + Utilities.rightPad("dp)", bulletSize) + " " + Utilities.rightPad("Data path:", nameSize) + dbImportDefinition.getDataPath());
					autoCompletionStrings.add("dp");
				}
				if (dbImportDefinition.getDataType() == DataType.JSON || dbImportDefinition.getDataType() == DataType.XML) {
					System.out.println("  " + Utilities.rightPad("sp)", bulletSize) + " " + Utilities.rightPad("Schema file:", nameSize) + (dbImportDefinition.getSchemaFilePath() == null ? "<empty>" : dbImportDefinition.getSchemaFilePath()));
					autoCompletionStrings.add("sp");
				}
				System.out.println("  " + Utilities.rightPad("m)", bulletSize) + " " + Utilities.rightPad("Column mappings:", nameSize) + dbImportDefinition.getMapping());
				autoCompletionStrings.add("m");
				if (dbImportDefinition.getDataType() == DataType.CSV || dbImportDefinition.getDataType() == DataType.XML) {
					System.out.println("  " + Utilities.rightPad("n)", bulletSize) + " " + Utilities.rightPad("Null value string:", nameSize) + "'" + dbImportDefinition.getNullValueString() + "'");
					autoCompletionStrings.add("n");
				}
				System.out.println("  " + Utilities.rightPad("l)", bulletSize) + " " + Utilities.rightPad("Log import information in .log files:", nameSize) + dbImportDefinition.isLog());
				autoCompletionStrings.add("l");
				System.out.println("  " + Utilities.rightPad("v)", bulletSize) + " " + Utilities.rightPad("Verbose terminal output:", nameSize) + dbImportDefinition.isVerbose());
				autoCompletionStrings.add("v");
				if (dbImportDefinition.getDataType() == DataType.CSV || dbImportDefinition.getDataType() == DataType.JSON) {
					System.out.println("  " + Utilities.rightPad("e)", bulletSize) + " " + Utilities.rightPad("Output encoding:", nameSize) + dbImportDefinition.getEncoding());
					autoCompletionStrings.add("e");
				}
				if (dbImportDefinition.getDataType() == DataType.CSV || dbImportDefinition.getDataType() == DataType.XML) {
					System.out.println("  " + Utilities.rightPad("s)", bulletSize) + " " + Utilities.rightPad("CSV separator character:", nameSize) + dbImportDefinition.getSeparator());
					autoCompletionStrings.add("s");
					System.out.println("  " + Utilities.rightPad("q)", bulletSize) + " " + Utilities.rightPad("CSV string quote character:", nameSize) + dbImportDefinition.getStringQuote());
					autoCompletionStrings.add("q");
					System.out.println("  " + Utilities.rightPad("qe)", bulletSize) + " " + Utilities.rightPad("CSV string quote escape character:", nameSize) + dbImportDefinition.getEscapeStringQuote());
					autoCompletionStrings.add("qe");
					System.out.println("  " + Utilities.rightPad("noheaders)", bulletSize) + " " + Utilities.rightPad("CSV file has no headers:", nameSize) + dbImportDefinition.isNoHeaders());
					autoCompletionStrings.add("noheaders");
				}
				System.out.println("  " + Utilities.rightPad("c)", bulletSize) + " " + Utilities.rightPad("Complete commit only:", nameSize) + dbImportDefinition.isInlineData());
				autoCompletionStrings.add("c");
				System.out.println("  " + Utilities.rightPad("nonewindex)", bulletSize) + " " + Utilities.rightPad("No new indexes on destination table:", nameSize) + !dbImportDefinition.isCreateNewIndexIfNeeded());
				autoCompletionStrings.add("nonewindex");
				System.out.println("  " + Utilities.rightPad("deactivatefk)", bulletSize) + " " + Utilities.rightPad("Deactive FKs during import:", nameSize) + dbImportDefinition.isDeactivateForeignKeyConstraints());
				autoCompletionStrings.add("deactivatefk");
				System.out.println("  " + Utilities.rightPad("deactivatetriggers)", bulletSize) + " " + Utilities.rightPad("Deactive Triggers during import:", nameSize) + dbImportDefinition.isDeactivateTriggers());
				autoCompletionStrings.add("deactivatetriggers");
				if (dbImportDefinition.getDataType() == DataType.CSV) {
					System.out.println("  " + Utilities.rightPad("a)", bulletSize) + " " + Utilities.rightPad("Allow underfilled lines:", nameSize) + dbImportDefinition.isAllowUnderfilledLines());
					autoCompletionStrings.add("a");
				}
				if (dbImportDefinition.getDataType() == DataType.CSV) {
					System.out.println("  " + Utilities.rightPad("r)", bulletSize) + " " + Utilities.rightPad("Allow lines with surplus empty trailing columns:", nameSize) + !dbImportDefinition.isRemoveSurplusEmptyTrailingColumns());
					autoCompletionStrings.add("r");
				}
				System.out.println("  " + Utilities.rightPad("t)", bulletSize) + " " + Utilities.rightPad("Trim data values:", nameSize) + dbImportDefinition.isTrimData());
				autoCompletionStrings.add("t");
				System.out.println("  " + Utilities.rightPad("i)", bulletSize) + " " + Utilities.rightPad("Import mode:", nameSize) + dbImportDefinition.getImportMode().name());
				autoCompletionStrings.add("i");
				System.out.println("  " + Utilities.rightPad("d)", bulletSize) + " " + Utilities.rightPad("Duplicate mode:", nameSize) + dbImportDefinition.getDuplicateMode().name());
				autoCompletionStrings.add("d");
				System.out.println("  " + Utilities.rightPad("u)", bulletSize) + " " + Utilities.rightPad("Don't update with null values:", nameSize) + dbImportDefinition.isUpdateNullData());
				autoCompletionStrings.add("u");

				System.out.println("  " + Utilities.rightPad("k)", bulletSize) + " " + Utilities.rightPad("Keycolumns:", nameSize) + (dbImportDefinition.getKeycolumns() == null || dbImportDefinition.getKeycolumns().isEmpty() ? "<empty>" : Utilities.join(dbImportDefinition.getKeycolumns(), ", ")));
				autoCompletionStrings.add("k");
				System.out.println("  " + Utilities.rightPad("insvalues)", bulletSize) + " " + Utilities.rightPad("Additional insert values:", nameSize) + (Utilities.isBlank(dbImportDefinition.getAdditionalInsertValues()) ? "<empty>" : dbImportDefinition.getAdditionalInsertValues()));
				autoCompletionStrings.add("insvalues");
				System.out.println("  " + Utilities.rightPad("updvalues)", bulletSize) + " " + Utilities.rightPad("Additional update values:", nameSize) + (Utilities.isBlank(dbImportDefinition.getAdditionalUpdateValues()) ? "<empty>" : dbImportDefinition.getAdditionalInsertValues()));
				autoCompletionStrings.add("updvalues");

				System.out.println("  " + Utilities.rightPad("create)", bulletSize) + " " + Utilities.rightPad("Scan data and create suitable table:", nameSize) + dbImportDefinition.isCreateTable());
				autoCompletionStrings.add("create");
				System.out.println("  " + Utilities.rightPad("structure)", bulletSize) + " " + Utilities.rightPad("Structure file:", nameSize) + (dbImportDefinition.getStructureFilePath() == null ? "<empty>" : dbImportDefinition.getStructureFilePath()));
				autoCompletionStrings.add("structure");
				System.out.println("  " + Utilities.rightPad("logerrors)", bulletSize) + " " + Utilities.rightPad("Log error data items in file:", nameSize) + dbImportDefinition.isLogErroneousData());
				autoCompletionStrings.add("logerrors");
				if (dbImportDefinition.getImportFilePathOrData().toLowerCase().endsWith(".zip")) {
					System.out.println("  " + Utilities.rightPad("zippassword)", bulletSize) + " " + Utilities.rightPad("Zip file password:", nameSize) + (dbImportDefinition.getZipPassword() == null ? "<empty>" : "***"));
					autoCompletionStrings.add("zippassword");
				}
				if (dbImportDefinition.getDataType() == DataType.KDBX) {
					System.out.println("  " + Utilities.rightPad("kdbxpassword)", bulletSize) + " " + Utilities.rightPad("KDBX file password:", nameSize) + (dbImportDefinition.getKdbxPassword() == null ? "<empty>" : "***"));
					autoCompletionStrings.add("kdbxpassword");
				}
				System.out.println("  " + Utilities.rightPad("dbtz)", bulletSize) + " " + Utilities.rightPad("DatabaseTimeZone:", nameSize) + dbImportDefinition.getDatabaseTimeZone());
				autoCompletionStrings.add("dbtz");
				System.out.println("  " + Utilities.rightPad("idtz)", bulletSize) + " " + Utilities.rightPad("ImportDataTimeZone:", nameSize) + dbImportDefinition.getImportDataTimeZone());
				autoCompletionStrings.add("idtz");

				System.out.println();
				System.out.println("  " + Utilities.rightPad("params)", bulletSize) + " " + "Print parameters for later use (Includes passwords)");
				autoCompletionStrings.add("params");
				System.out.println("  " + Utilities.rightPad("start)", bulletSize) + " " + "Start import");
				autoCompletionStrings.add("start");
				System.out.println("  " + Utilities.rightPad("cancel)", bulletSize) + " " + "Cancel");
				autoCompletionStrings.add("cancel");

				String choice = new SimpleConsoleInput().setAutoCompletionStrings(autoCompletionStrings).setPresetContent("start").setPrompt(" > ").readInput();
				choice = choice == null ? "" : choice.trim();
				if (Utilities.isBlank(choice)) {
					getParentMenu().getMessages().add("Canceled by user");
					return 0;
				} else if ("reset".equalsIgnoreCase(choice)) {
					dbImportDefinition.setDbVendor((DbVendor) null);
					dbImportDefinition.setHostnameAndPort(null);
					dbImportDefinition.setUsername(null);
					dbImportDefinition.setDbName(null);
					dbImportDefinition.setDbName(null);
					dbImportDefinition.setTableName(null);
					dbImportDefinition.setPassword(null);
					dbImportDefinition.setSecureConnection(false);
					dbImportDefinition.setTrustStoreFile(null);
					dbImportDefinition.setTrustStorePassword(null);
				} else if ("secure".equalsIgnoreCase(choice)) {
					dbImportDefinition.setSecureConnection(dbImportDefinition.isSecureConnection());
				} else if ("truststore".equalsIgnoreCase(choice)) {
					System.out.println();
					System.out.println("Please enter db TrustStore filepath (Blank => None)");
					String choiceTruststore = new SimpleConsoleInput().setPrompt(" > ").readInput();
					choiceTruststore = choiceTruststore == null ? "" : choiceTruststore.trim();
					if (Utilities.isBlank(choiceTruststore)) {
						dbImportDefinition.setTrustStoreFile(null);
					} else if (!FileUtilities.isValidFilePath(choiceTruststore)) {
						getErrors().add("Not a valid filepath");
					} else if (!new File(choiceTruststore).exists()) {
						getErrors().add("Filepath does not exist");
					} else {
						dbImportDefinition.setTrustStoreFile(new File(choiceTruststore));
					}
				} else if ("truststorepassword".equalsIgnoreCase(choice)) {
					System.out.println();
					System.out.println("Please enter TrustStore password (Blank => Empty)");
					final char[] passwordArray = new PasswordConsoleInput().setPrompt(" > ").readInput();
					dbImportDefinition.setTrustStorePassword(Utilities.isNotEmpty(passwordArray) ? passwordArray : null);
				} else if ("data".equalsIgnoreCase(choice)) {
					dbImportDefinition.setInlineData(!dbImportDefinition.isInlineData());
				} else if ("x".equalsIgnoreCase(choice)) {
					while (true) {
						try {
							final String importformatString = askForSelection("Please select importformat", Stream.of(DataType.values()).map(Enum::name).collect(Collectors.toList()));
							if (importformatString == null) {
								getParentMenu().getMessages().add("Canceled by user");
							} else {
								dbImportDefinition.setDataType(DataType.getFromString(importformatString));
							}
							break;
						} catch (final Exception e) {
							System.out.println(ConsoleUtilities.getAnsiColoredText(e.getMessage(), TextColor.Light_red));
						}
					}
				} else if ("dp".equalsIgnoreCase(choice)) {
					System.out.println();
					System.out.println("Please enter data path");
					final String dataPath = new SimpleConsoleInput().setPrompt(" > ").readInput();
					dbImportDefinition.setDataPath(dataPath);
				} else if ("structure".equalsIgnoreCase(choice)) {
					System.out.println();
					System.out.println("Please enter structure file path");
					final String structureFilePath = new FilepathConsoleInput().setPrompt(" > ").readInput();
					dbImportDefinition.setStructureFilePath(Utilities.isBlank(structureFilePath) ? null : structureFilePath);
				} else if ("sp".equalsIgnoreCase(choice)) {
					System.out.println();
					System.out.println("Please enter schema file path");
					final String schemaFilePath = new FilepathConsoleInput().setPrompt(" > ").readInput();
					dbImportDefinition.setSchemaFilePath(Utilities.isBlank(schemaFilePath) ? null : schemaFilePath);
				} else if ("m".equalsIgnoreCase(choice)) {
					System.out.println();
					System.out.println("Please enter column mappings");
					final String mappings = new SimpleConsoleInput().setPrompt(" > ").readInput();
					dbImportDefinition.setMapping(Utilities.isBlank(mappings) ? null : mappings);
				} else if ("mf".equalsIgnoreCase(choice)) {
					System.out.println();
					System.out.println("Please enter mapping file path");
					final String mappingFilePath = new FilepathConsoleInput().setPrompt(" > ").readInput();
					if (Utilities.isNotBlank(mappingFilePath)) {
						if (!new File(mappingFilePath).exists()) {
							throw new DbImportException("Mapping file does not exist");
						} else if (Utilities.isNotBlank(dbImportDefinition.getMapping())) {
							throw new DbImportException("Mapping is already defined");
						} else {
							dbImportDefinition.setMapping(new String(Utilities.readFileToByteArray(new File(mappingFilePath)), StandardCharsets.UTF_8));
						}
					}
				} else if ("n".equalsIgnoreCase(choice)) {
					System.out.println();
					System.out.println("Please enter null value string");
					final String nullValueString = new SimpleConsoleInput().setPrompt(" > ").readInput();
					dbImportDefinition.setNullValueString(nullValueString);
				} else if ("l".equalsIgnoreCase(choice)) {
					dbImportDefinition.setLog(!dbImportDefinition.isLog());
				} else if ("v".equalsIgnoreCase(choice)) {
					dbImportDefinition.setVerbose(!dbImportDefinition.isVerbose());
				} else if ("e".equalsIgnoreCase(choice)) {
					while (true) {
						System.out.println();
						System.out.println("Please enter output encoding");
						String encodingString = new SimpleConsoleInput().setPrompt(" > ").readInput();
						encodingString = encodingString == null ? "" : encodingString.trim();
						if (Utilities.isBlank(encodingString)) {
							getParentMenu().getMessages().add("Canceled by user");
							return 0;
						} else {
							try {
								dbImportDefinition.setEncoding(Charset.forName(encodingString));
								break;
							} catch (@SuppressWarnings("unused") final Exception e) {
								System.out.println(ConsoleUtilities.getAnsiColoredText("Unsupported encoding: " + encodingString, TextColor.Light_red));
							}
						}
					}
				} else if ("s".equalsIgnoreCase(choice)) {
					System.out.println();
					System.out.println("Please enter CSV separator character");
					String separatorString = new SimpleConsoleInput().setPrompt(" > ").readInput();
					separatorString = separatorString == null ? "" : separatorString;
					try {
						dbImportDefinition.setSeparator(separatorString.charAt(0));
					} catch (@SuppressWarnings("unused") final Exception e) {
						System.out.println(ConsoleUtilities.getAnsiColoredText("Invalid CSV separator character", TextColor.Light_red));
					}
				} else if ("q".equalsIgnoreCase(choice)) {
					System.out.println();
					System.out.println("Please enter CSV string quote character");
					String stringQuoteCharacterString = new SimpleConsoleInput().setPrompt(" > ").readInput();
					stringQuoteCharacterString = stringQuoteCharacterString == null ? "" : stringQuoteCharacterString;
					try {
						dbImportDefinition.setStringQuote(stringQuoteCharacterString.charAt(0));
					} catch (@SuppressWarnings("unused") final Exception e) {
						System.out.println(ConsoleUtilities.getAnsiColoredText("Invalid CSV string quote character", TextColor.Light_red));
					}
				} else if ("qe".equalsIgnoreCase(choice)) {
					System.out.println();
					System.out.println("Please enter CSV string quote escape character");
					String stringQuoteEscapeCharacterString = new SimpleConsoleInput().setPrompt(" > ").readInput();
					stringQuoteEscapeCharacterString = stringQuoteEscapeCharacterString == null ? "" : stringQuoteEscapeCharacterString;
					try {
						dbImportDefinition.setEscapeStringQuote(stringQuoteEscapeCharacterString.charAt(0));
					} catch (@SuppressWarnings("unused") final Exception e) {
						System.out.println(ConsoleUtilities.getAnsiColoredText("Invalid CSV string quote character", TextColor.Light_red));
					}
				} else if ("noheaders".equalsIgnoreCase(choice)) {
					dbImportDefinition.setNoHeaders(!dbImportDefinition.isNoHeaders());
				} else if ("c".equalsIgnoreCase(choice)) {
					dbImportDefinition.setCompleteCommit(!dbImportDefinition.isCompleteCommit());
				} else if ("nonewindex".equalsIgnoreCase(choice)) {
					dbImportDefinition.setCreateNewIndexIfNeeded(!dbImportDefinition.isCreateNewIndexIfNeeded());
				} else if ("deactivatefk".equalsIgnoreCase(choice)) {
					dbImportDefinition.setDeactivateForeignKeyConstraints(!dbImportDefinition.isDeactivateForeignKeyConstraints());
				} else if ("deactivatetriggers".equalsIgnoreCase(choice)) {
					dbImportDefinition.setDeactivateTriggers(!dbImportDefinition.isDeactivateTriggers());
				} else if ("a".equalsIgnoreCase(choice)) {
					dbImportDefinition.setAllowUnderfilledLines(!dbImportDefinition.isAllowUnderfilledLines());
				} else if ("r".equalsIgnoreCase(choice)) {
					dbImportDefinition.setRemoveSurplusEmptyTrailingColumns(!dbImportDefinition.isRemoveSurplusEmptyTrailingColumns());
				} else if ("t".equalsIgnoreCase(choice)) {
					dbImportDefinition.setTrimData(!dbImportDefinition.isTrimData());
				} else if ("i".equalsIgnoreCase(choice)) {
					while (true) {
						try {
							final String importformatString = askForSelection("Please choose import mode", Stream.of(ImportMode.values()).map(Enum::name).collect(Collectors.toList()));
							if (importformatString == null) {
								getParentMenu().getMessages().add("Canceled by user");
							} else {
								dbImportDefinition.setImportMode(ImportMode.getFromString(importformatString));
							}
							break;
						} catch (final Exception e) {
							System.out.println(ConsoleUtilities.getAnsiColoredText(e.getMessage(), TextColor.Light_red));
						}
					}
				} else if ("d".equalsIgnoreCase(choice)) {
					while (true) {
						try {
							final String importformatString = askForSelection("Please choose duplicate mode", Stream.of(DuplicateMode.values()).map(Enum::name).collect(Collectors.toList()));
							if (importformatString == null) {
								getParentMenu().getMessages().add("Canceled by user");
							} else {
								dbImportDefinition.setDuplicateMode(DuplicateMode.getFromString(importformatString));
							}
							break;
						} catch (final Exception e) {
							System.out.println(ConsoleUtilities.getAnsiColoredText(e.getMessage(), TextColor.Light_red));
						}
					}
				} else if ("u".equalsIgnoreCase(choice)) {
					dbImportDefinition.setUpdateNullData(!dbImportDefinition.isUpdateNullData());
				} else if ("k".equalsIgnoreCase(choice)) {
					System.out.println();
					System.out.println("Please enter keycolumns list comma separated");
					final String keycolumns = new SimpleConsoleInput().setPrompt(" > ").readInput();
					dbImportDefinition.setKeycolumns(Utilities.splitAndTrimList(keycolumns));
				} else if ("insvalues".equalsIgnoreCase(choice)) {
					System.out.println();
					System.out.println("Please enter additional insert values");
					final String additionalInsertValues = new SimpleConsoleInput().setPrompt(" > ").readInput();
					dbImportDefinition.setAdditionalInsertValues(additionalInsertValues);
				} else if ("updvalues".equalsIgnoreCase(choice)) {
					System.out.println();
					System.out.println("Please enter additional update values");
					final String additionalUpdateValues = new SimpleConsoleInput().setPrompt(" > ").readInput();
					dbImportDefinition.setAdditionalUpdateValues(additionalUpdateValues);
				} else if ("create".equalsIgnoreCase(choice)) {
					dbImportDefinition.setCreateTable(!dbImportDefinition.isCreateTable());
				} else if ("structure".equalsIgnoreCase(choice)) {
					System.out.println();
					System.out.println("Please enter structure file path");
					final String structureFilePath = new FilepathConsoleInput().setPrompt(" > ").readInput();
					dbImportDefinition.setStructureFilePath(Utilities.isBlank(structureFilePath) ? null : structureFilePath);
				} else if ("logerrors".equalsIgnoreCase(choice)) {
					dbImportDefinition.setLogErroneousData(!dbImportDefinition.isLogErroneousData());
				} else if ("zippassword".equalsIgnoreCase(choice)) {
					System.out.println();
					System.out.println("Please enter zip password");
					final char[] zipPasswordArray = new PasswordConsoleInput().setPrompt(" > ").readInput();
					dbImportDefinition.setZipPassword(zipPasswordArray);
				} else if ("kdbxpassword".equalsIgnoreCase(choice)) {
					System.out.println();
					System.out.println("Please enter KDBX password");
					final char[] kdbxPasswordArray = new PasswordConsoleInput().setPrompt(" > ").readInput();
					dbImportDefinition.setKdbxPassword(kdbxPasswordArray);
				} else if ("dbtz".equalsIgnoreCase(choice)) {
					while (true) {
						System.out.println();
						System.out.println("Please enter database timezone");
						String dbtzString = new SimpleConsoleInput().setPrompt(" > ").readInput();
						dbtzString = dbtzString == null ? "" : dbtzString.trim();
						if (Utilities.isBlank(dbtzString)) {
							getParentMenu().getMessages().add("Canceled by user");
							return 0;
						} else {
							try {
								dbImportDefinition.setDatabaseTimeZone(TimeZone.getTimeZone(dbtzString).toString());
								break;
							} catch (@SuppressWarnings("unused") final Exception e) {
								System.out.println(ConsoleUtilities.getAnsiColoredText("Unsupported timezone: " + dbtzString, TextColor.Light_red));
							}
						}
					}
				} else if ("idtz".equalsIgnoreCase(choice)) {
					while (true) {
						System.out.println();
						System.out.println("Please enter import data timezone");
						String idtzString = new SimpleConsoleInput().setPrompt(" > ").readInput();
						idtzString = idtzString == null ? "" : idtzString.trim();
						if (Utilities.isBlank(idtzString)) {
							getParentMenu().getMessages().add("Canceled by user");
							return 0;
						} else {
							try {
								dbImportDefinition.setImportDataTimeZone(TimeZone.getTimeZone(idtzString).toString());
								break;
							} catch (@SuppressWarnings("unused") final Exception e) {
								System.out.println(ConsoleUtilities.getAnsiColoredText("Unsupported timezone: " + idtzString, TextColor.Light_red));
							}
						}
					}
				} else if ("params".equalsIgnoreCase(choice)) {
					getParentMenu().getMessages().add("Parameters: " + dbImportDefinition.toParamsString());
					return 0;
				} else if ("start".equalsIgnoreCase(choice)) {
					return -1;
				} else {
					System.out.println(ConsoleUtilities.getAnsiColoredText("Invalid selection: " + choice, TextColor.Light_red));
				}
			}
		} catch (final Exception e) {
			e.printStackTrace();
			System.exit(1);
			return 0;
		}
	}
}
