package de.soderer.dbexport.console;

import java.io.File;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import de.soderer.dbexport.DbExport;
import de.soderer.dbexport.DbExportDefinition;
import de.soderer.dbexport.DbExportDefinition.DataType;
import de.soderer.utilities.DbUtilities.DbVendor;
import de.soderer.utilities.FileUtilities;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.console.ConsoleMenu;
import de.soderer.utilities.console.ConsoleUtilities;
import de.soderer.utilities.console.ConsoleUtilities.TextColor;
import de.soderer.utilities.console.PasswordConsoleInput;
import de.soderer.utilities.console.SimpleConsoleInput;

public class ExportMenu extends ConsoleMenu {
	private DbExportDefinition dbExportDefinition = new DbExportDefinition();

	public DbExportDefinition getDbExportDefinition() {
		return dbExportDefinition;
	}

	public void setDbExportDefinition(final DbExportDefinition dbExportDefinition) {
		this.dbExportDefinition = dbExportDefinition;
	}

	public ExportMenu(final ConsoleMenu parentMenu) throws Exception {
		super(parentMenu, "Export data");
	}

	@Override
	public int show() throws Exception {
		try {
			ConsoleUtilities.clearScreen();

			ConsoleUtilities.printBoxed(DbExport.APPLICATION_NAME + " (v" + DbExport.VERSION.toString() + ")");

			System.out.println();

			System.out.println("Export data");
			System.out.println();
			printMessages();

			while (true) {
				while (dbExportDefinition.getDbVendor() == null) {
					try {
						final String dbVendorString = askForSelection("Please choose DB vendor", Stream.of(DbVendor.values()).map(Enum::name).collect(Collectors.toList()));
						if (dbVendorString == null) {
							getParentMenu().getMessages().add("Canceled by user");
							return 0;
						} else {
							dbExportDefinition.setDbVendor(DbVendor.getDbVendorByName(dbVendorString));
						}
					} catch (final Exception e) {
						System.out.println(ConsoleUtilities.getAnsiColoredText(e.getMessage(), TextColor.Light_red));
					}
				}

				if (Utilities.isBlank(dbExportDefinition.getHostname()) && dbExportDefinition.getDbVendor() != DbVendor.SQLite && dbExportDefinition.getDbVendor() != DbVendor.HSQL && dbExportDefinition.getDbVendor() != DbVendor.Derby) {
					System.out.println();
					System.out.println("Please enter db hostname and optional port separated by ':' (No port uses db vendors default port, Blank => Cancel)");
					String choice = new SimpleConsoleInput().setPrompt(" > ").readInput();
					choice = choice == null ? "" : choice.trim();
					if (Utilities.isBlank(choice)) {
						getParentMenu().getMessages().add("Canceled by user");
						return 0;
					} else {
						dbExportDefinition.setHostname(choice);
					}
				}

				if (dbExportDefinition.getDbVendor() == DbVendor.SQLite || dbExportDefinition.getDbVendor() == DbVendor.Derby) {
					while (Utilities.isBlank(dbExportDefinition.getDbName())) {
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
							dbExportDefinition.setDbName(choice);
						}
					}
				} else {
					while (Utilities.isBlank(dbExportDefinition.getDbName())) {
						System.out.println();
						System.out.println("Please enter db name (Blank => Cancel)");
						String choice = new SimpleConsoleInput().setPrompt(" > ").readInput();
						choice = choice == null ? "" : choice.trim();
						if (Utilities.isBlank(choice)) {
							getParentMenu().getMessages().add("Canceled by user");
							return 0;
						} else {
							dbExportDefinition.setDbName(choice);
						}
					}
				}

				if (Utilities.isBlank(dbExportDefinition.getUsername()) && dbExportDefinition.getDbVendor() != DbVendor.SQLite && dbExportDefinition.getDbVendor() != DbVendor.Derby) {
					System.out.println();
					System.out.println("Please enter db username (Blank => Cancel)");
					String choice = new SimpleConsoleInput().setPrompt(" > ").readInput();
					choice = choice == null ? "" : choice.trim();
					if (Utilities.isBlank(choice)) {
						if (dbExportDefinition.getDbVendor() == DbVendor.Cassandra) {
							dbExportDefinition.setUsername(null);
						} else {
							getParentMenu().getMessages().add("Canceled by user");
							return 0;
						}
					} else {
						dbExportDefinition.setUsername(choice);
					}
				}

				if (Utilities.isBlank(dbExportDefinition.getPassword()) && dbExportDefinition.getDbVendor() != DbVendor.SQLite && dbExportDefinition.getDbVendor() != DbVendor.HSQL && dbExportDefinition.getDbVendor() != DbVendor.Derby && (dbExportDefinition.getDbVendor() != DbVendor.Cassandra || dbExportDefinition.getUsername() != null)) {
					System.out.println();
					System.out.println("Please enter db password (Blank => Cancel)");
					final char[] passwordArray = new PasswordConsoleInput().setPrompt(" > ").readInput();
					if (Utilities.isBlank(passwordArray)) {
						getParentMenu().getMessages().add("Canceled by user");
						return 0;
					}
					dbExportDefinition.setPassword(passwordArray);
				}

				while (Utilities.isBlank(dbExportDefinition.getSqlStatementOrTablelist())) {
					System.out.println();
					System.out.println("Please enter export statement or comma-separated table list (tablename wildcards *? and !(=not, use as tablename prefix), Blank => Cancel)");
					String choice = new SimpleConsoleInput().setPrompt(" > ").readInput();
					choice = choice == null ? "" : choice.trim();
					if (Utilities.isBlank(choice)) {
						getParentMenu().getMessages().add("Canceled by user");
						return 0;
					} else {
						dbExportDefinition.setSqlStatementOrTablelist(choice);
					}
				}

				while (Utilities.isBlank(dbExportDefinition.getOutputpath())) {
					System.out.println();
					System.out.println("Please enter export filepath ('console' for output to terminal, Blank => Cancel)");
					String choice = new SimpleConsoleInput().setPrompt(" > ").readInput();
					choice = choice == null ? "" : choice.trim();
					if (Utilities.isBlank(choice)) {
						getParentMenu().getMessages().add("Canceled by user");
						return 0;
					} else if (!"console".equalsIgnoreCase(choice) && new File(choice).exists() && new File(choice).isFile()) {
						System.out.println(ConsoleUtilities.getAnsiColoredText("Filepath already exist", TextColor.Light_red));
					} else if (!"console".equalsIgnoreCase(choice) && !new File(choice).getParentFile().exists()) {
						System.out.println(ConsoleUtilities.getAnsiColoredText("Parent directory does not exist", TextColor.Light_red));
					} else {
						dbExportDefinition.setOutputpath(choice);
					}
				}

				ConsoleUtilities.clearScreen();

				ConsoleUtilities.printBoxed(DbExport.APPLICATION_NAME + " (v" + DbExport.VERSION.toString() + ")");

				System.out.println();

				System.out.println("Change parameters or start export");
				System.out.println();
				printMessages();

				final int bulletSize = 19;
				final int nameSize = 47;

				final List<String> autoCompletionStrings = new ArrayList<>();
				autoCompletionStrings.add("");

				System.out.println("  " + Utilities.rightPad("DbVendor:", bulletSize) + " " + dbExportDefinition.getDbVendor().name());
				if (dbExportDefinition.getDbVendor() != DbVendor.SQLite && dbExportDefinition.getDbVendor() != DbVendor.HSQL && dbExportDefinition.getDbVendor() != DbVendor.Derby) {
					System.out.println("  " + Utilities.rightPad("Hostname:", bulletSize) + " " + dbExportDefinition.getHostname());
				}
				if (dbExportDefinition.getDbVendor() == DbVendor.SQLite || dbExportDefinition.getDbVendor() == DbVendor.Derby) {
					System.out.println("  " + Utilities.rightPad("Db filepath:", bulletSize) + " " + dbExportDefinition.getDbName());
				} else {
					System.out.println("  " + Utilities.rightPad("Db name:", bulletSize) + " " + dbExportDefinition.getDbName());
				}
				if (dbExportDefinition.getDbVendor() != DbVendor.SQLite && dbExportDefinition.getDbVendor() != DbVendor.Derby) {
					System.out.println("  " + Utilities.rightPad("Db username:", bulletSize) + " " + (dbExportDefinition.getUsername() == null ? "<empty>" : dbExportDefinition.getUsername()));
				}
				System.out.println("  " + Utilities.rightPad("Db password:", bulletSize) + " " + (dbExportDefinition.getPassword() == null ? "<empty>" : "***"));

				if (dbExportDefinition.getDbVendor() == DbVendor.Oracle || dbExportDefinition.getDbVendor() == DbVendor.MySQL || dbExportDefinition.getDbVendor() == DbVendor.MariaDB) {
					System.out.println("  " + Utilities.rightPad("Secure connection:", bulletSize) + " " + (dbExportDefinition.getSecureConnection() ? "yes" : "no"));
					System.out.println("  " + Utilities.rightPad("TrustStore filepath:", bulletSize) + " " + (Utilities.isBlank(dbExportDefinition.getTrustStoreFilePath()) ? "<none>" : dbExportDefinition.getTrustStoreFilePath()));
					System.out.println("  " + Utilities.rightPad("TrustStore password:", bulletSize) + " " + (dbExportDefinition.getTrustStorePassword() == null ? "<empty>" : "***"));
					System.out.println();
				}

				System.out.println("  " + Utilities.rightPad("SQL statement:", bulletSize) + " " + dbExportDefinition.getSqlStatementOrTablelist());
				System.out.println("  " + Utilities.rightPad("Output filepath:", bulletSize) + " " + dbExportDefinition.getOutputpath());
				System.out.println();

				System.out.println("  " + Utilities.rightPad("reset)", bulletSize) + " " + "Reset basic db parameters");
				autoCompletionStrings.add("reset");
				System.out.println();

				if (dbExportDefinition.getDbVendor() == DbVendor.Oracle || dbExportDefinition.getDbVendor() == DbVendor.MySQL || dbExportDefinition.getDbVendor() == DbVendor.MariaDB) {
					System.out.println("  " + Utilities.rightPad("secure)", bulletSize) + " " + "Change setting for secure connection via SSL/TLS");
					autoCompletionStrings.add("secure");
					System.out.println("  " + Utilities.rightPad("truststore)", bulletSize) + " " + "Define TrustStore filepath (Optional)");
					autoCompletionStrings.add("truststore");
					System.out.println("  " + Utilities.rightPad("truststorepassword)", bulletSize) + " " + "Define TrustStore password");
					autoCompletionStrings.add("truststorepassword");
					System.out.println();
				}

				System.out.println();

				System.out.println("  " + Utilities.rightPad("x)", bulletSize) + " " + Utilities.rightPad("Exportformat:", nameSize) + dbExportDefinition.getDataType().name());
				autoCompletionStrings.add("x");
				if (dbExportDefinition.getDataType() == DataType.CSV || dbExportDefinition.getDataType() == DataType.XML) {
					System.out.println("  " + Utilities.rightPad("n)", bulletSize) + " " + Utilities.rightPad("Null value string:", nameSize) + "'" + dbExportDefinition.getNullValueString() + "'");
					autoCompletionStrings.add("n");
				}
				System.out.println("  " + Utilities.rightPad("file)", bulletSize) + " " + Utilities.rightPad("Read statement from file:", nameSize) + dbExportDefinition.isStatementFile());
				autoCompletionStrings.add("file");
				System.out.println("  " + Utilities.rightPad("l)", bulletSize) + " " + Utilities.rightPad("Log export information in .log files:", nameSize) + dbExportDefinition.isLog());
				autoCompletionStrings.add("l");
				System.out.println("  " + Utilities.rightPad("v)", bulletSize) + " " + Utilities.rightPad("Verbose terminal output:", nameSize) + dbExportDefinition.isVerbose());
				autoCompletionStrings.add("v");
				if (!"console".equalsIgnoreCase(dbExportDefinition.getOutputpath())) {
					System.out.println("  " + Utilities.rightPad("z)", bulletSize) + " " + Utilities.rightPad("Output as zipfile (Not for console output):", nameSize) + dbExportDefinition.isZip());
					autoCompletionStrings.add("z");
					if (dbExportDefinition.isZip()) {
						System.out.println("  " + Utilities.rightPad("zippassword)", bulletSize) + " " + Utilities.rightPad("Zip file password:", nameSize) + (dbExportDefinition.getZipPassword() == null ? "<empty>" : "***"));
						autoCompletionStrings.add("zippassword");
						if (dbExportDefinition.getZipPassword() != null) {
							System.out.println("  " + Utilities.rightPad("zipcrypto)", bulletSize) + " " + Utilities.rightPad("Use weak ZipCrypto:", nameSize) + dbExportDefinition.isUseZipCrypto());
							autoCompletionStrings.add("zipcrypto");
						}
					}
				}
				System.out.println("  " + Utilities.rightPad("e)", bulletSize) + " " + Utilities.rightPad("Output encoding:", nameSize) + dbExportDefinition.getEncoding());
				autoCompletionStrings.add("e");
				if (dbExportDefinition.getDataType() == DataType.CSV || dbExportDefinition.getDataType() == DataType.XML) {
					System.out.println("  " + Utilities.rightPad("s)", bulletSize) + " " + Utilities.rightPad("CSV separator character:", nameSize) + dbExportDefinition.getSeparator());
					autoCompletionStrings.add("s");
					System.out.println("  " + Utilities.rightPad("q)", bulletSize) + " " + Utilities.rightPad("CSV string quote character:", nameSize) + dbExportDefinition.getStringQuote());
					autoCompletionStrings.add("q");
					System.out.println("  " + Utilities.rightPad("qe)", bulletSize) + " " + Utilities.rightPad("CSV string quote escape character:", nameSize) + dbExportDefinition.getStringQuoteEscapeCharacter());
					autoCompletionStrings.add("qe");
					System.out.println("  " + Utilities.rightPad("a)", bulletSize) + " " + Utilities.rightPad("Always quote CSV value:", nameSize) + dbExportDefinition.isAlwaysQuote());
					autoCompletionStrings.add("a");
					System.out.println("  " + Utilities.rightPad("noheaders)", bulletSize) + " " + Utilities.rightPad("Don't export CSV headers:", nameSize) + dbExportDefinition.isNoHeaders());
					autoCompletionStrings.add("noheaders");
				}
				if (dbExportDefinition.getDataType() == DataType.JSON || dbExportDefinition.getDataType() == DataType.XML) {
					System.out.println("  " + Utilities.rightPad("i)", bulletSize) + " " + Utilities.rightPad("Indentation string:", nameSize) + dbExportDefinition.getIndentation());
					autoCompletionStrings.add("i");
				}
				if (dbExportDefinition.getDbVendor() != DbVendor.SQLite) {
					System.out.println("  " + Utilities.rightPad("f)", bulletSize) + " " + Utilities.rightPad("Number and datetime format locale:", nameSize) + dbExportDefinition.getDateFormatLocale());
					autoCompletionStrings.add("f");
				}
				System.out.println("  " + Utilities.rightPad("blobfiles)", bulletSize) + " " + Utilities.rightPad("Create blob files:", nameSize) + dbExportDefinition.isCreateBlobFiles());
				autoCompletionStrings.add("blobfiles");
				System.out.println("  " + Utilities.rightPad("clobfiles)", bulletSize) + " " + Utilities.rightPad("Create clob files:", nameSize) + dbExportDefinition.isCreateClobFiles());
				autoCompletionStrings.add("clobfiles");
				if (dbExportDefinition.getDataType() == DataType.CSV || dbExportDefinition.getDataType() == DataType.JSON) {
					System.out.println("  " + Utilities.rightPad("beautify)", bulletSize) + " " + Utilities.rightPad("Beautify output:", nameSize) + dbExportDefinition.isBeautify());
					autoCompletionStrings.add("beautify");
				}
				System.out.println("  " + Utilities.rightPad("structure)", bulletSize) + " " + Utilities.rightPad("Export the tables structure:", nameSize) + dbExportDefinition.isExportStructure());
				autoCompletionStrings.add("structure");
				System.out.println("  " + Utilities.rightPad("dbtz)", bulletSize) + " " + Utilities.rightPad("DatabaseTimeZone:", nameSize) + dbExportDefinition.getDatabaseTimeZone());
				autoCompletionStrings.add("dbtz");
				System.out.println("  " + Utilities.rightPad("edtz)", bulletSize) + " " + Utilities.rightPad("ExportDataTimeZone:", nameSize) + dbExportDefinition.getExportDataTimeZone());
				autoCompletionStrings.add("edtz");

				System.out.println();
				System.out.println("  " + Utilities.rightPad("params)", bulletSize) + " " + "Print parameters for later use (Includes passwords)");
				autoCompletionStrings.add("params");
				System.out.println("  " + Utilities.rightPad("start)", bulletSize) + " " + "Start export");
				autoCompletionStrings.add("start");

				String choice = new SimpleConsoleInput().setAutoCompletionStrings(autoCompletionStrings).setPrompt(" > ").readInput();
				choice = choice == null ? "" : choice.trim();
				if (Utilities.isBlank(choice)) {
					getParentMenu().getMessages().add("Canceled by user");
					return 0;
				} else if ("reset".equalsIgnoreCase(choice)) {
					dbExportDefinition.setDbVendor((DbVendor) null);
					dbExportDefinition.setHostname(null);
					dbExportDefinition.setUsername(null);
					dbExportDefinition.setDbName(null);
					dbExportDefinition.setDbName(null);
					dbExportDefinition.setSqlStatementOrTablelist(null);
					dbExportDefinition.setPassword(null);
					dbExportDefinition.setSecureConnection(false);
					dbExportDefinition.setTrustStoreFilePath(null);
					dbExportDefinition.setTrustStorePassword(null);
				} else if ("secure".equalsIgnoreCase(choice)) {
					dbExportDefinition.setSecureConnection(dbExportDefinition.getSecureConnection());
				} else if ("truststore".equalsIgnoreCase(choice)) {
					System.out.println();
					System.out.println("Please enter db TrustStore filepath (Blank => None)");
					String choiceTruststore = new SimpleConsoleInput().setPrompt(" > ").readInput();
					choiceTruststore = choiceTruststore == null ? "" : choiceTruststore.trim();
					if (Utilities.isBlank(choiceTruststore)) {
						dbExportDefinition.setTrustStoreFilePath(null);
					} else if (!FileUtilities.isValidFilePath(choiceTruststore)) {
						getErrors().add("Not a valid filepath");
					} else if (!new File(choiceTruststore).exists()) {
						getErrors().add("Filepath does not exist");
					} else {
						dbExportDefinition.setTrustStoreFilePath(choiceTruststore);
					}
				} else if ("truststorepassword".equalsIgnoreCase(choice)) {
					System.out.println();
					System.out.println("Please enter TrustStore password (Blank => Empty)");
					final char[] passwordArray = new PasswordConsoleInput().setPrompt(" > ").readInput();
					dbExportDefinition.setTrustStorePassword(Utilities.isNotEmpty(passwordArray) ? passwordArray : null);
				} else if ("x".equalsIgnoreCase(choice)) {
					while (true) {
						try {
							final String importformatString = askForSelection("Please select exportformat", Stream.of(DataType.values()).map(Enum::name).collect(Collectors.toList()));
							if (importformatString == null) {
								getParentMenu().getMessages().add("Canceled by user");
							} else {
								dbExportDefinition.setDataType(DataType.getFromString(importformatString));
							}
							break;
						} catch (final Exception e) {
							System.out.println(ConsoleUtilities.getAnsiColoredText(e.getMessage(), TextColor.Light_red));
						}
					}
				} else if ("n".equalsIgnoreCase(choice)) {
					System.out.println();
					System.out.println("Please enter null value string");
					final String nullValueString = new SimpleConsoleInput().setPrompt(" > ").readInput();
					dbExportDefinition.setNullValueString(nullValueString);
				} else if ("file".equalsIgnoreCase(choice)) {
					dbExportDefinition.setStatementFile(!dbExportDefinition.isStatementFile());
				} else if ("l".equalsIgnoreCase(choice)) {
					dbExportDefinition.setLog(!dbExportDefinition.isLog());
				} else if ("v".equalsIgnoreCase(choice)) {
					dbExportDefinition.setVerbose(!dbExportDefinition.isVerbose());
				} else if ("z".equalsIgnoreCase(choice)) {
					dbExportDefinition.setZip(!dbExportDefinition.isZip());
				} else if ("zippassword".equalsIgnoreCase(choice)) {
					System.out.println();
					System.out.println("Please enter zip password");
					final char[] zipPasswordArray = new PasswordConsoleInput().setPrompt(" > ").readInput();
					dbExportDefinition.setZipPassword(zipPasswordArray);
				} else if ("zipcrypto".equalsIgnoreCase(choice)) {
					dbExportDefinition.setUseZipCrypto(!dbExportDefinition.isUseZipCrypto());
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
								dbExportDefinition.setEncoding(Charset.forName(encodingString));
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
						dbExportDefinition.setSeparator(separatorString.charAt(0));
					} catch (@SuppressWarnings("unused") final Exception e) {
						System.out.println(ConsoleUtilities.getAnsiColoredText("Invalid CSV separator character", TextColor.Light_red));
					}
				} else if ("q".equalsIgnoreCase(choice)) {
					System.out.println();
					System.out.println("Please enter CSV string quote character");
					String stringQuoteCharacterString = new SimpleConsoleInput().setPrompt(" > ").readInput();
					stringQuoteCharacterString = stringQuoteCharacterString == null ? "" : stringQuoteCharacterString;
					try {
						dbExportDefinition.setStringQuote(stringQuoteCharacterString.charAt(0));
					} catch (@SuppressWarnings("unused") final Exception e) {
						System.out.println(ConsoleUtilities.getAnsiColoredText("Invalid CSV string quote character", TextColor.Light_red));
					}
				} else if ("qe".equalsIgnoreCase(choice)) {
					System.out.println();
					System.out.println("Please enter CSV string quote escape character");
					String stringQuoteEscapeCharacterString = new SimpleConsoleInput().setPrompt(" > ").readInput();
					stringQuoteEscapeCharacterString = stringQuoteEscapeCharacterString == null ? "" : stringQuoteEscapeCharacterString;
					try {
						dbExportDefinition.setStringQuoteEscapeCharacter(stringQuoteEscapeCharacterString.charAt(0));
					} catch (@SuppressWarnings("unused") final Exception e) {
						System.out.println(ConsoleUtilities.getAnsiColoredText("Invalid CSV string quote character", TextColor.Light_red));
					}
				} else if ("a".equalsIgnoreCase(choice)) {
					dbExportDefinition.setAlwaysQuote(!dbExportDefinition.isAlwaysQuote());
				} else if ("noheaders".equalsIgnoreCase(choice)) {
					dbExportDefinition.setNoHeaders(!dbExportDefinition.isNoHeaders());
				} else if ("i".equalsIgnoreCase(choice)) {
					System.out.println();
					System.out.println("Please enter indentation string");
					String indentationString = new SimpleConsoleInput().setPrompt(" > ").readInput();
					indentationString = indentationString == null ? "" : indentationString;
					dbExportDefinition.setIndentation(indentationString);
				} else if ("f".equalsIgnoreCase(choice)) {
					while (true) {
						System.out.println();
						System.out.println("Please enter output format locale");
						String localeString = new SimpleConsoleInput().setPrompt(" > ").readInput();
						localeString = localeString == null ? "" : localeString.trim();
						if (Utilities.isBlank(localeString)) {
							getParentMenu().getMessages().add("Canceled by user");
							return 0;
						} else {
							try {
								dbExportDefinition.setDateFormatLocale(Locale.forLanguageTag(localeString));
								break;
							} catch (@SuppressWarnings("unused") final Exception e) {
								System.out.println(ConsoleUtilities.getAnsiColoredText("Unsupported output format locale: " + localeString, TextColor.Light_red));
							}
						}
					}
				} else if ("blobfiles".equalsIgnoreCase(choice)) {
					dbExportDefinition.setCreateBlobFiles(!dbExportDefinition.isCreateBlobFiles());
				} else if ("clobfiles".equalsIgnoreCase(choice)) {
					dbExportDefinition.setCreateClobFiles(!dbExportDefinition.isCreateClobFiles());
				} else if ("beautify".equalsIgnoreCase(choice)) {
					dbExportDefinition.setBeautify(!dbExportDefinition.isBeautify());
				} else if ("structure".equalsIgnoreCase(choice)) {
					dbExportDefinition.setExportStructure(!dbExportDefinition.isExportStructure());
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
								dbExportDefinition.setDatabaseTimeZone(TimeZone.getTimeZone(dbtzString).toString());
								break;
							} catch (@SuppressWarnings("unused") final Exception e) {
								System.out.println(ConsoleUtilities.getAnsiColoredText("Unsupported timezone: " + dbtzString, TextColor.Light_red));
							}
						}
					}
				} else if ("edtz".equalsIgnoreCase(choice)) {
					while (true) {
						System.out.println();
						System.out.println("Please enter export data timezone");
						String edtzString = new SimpleConsoleInput().setPrompt(" > ").readInput();
						edtzString = edtzString == null ? "" : edtzString.trim();
						if (Utilities.isBlank(edtzString)) {
							getParentMenu().getMessages().add("Canceled by user");
							return 0;
						} else {
							try {
								dbExportDefinition.setExportDataTimeZone(TimeZone.getTimeZone(edtzString).toString());
								break;
							} catch (@SuppressWarnings("unused") final Exception e) {
								System.out.println(ConsoleUtilities.getAnsiColoredText("Unsupported timezone: " + edtzString, TextColor.Light_red));
							}
						}
					}
				} else if ("params".equalsIgnoreCase(choice)) {
					getParentMenu().getMessages().add("Parameters: " + dbExportDefinition.toParamsString());
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
