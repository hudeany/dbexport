package de.soderer.dbexport.console;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import de.soderer.dbexport.ConnectionTestDefinition;
import de.soderer.dbexport.DbExport;
import de.soderer.utilities.DbDefinition;
import de.soderer.utilities.DbUtilities.DbVendor;
import de.soderer.utilities.FileUtilities;
import de.soderer.utilities.NumberUtilities;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.console.ConsoleMenu;
import de.soderer.utilities.console.ConsoleUtilities;
import de.soderer.utilities.console.ConsoleUtilities.TextColor;
import de.soderer.utilities.console.PasswordConsoleInput;
import de.soderer.utilities.console.SimpleConsoleInput;

public class ConnectionTestMenu extends ConsoleMenu {
	private ConnectionTestDefinition connectionTestDefinition = new ConnectionTestDefinition();
	private DbDefinition dbDefinitionCache = null;

	public ConnectionTestDefinition getConnectionTestDefinition() {
		return connectionTestDefinition;
	}

	public void setConnectionTestDefinition(final ConnectionTestDefinition connectionTestDefinition) {
		this.connectionTestDefinition = connectionTestDefinition;
	}

	public ConnectionTestMenu(final ConsoleMenu parentMenu, final DbDefinition dbDefinitionCache) throws Exception {
		super(parentMenu, "Database connection test");

		this.dbDefinitionCache = dbDefinitionCache;
	}

	@Override
	public int show() throws Exception {
		try {
			connectionTestDefinition.importParameters(dbDefinitionCache);

			ConsoleUtilities.clearScreen();
			ConsoleUtilities.printBoxed(DbExport.APPLICATION_NAME + " (v" + DbExport.VERSION.toString() + ")");
			System.out.println();
			System.out.println("Database connection test");
			System.out.println();
			printMessages();

			while (true) {
				while (connectionTestDefinition.getDbVendor() == null) {
					try {
						final String dbVendorString = askForSelection("Please choose DB vendor", Stream.of(DbVendor.values()).map(Enum::name).collect(Collectors.toList()));
						if (dbVendorString == null) {
							getParentMenu().getMessages().add("Canceled by user");
							return 0;
						} else {
							connectionTestDefinition.setDbVendor(DbVendor.getDbVendorByName(dbVendorString));
						}
					} catch (final Exception e) {
						System.out.println(ConsoleUtilities.getAnsiColoredText(e.getMessage(), TextColor.Light_red));
					}
				}

				if (Utilities.isBlank(connectionTestDefinition.getHostnameAndPort()) && connectionTestDefinition.getDbVendor() != DbVendor.SQLite && connectionTestDefinition.getDbVendor() != DbVendor.HSQL && connectionTestDefinition.getDbVendor() != DbVendor.Derby) {
					System.out.println();
					System.out.println("Please enter db hostname and optional port separated by ':' (No port uses db vendors default port, Blank => Cancel)");
					String choice = new SimpleConsoleInput().setPrompt(" > ").readInput();
					choice = choice == null ? "" : choice.trim();
					if (Utilities.isBlank(choice)) {
						getParentMenu().getMessages().add("Canceled by user");
						return 0;
					} else {
						connectionTestDefinition.setHostnameAndPort(choice);
					}
				}

				if (connectionTestDefinition.getDbVendor() == DbVendor.SQLite || connectionTestDefinition.getDbVendor() == DbVendor.Derby) {
					while (Utilities.isBlank(connectionTestDefinition.getDbName())) {
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
							connectionTestDefinition.setDbName(choice);
						}
					}
				} else {
					while (Utilities.isBlank(connectionTestDefinition.getDbName())) {
						System.out.println();
						System.out.println("Please enter db name (Blank => Cancel)");
						String choice = new SimpleConsoleInput().setPrompt(" > ").readInput();
						choice = choice == null ? "" : choice.trim();
						if (Utilities.isBlank(choice)) {
							getParentMenu().getMessages().add("Canceled by user");
							return 0;
						} else {
							connectionTestDefinition.setDbName(choice);
						}
					}
				}

				if (Utilities.isBlank(connectionTestDefinition.getUsername()) && connectionTestDefinition.getDbVendor() != DbVendor.SQLite && connectionTestDefinition.getDbVendor() != DbVendor.Derby) {
					System.out.println();
					System.out.println("Please enter db username (Blank => Cancel)");
					String choice = new SimpleConsoleInput().setPrompt(" > ").readInput();
					choice = choice == null ? "" : choice.trim();
					if (Utilities.isBlank(choice)) {
						if (connectionTestDefinition.getDbVendor() == DbVendor.Cassandra) {
							connectionTestDefinition.setUsername(null);
						} else {
							getParentMenu().getMessages().add("Canceled by user");
							return 0;
						}
					} else {
						connectionTestDefinition.setUsername(choice);
					}
				}

				if (Utilities.isBlank(connectionTestDefinition.getPassword()) && connectionTestDefinition.getDbVendor() != DbVendor.SQLite && connectionTestDefinition.getDbVendor() != DbVendor.HSQL && connectionTestDefinition.getDbVendor() != DbVendor.Derby && (connectionTestDefinition.getDbVendor() != DbVendor.Cassandra || connectionTestDefinition.getUsername() != null)) {
					System.out.println();
					System.out.println("Please enter db password (Blank => Cancel)");
					final char[] passwordArray = new PasswordConsoleInput().setPrompt(" > ").readInput();
					if (Utilities.isBlank(passwordArray)) {
						getParentMenu().getMessages().add("Canceled by user");
						return 0;
					}
					connectionTestDefinition.setPassword(passwordArray);
				}

				ConsoleUtilities.clearScreen();

				ConsoleUtilities.printBoxed(DbExport.APPLICATION_NAME + " (v" + DbExport.VERSION.toString() + ")");

				System.out.println();

				System.out.println("Change parameters or start connection test");
				System.out.println();
				printMessages();

				final int bulletSize = 20;
				final int nameSize = 30;

				final List<String> autoCompletionStrings = new ArrayList<>();
				autoCompletionStrings.add("");

				System.out.println("  " + Utilities.rightPad("DbVendor:", bulletSize) + " " + connectionTestDefinition.getDbVendor().name());
				if (connectionTestDefinition.getDbVendor() != DbVendor.SQLite && connectionTestDefinition.getDbVendor() != DbVendor.HSQL && connectionTestDefinition.getDbVendor() != DbVendor.Derby) {
					System.out.println("  " + Utilities.rightPad("Hostname:", bulletSize) + " " + connectionTestDefinition.getHostnameAndPort());
				}
				if (connectionTestDefinition.getDbVendor() == DbVendor.SQLite || connectionTestDefinition.getDbVendor() == DbVendor.Derby) {
					System.out.println("  " + Utilities.rightPad("Db filepath:", bulletSize) + " " + connectionTestDefinition.getDbName());
				} else {
					System.out.println("  " + Utilities.rightPad("Db name:", bulletSize) + " " + connectionTestDefinition.getDbName());
				}
				if (connectionTestDefinition.getDbVendor() != DbVendor.SQLite && connectionTestDefinition.getDbVendor() != DbVendor.Derby) {
					System.out.println("  " + Utilities.rightPad("Db username:", bulletSize) + " " + (connectionTestDefinition.getUsername() == null ? "<empty>" : connectionTestDefinition.getUsername()));
				}
				System.out.println("  " + Utilities.rightPad("Db password:", bulletSize) + " " + (connectionTestDefinition.getPassword() == null ? "<empty>" : "***"));

				if (connectionTestDefinition.getDbVendor() == DbVendor.Oracle || connectionTestDefinition.getDbVendor() == DbVendor.MySQL || connectionTestDefinition.getDbVendor() == DbVendor.MariaDB) {
					System.out.println("  " + Utilities.rightPad("Secure connection:", bulletSize) + " " + (connectionTestDefinition.isSecureConnection() ? "yes" : "no"));
					System.out.println("  " + Utilities.rightPad("TrustStore filepath:", bulletSize) + " " + (connectionTestDefinition.getTrustStoreFile() == null ? "<none>" : connectionTestDefinition.getTrustStoreFile().getAbsolutePath()));
					System.out.println("  " + Utilities.rightPad("TrustStore password:", bulletSize) + " " + (connectionTestDefinition.getTrustStorePassword() == null ? "<empty>" : "***"));
				}

				System.out.println();
				System.out.println("  " + Utilities.rightPad("reset)", bulletSize) + " " + "Reset basic db parameters");
				autoCompletionStrings.add("reset");
				System.out.println();

				if (connectionTestDefinition.getDbVendor() == DbVendor.Oracle || connectionTestDefinition.getDbVendor() == DbVendor.MySQL || connectionTestDefinition.getDbVendor() == DbVendor.MariaDB) {
					System.out.println("  " + Utilities.rightPad("secure)", bulletSize) + " " + "Change setting for secure connection via SSL/TLS");
					autoCompletionStrings.add("secure");
					System.out.println("  " + Utilities.rightPad("truststore)", bulletSize) + " " + "Define TrustStore filepath (Optional)");
					autoCompletionStrings.add("truststore");
					System.out.println("  " + Utilities.rightPad("truststorepassword)", bulletSize) + " " + "Define TrustStore password");
					autoCompletionStrings.add("truststorepassword");
					System.out.println();
				}

				System.out.println("  " + Utilities.rightPad("iter)", bulletSize) + " " + Utilities.rightPad("Iterations (0 = unlimited):", nameSize) + connectionTestDefinition.getIterations());
				autoCompletionStrings.add("iter");
				System.out.println("  " + Utilities.rightPad("sleep)", bulletSize) + " " + Utilities.rightPad("Sleeptime:", nameSize) + connectionTestDefinition.getSleepTime());
				autoCompletionStrings.add("sleep");
				System.out.println("  " + Utilities.rightPad("check)", bulletSize) + " " + Utilities.rightPad("SQL statement to check:", nameSize) + (connectionTestDefinition.getCheckStatement() == null ? "" : connectionTestDefinition.getCheckStatement()));
				autoCompletionStrings.add("check");

				System.out.println();
				System.out.println("  " + Utilities.rightPad("params)", bulletSize) + " " + "Print parameters for later use (Includes passwords)");
				autoCompletionStrings.add("params");
				System.out.println("  " + Utilities.rightPad("start)", bulletSize) + " " + "Start connection test");
				autoCompletionStrings.add("start");
				System.out.println("  " + Utilities.rightPad("cancel)", bulletSize) + " " + "Cancel");
				autoCompletionStrings.add("cancel");

				String choice = new SimpleConsoleInput().setAutoCompletionStrings(autoCompletionStrings).setPrompt(" > ").readInput();
				choice = choice == null ? "" : choice.trim();
				if (Utilities.isBlank(choice)) {
					if (dbDefinitionCache != null) {
						dbDefinitionCache.importParameters(connectionTestDefinition);
					}

					getParentMenu().getMessages().add("Canceled by user");
					return 0;
				} else if ("reset".equalsIgnoreCase(choice)) {
					connectionTestDefinition.setDbVendor((DbVendor) null);
					connectionTestDefinition.setHostnameAndPort(null);
					connectionTestDefinition.setDbName(null);
					connectionTestDefinition.setUsername(null);
					connectionTestDefinition.setPassword(null);
					connectionTestDefinition.setSecureConnection(false);
					connectionTestDefinition.setTrustStoreFile(null);
					connectionTestDefinition.setTrustStorePassword(null);
				} else if ("secure".equalsIgnoreCase(choice)) {
					connectionTestDefinition.setSecureConnection(connectionTestDefinition.isSecureConnection());
				} else if ("truststore".equalsIgnoreCase(choice)) {
					System.out.println();
					System.out.println("Please enter db TrustStore filepath (Blank => None)");
					String choiceTruststore = new SimpleConsoleInput().setPrompt(" > ").readInput();
					choiceTruststore = choiceTruststore == null ? "" : choiceTruststore.trim();
					if (Utilities.isBlank(choiceTruststore)) {
						connectionTestDefinition.setTrustStoreFile(null);
					} else if (!FileUtilities.isValidFilePath(choiceTruststore)) {
						getErrors().add("Not a valid filepath");
					} else if (!new File(choiceTruststore).exists()) {
						getErrors().add("Filepath does not exist");
					} else {
						connectionTestDefinition.setHostnameAndPort(choiceTruststore);
					}
				} else if ("truststorepassword".equalsIgnoreCase(choice)) {
					System.out.println();
					System.out.println("Please enter TrustStore password (Blank => Empty)");
					final char[] passwordArray = new PasswordConsoleInput().setPrompt(" > ").readInput();
					connectionTestDefinition.setTrustStorePassword(Utilities.isNotEmpty(passwordArray) ? passwordArray : null);
				} else if ("iter".equalsIgnoreCase(choice)) {
					System.out.println();
					System.out.println("Please enter iterations (0 = unlimited, Blank => Back to Database connection test menu)");
					String choiceIterations = new SimpleConsoleInput().setPrompt(" > ").readInput();
					choiceIterations = choiceIterations == null ? "" : choiceIterations.trim();
					if (Utilities.isBlank(choiceIterations)) {
						System.out.println(ConsoleUtilities.getAnsiColoredText("Canceled by user", TextColor.Light_green));
						return 0;
					} else if (!NumberUtilities.isNumber(choiceIterations)) {
						System.out.println(ConsoleUtilities.getAnsiColoredText("Invalid integer value: " + choiceIterations, TextColor.Light_red));
					} else {
						connectionTestDefinition.setIterations(Integer.parseInt(choiceIterations));
					}
				} else if ("sleep".equalsIgnoreCase(choice)) {
					System.out.println();
					System.out.println("Please enter sleeptime seconds between iterations (Blank => Back to Database connection test menu)");
					String choiceIterations = new SimpleConsoleInput().setPrompt(" > ").readInput();
					choiceIterations = choiceIterations == null ? "" : choiceIterations.trim();
					if (Utilities.isBlank(choiceIterations)) {
						System.out.println(ConsoleUtilities.getAnsiColoredText("Canceled by user", TextColor.Light_green));
						return 0;
					} else if (!NumberUtilities.isNumber(choiceIterations)) {
						System.out.println(ConsoleUtilities.getAnsiColoredText("Invalid integer value: " + choiceIterations, TextColor.Light_red));
					} else {
						connectionTestDefinition.setSleepTime(Integer.parseInt(choiceIterations));
					}
				} else if ("check".equalsIgnoreCase(choice)) {
					System.out.println();
					System.out.println("Please enter check sql statement or 'vendor' for vendors default check statement (Blank => Connection check only)");
					String choiceCheckSql = new SimpleConsoleInput().setPrompt(" > ").readInput();
					choiceCheckSql = choiceCheckSql == null ? "" : choiceCheckSql.trim();
					if (Utilities.isBlank(choiceCheckSql)) {
						connectionTestDefinition.setCheckStatement(null);
					} else {
						connectionTestDefinition.setCheckStatement(choiceCheckSql);
					}
				} else if ("params".equalsIgnoreCase(choice)) {
					getParentMenu().getMessages().add("Parameters: " + connectionTestDefinition.toParamsString());
					return 0;
				} else if ("start".equalsIgnoreCase(choice)) {
					return -3;
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
