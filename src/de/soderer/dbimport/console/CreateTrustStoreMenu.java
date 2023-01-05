package de.soderer.dbimport.console;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import de.soderer.dbimport.ConnectionTestDefinition;
import de.soderer.dbimport.DbDefinition;
import de.soderer.dbimport.DbImport;
import de.soderer.utilities.DbUtilities.DbVendor;
import de.soderer.utilities.FileUtilities;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.console.ConsoleMenu;
import de.soderer.utilities.console.ConsoleUtilities;
import de.soderer.utilities.console.ConsoleUtilities.TextColor;
import de.soderer.utilities.console.PasswordConsoleInput;
import de.soderer.utilities.console.SimpleConsoleInput;

public class CreateTrustStoreMenu extends ConsoleMenu {
	private ConnectionTestDefinition connectionTestDefinition = new ConnectionTestDefinition();
	private DbDefinition dbDefinitionCache = null;

	public ConnectionTestDefinition getConnectionTestDefinition() {
		return connectionTestDefinition;
	}

	public void setConnectionTestDefinition(final ConnectionTestDefinition connectionTestDefinition) {
		this.connectionTestDefinition = connectionTestDefinition;
	}

	public CreateTrustStoreMenu(final ConsoleMenu parentMenu, final DbDefinition dbDefinitionCache) throws Exception {
		super(parentMenu, "Create TrustStore");

		this.dbDefinitionCache = dbDefinitionCache;
	}

	@Override
	public int show() throws Exception {
		try {
			connectionTestDefinition.importParameters(dbDefinitionCache);

			ConsoleUtilities.clearScreen();
			ConsoleUtilities.printBoxed(DbImport.APPLICATION_NAME + " (v" + DbImport.VERSION.toString() + ")");
			System.out.println();
			System.out.println("Create TrustStore");
			System.out.println();
			printMessages();

			while (true) {
				while (Utilities.isBlank(connectionTestDefinition.getHostname())) {
					System.out.println();
					System.out.println("Please enter db hostname and optional port separated by ':' (Default port is 443, Blank => Cancel)");
					String choice = new SimpleConsoleInput().setPrompt(" > ").readInput();
					choice = choice == null ? "" : choice.trim();
					if (Utilities.isBlank(choice)) {
						getParentMenu().getMessages().add("Canceled by user");
						return 0;
					} else {
						connectionTestDefinition.setHostname(choice);
					}
				}

				while (Utilities.isBlank(connectionTestDefinition.getTrustStoreFilePath())) {
					System.out.println();
					System.out.println("Please enter db TrustStore filepath (Blank => Cancel)");
					String choice = new SimpleConsoleInput().setPrompt(" > ").readInput();
					choice = choice == null ? "" : choice.trim();
					if (Utilities.isBlank(choice)) {
						getParentMenu().getMessages().add("Canceled by user");
						return 0;
					} else if (!FileUtilities.isValidFilePath(choice)) {
						System.out.println(ConsoleUtilities.getAnsiColoredText("Not a valid filepath", TextColor.Light_red));
					} else if (new File(choice).exists()) {
						System.out.println(ConsoleUtilities.getAnsiColoredText("Filepath already exists", TextColor.Light_red));
					} else {
						connectionTestDefinition.setTrustStoreFilePath(choice);
					}
				}

				ConsoleUtilities.clearScreen();

				ConsoleUtilities.printBoxed(DbImport.APPLICATION_NAME + " (v" + DbImport.VERSION.toString() + ")");

				System.out.println();

				System.out.println("Change parameters or start creation of TrustStore");
				System.out.println();
				printMessages();

				final int bulletSize = 20;

				final List<String> autoCompletionStrings = new ArrayList<>();
				autoCompletionStrings.add("");

				System.out.println("  " + Utilities.rightPad("Hostname (and port):", bulletSize) + " " + connectionTestDefinition.getHostname());
				System.out.println("  " + Utilities.rightPad("TrustStore filepath:", bulletSize) + " " + connectionTestDefinition.getTrustStoreFilePath());
				System.out.println("  " + Utilities.rightPad("TrustStore password:", bulletSize) + " " + (connectionTestDefinition.getTrustStorePassword() == null ? "<empty>" : "***"));

				System.out.println();
				System.out.println("  " + Utilities.rightPad("reset)", bulletSize) + " " + "Reset data");
				autoCompletionStrings.add("reset");
				System.out.println("  " + Utilities.rightPad("password)", bulletSize) + " " + "Define TrustStore password (Optional)");
				autoCompletionStrings.add("password");
				System.out.println("  " + Utilities.rightPad("params)", bulletSize) + " " + "Print parameters for later use (Includes passwords)");
				autoCompletionStrings.add("params");
				System.out.println("  " + Utilities.rightPad("start)", bulletSize) + " " + "Start TrustStore creation");
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
				} else if ("reset".equalsIgnoreCase(choice)) {
					connectionTestDefinition.setDbVendor((DbVendor) null);
					connectionTestDefinition.setHostname(null);
					connectionTestDefinition.setDbName(null);
					connectionTestDefinition.setUsername(null);
					connectionTestDefinition.setPassword(null);
					connectionTestDefinition.setTrustStoreFilePath(null);
					connectionTestDefinition.setTrustStorePassword(null);
				} else if ("password".equalsIgnoreCase(choice)) {
					System.out.println();
					System.out.println("Please enter TrustStore password (Blank => Empty)");
					final char[] passwordArray = new PasswordConsoleInput().setPrompt(" > ").readInput();
					connectionTestDefinition.setTrustStorePassword(Utilities.isNotEmpty(passwordArray) ? passwordArray : null);
				} else if ("params".equalsIgnoreCase(choice)) {
					String params = "createtruststore";
					params += " " + connectionTestDefinition.getHostname();
					params += " " + connectionTestDefinition.getTrustStoreFilePath();
					if (connectionTestDefinition.getTrustStorePassword() != null) {
						params += " \"" + new String(connectionTestDefinition.getTrustStorePassword()) + "\"";
					}
					getParentMenu().getMessages().add("Parameters: " + params);
					return 0;
				} else if ("start".equalsIgnoreCase(choice)) {
					return -5;
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
