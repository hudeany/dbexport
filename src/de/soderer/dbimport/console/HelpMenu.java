package de.soderer.dbimport.console;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import de.soderer.dbimport.DbImport;
import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.IoUtilities;
import de.soderer.utilities.console.ConsoleMenu;
import de.soderer.utilities.console.ConsoleUtilities;

public class HelpMenu extends ConsoleMenu {
	public HelpMenu(final ConsoleMenu parentMenu) throws Exception {
		super(parentMenu, "Print help text");
	}

	@Override
	public int show() throws Exception {
		ConsoleUtilities.clearScreen();
		try (InputStream helpInputStream = DbImport.class.getResourceAsStream(DbImport.HELP_RESOURCE_FILE)) {
			System.out.println("DbImport (by Andreas Soderer, mail: dbimport@soderer.de)\n"
					+ "VERSION: " + DbImport.VERSION.toString() + " (" + DateUtilities.formatDate(DateUtilities.YYYY_MM_DD_HHMMSS, DbImport.VERSION_BUILDTIME) + ")" + "\n\n"
					+ new String(IoUtilities.toByteArray(helpInputStream), StandardCharsets.UTF_8));
		} catch (final Exception e) {
			System.out.println("Help info is missing");
		}
		System.exit(0);
		return 0;
	}
}
