package de.soderer.utilities;

import java.awt.HeadlessException;
import java.io.Console;
import java.text.MessageFormat;
import java.time.LocalDateTime;
import java.util.Locale;

import de.soderer.utilities.appupdate.ApplicationUpdateParent;
import de.soderer.utilities.console.ConsoleType;
import de.soderer.utilities.console.ConsoleUtilities;
import de.soderer.utilities.console.PasswordConsoleInput;
import de.soderer.utilities.console.SimpleConsoleInput;
import de.soderer.utilities.worker.WorkerSimple;

public class UpdateableConsoleApplication implements ApplicationUpdateParent {
	private final String applicationName;
	private final Version applicationVersion;

	public UpdateableConsoleApplication(final String applicationName, final Version applicationVersion) throws HeadlessException {
		this.applicationName = applicationName;
		this.applicationVersion = applicationVersion;
	}

	@Override
	public boolean askForUpdate(final Version availableNewVersion) throws Exception {
		final Console console = System.console();
		if (console == null) {
			throw new Exception("Couldn't get Console instance");
		}
		ConsoleUtilities.clearScreen();
		if (availableNewVersion == null) {
			System.out.println(getI18NString("noNewerVersion", applicationName, applicationVersion.toString()));
			System.out.println();
			return false;
		} else {
			System.out.println(getI18NString("newApplicationVersion", availableNewVersion.toString(), applicationName, applicationVersion.toString()));
			final String input = new SimpleConsoleInput().setPrompt(getI18NString("installUpdate") + ": ").readInput();
			System.out.println();
			return input != null && (input.toLowerCase().startsWith("y") || input.toLowerCase().startsWith("j"));
		}
	}

	@Override
	public Credentials aquireCredentials(final String text, final boolean aquireUsername, final boolean aquirePassword, final boolean firstRequest) throws Exception {
		final Console console = System.console();
		if (console == null) {
			throw new Exception("Couldn't get Console instance");
		}

		String userName = null;
		char[] password = null;

		if (aquireUsername && aquirePassword) {
			userName = new SimpleConsoleInput().setPrompt(getI18NString("enterUsername") + ": ").readInput();
			password = new PasswordConsoleInput().setPrompt(getI18NString("enterPassword") + ": ").readInput();
		} else if (aquireUsername) {
			userName = new SimpleConsoleInput().setPrompt(getI18NString("enterUsername") + ": ").readInput();
		} else if (aquirePassword) {
			password = new PasswordConsoleInput().setPrompt(getI18NString("enterPassword") + ": ").readInput();
		}

		if (Utilities.isBlank(userName) && Utilities.isBlank(password)) {
			return null;
		} else {
			return new Credentials(userName, password);
		}
	}

	@Override
	public void showUpdateError(final String errorText) {
		System.err.println(errorText);
	}

	@Override
	public void showUpdateProgress(final LocalDateTime itemStart, final long itemsToDo, final long itemsDone) {
		if (ConsoleUtilities.getConsoleType() == ConsoleType.ANSI) {
			ConsoleUtilities.saveCurrentCursorPosition();

			System.out.print(ConsoleUtilities.getConsoleProgressString(80 - 1, itemStart, itemsToDo, itemsDone));

			ConsoleUtilities.moveCursorToSavedPosition();
		} else if (ConsoleUtilities.getConsoleType() == ConsoleType.ANSI) {
			System.out.print(ConsoleUtilities.getConsoleProgressString(80 - 1, itemStart, itemsToDo, itemsDone) + "\n");
		} else {
			System.out.print("\r" + ConsoleUtilities.getConsoleProgressString(80 - 1, itemStart, itemsToDo, itemsDone) + "\r");
		}
	}

	@Override
	public void showUpdateDone(final LocalDateTime startTime, final LocalDateTime endTime, final long itemsDone) {
		System.out.println();
		System.out.println(getI18NString("updateDone"));
		System.out.println();
	}

	@Override
	public void showUpdateDownloadStart(final WorkerSimple<Boolean> worker) {
		// Do nothing
	}

	@Override
	public void showUpdateDownloadEnd(final LocalDateTime startTime, final LocalDateTime endTime, final long itemsDone) {
		// Do nothing
	}

	private static String getI18NString(final String resourceKey, final Object... arguments) {
		if (LangResources.existsKey(resourceKey)) {
			return LangResources.get(resourceKey, arguments);
		} else if ("de".equalsIgnoreCase(Locale.getDefault().getLanguage())) {
			String pattern;
			switch(resourceKey) {
				case "noNewerVersion": pattern = "Es ist keine neuere Version verfügbar für {0}.\nDie aktuelle lokale Version ist {1}."; break;
				case "newApplicationVersion": pattern = "Es ist eine neue Version {0} verfügbar für {1}.\nDie aktuelle lokale Version ist {2}."; break;
				case "installUpdate": pattern = "Update installieren? (jN)"; break;
				case "enterUsername": pattern = "Bitte Usernamen eingeben"; break;
				case "enterPassword": pattern = "Bitte Passwort eingeben"; break;
				case "updateDone": pattern = "Update beendet"; break;
				default: pattern = "MessageKey unbekannt: " + resourceKey + (arguments != null && arguments.length > 0 ? " Argumente: " + Utilities.join(arguments, ", ") : "");
			}
			return MessageFormat.format(pattern, arguments);
		} else {
			String pattern;
			switch(resourceKey) {
				case "noNewerVersion": pattern = "There is no newer version available for {0}.\nThe current local version is {1}."; break;
				case "newApplicationVersion": pattern = "New version {0} is available for {1}.\nThe current local version is {2}."; break;
				case "installUpdate": pattern = "Install update? (yN)"; break;
				case "enterUsername": pattern = "Please enter username"; break;
				case "enterPassword": pattern = "Please enter password"; break;
				case "updateDone": pattern = "Update done"; break;
				default: pattern = "MessageKey unknown: " + resourceKey + (arguments != null && arguments.length > 0 ? " arguments: " + Utilities.join(arguments, ", ") : "");
			}
			return MessageFormat.format(pattern, arguments);
		}
	}
}
