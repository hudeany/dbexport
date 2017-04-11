package de.soderer.utilities;

import java.awt.HeadlessException;
import java.io.Console;
import java.text.MessageFormat;
import java.util.Date;
import java.util.Locale;

public class BasicUpdateableConsoleApplication implements UpdateParent {
	private String applicationName;
	private Version applicationVersion;
	
	public BasicUpdateableConsoleApplication(String applicationName, Version applicationVersion) throws HeadlessException {
		this.applicationName = applicationName;
		this.applicationVersion = applicationVersion;
	}

	@Override
	public boolean askForUpdate(String availableNewVersion) throws Exception {
		Console console = System.console();
		if (console == null) {
			throw new Exception("Couldn't get Console instance");
		}
		if (availableNewVersion == null) {
			System.out.println(getI18NString("noNewerVersion", applicationName, applicationVersion.toString()));
			System.out.println();
			return false;
		} else {
			System.out.println(getI18NString("newVersion", availableNewVersion, applicationName, applicationVersion.toString()));
			String input = console.readLine(getI18NString("installUpdate") + ": ");
			System.out.println();
			return input != null && (input.toLowerCase().startsWith("y") || input.toLowerCase().startsWith("j"));
		}
	}

	@Override
	public Credentials aquireCredentials(String text, boolean aquireUsername, boolean aquirePassword) throws Exception {
		Console console = System.console();
		if (console == null) {
			throw new Exception("Couldn't get Console instance");
		}

		String userName = null;
		char[] password = null;
		
		if (aquireUsername && aquirePassword) {
			userName = console.readLine(getI18NString("enterUsername") + ": ");
			password = console.readPassword(getI18NString("enterPassword") + ": ");
		} else if (aquireUsername) {
			userName = console.readLine(getI18NString("enterUsername") + ": ");
		} else if (aquirePassword) {
			password = console.readPassword(getI18NString("enterPassword") + ": ");
		}
		
		if (Utilities.isBlank(userName) && Utilities.isBlank(password)) {
			return null;
		} else {
			return new Credentials(userName, password);
		}
	}

	@Override
	public void showUpdateError(String errorText) {
		System.err.println(errorText);
	}

	@Override
	public void showUpdateProgress(Date itemStart, long itemsToDo, long itemsDone) {
		System.out.print("\r" + Utilities.getConsoleProgressString(80, itemStart, itemsToDo, itemsDone));
	}

	@Override
	public void showUpdateDone() {
		System.out.println();
		System.out.println(getI18NString("updateDone"));
		System.out.println();
	}

	@Override
	public void showUpdateDownloadStart() {
	}

	@Override
	public void showUpdateDownloadEnd() {
		// Do nothing
	}
	
	private String getI18NString(String resourceKey, Object... arguments) {
		if (LangResources.existsKey(resourceKey)) {
			return LangResources.get(resourceKey, arguments);
		} else if ("de".equalsIgnoreCase(Locale.getDefault().getLanguage())) {
			String pattern;
			switch(resourceKey) {
				case "noNewerVersion": pattern = "Es ist keine neuere Version verfügbar für {0}.\nDie aktuelle lokale Version ist {1}."; break;
				case "newVersion": pattern = "Es ist die eine neue Version {0} verfügbar für {1}.\nDie aktuelle lokale Version ist {2}."; break;
				case "installUpdate": pattern = "Update installieren? (jN)"; break;
				case "enterUsername": pattern = "Bitte Usernamen eingeben"; break;
				case "enterPassword": pattern = "Bitte Passwort eingeben"; break;
				case "updateDone": pattern = "Update beendet"; break;
				default: pattern = "MessageKey unbekannt: " + resourceKey + " Argumente: " + Utilities.join(arguments, ", ");
			}
			return MessageFormat.format(pattern, arguments);
		} else {
			String pattern;
			switch(resourceKey) {
				case "noNewerVersion": pattern = "There is no newer version available for {0}.\nThe current local version is {1}."; break;
				case "newVersion": pattern = "New version {0} is available for {1}.\nThe current local version is {2}."; break;
				case "installUpdate": pattern = "Install update? (yN)"; break;
				case "enterUsername": pattern = "Please enter username"; break;
				case "enterPassword": pattern = "Please enter password"; break;
				case "updateDone": pattern = "Update done"; break;
				default: pattern = "MessageKey unknown: " + resourceKey + " arguments: " + Utilities.join(arguments, ", ");
			}
			return MessageFormat.format(pattern, arguments);
		}
	}
}
