package de.soderer.utilities.swing;

import java.awt.Color;
import java.awt.HeadlessException;
import java.text.MessageFormat;
import java.util.Date;
import java.util.Locale;

import javax.swing.JFrame;

import de.soderer.utilities.Credentials;
import de.soderer.utilities.LangResources;
import de.soderer.utilities.UpdateParent;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.Version;

public class BasicUpdateableGuiApplication extends JFrame implements UpdateParent {
	private static final long serialVersionUID = 1869492889654406822L;
	
	private String applicationName;
	private Version applicationVersion;
	
	private SimpleProgressDialog<?> updateProgressDialog;
	
	public BasicUpdateableGuiApplication(String applicationName, Version applicationVersion) throws HeadlessException {
		super(applicationName + " (Version " + applicationVersion.toString() + ")");
		
		this.applicationName = applicationName;
		this.applicationVersion = applicationVersion;
	}

	@Override
	public Credentials aquireCredentials(String text, boolean aquireUsername, boolean aquirePassword) throws Exception {
		CredentialsDialog credentialsDialog = new CredentialsDialog(this, "Update " + applicationName, text, aquireUsername, aquirePassword);
		credentialsDialog.setVisible(true);
		return credentialsDialog.getCredentials();
	}

	@Override
	public void showUpdateError(String errorText) {
		if (updateProgressDialog != null) {
			updateProgressDialog.dispose();
			updateProgressDialog = null;
		}
		
		new TextDialog(this, "Update Error", errorText, getI18NString("close"), false, Color.PINK).setVisible(true);
	}

	@Override
	public void showUpdateProgress(Date itemStart, long itemsToDo, long itemsDone) {
		if (updateProgressDialog != null) {
			updateProgressDialog.showProgress(itemStart, itemsToDo, itemsDone);
		}
	}

	@Override
	public void showUpdateDone() {
		if (updateProgressDialog != null) {
			updateProgressDialog.dispose();
			updateProgressDialog = null;
		}
		
		new QuestionDialog(this, "Update", getI18NString("updateDone"), getI18NString("restartNow")).setVisible(true);
	}

	@Override
	public boolean askForUpdate(String availableNewVersion) throws Exception {
		if (availableNewVersion == null) {
			TextDialog textDialog = new TextDialog(this, "Update", getI18NString("noNewerVersion", applicationName, applicationVersion.toString()), getI18NString("close"));
			textDialog.setVisible(true);
			return false;
		} else {
			QuestionDialog questionDialog = new QuestionDialog(this, "Update", getI18NString("newVersion", availableNewVersion, applicationName, applicationVersion.toString()) + "\n" + getI18NString("installUpdate"), getI18NString("yes"), getI18NString("no"));
			questionDialog.setVisible(true);
			return questionDialog.getAnswerButtonIndex() == 0;
		}
	}

	@Override
	public void showUpdateDownloadStart() {
		updateProgressDialog = new SimpleProgressDialog<>(this, "Update", null);
		updateProgressDialog.showDialog();
	}

	@Override
	public void showUpdateDownloadEnd() {
		if (updateProgressDialog != null) {
			updateProgressDialog.dispose();
			updateProgressDialog = null;
		}
	}
	
	private String getI18NString(String resourceKey, Object... arguments) {
		if (LangResources.existsKey(resourceKey)) {
			return LangResources.get(resourceKey, arguments);
		} else if ("de".equalsIgnoreCase(Locale.getDefault().getLanguage())) {
			String pattern;
			switch(resourceKey) {
				case "noNewerVersion": pattern = "Es ist keine neuere Version verfügbar für {0}.\nDie aktuelle lokale Version ist {1}."; break;
				case "newVersion": pattern = "Es ist eine neue Version {0} verfügbar für {1}.\nDie aktuelle lokale Version ist {2}."; break;
				case "installUpdate": pattern = "Update installieren?"; break;
				case "enterUsername": pattern = "Bitte Usernamen eingeben"; break;
				case "enterPassword": pattern = "Bitte Passwort eingeben"; break;
				case "yes": pattern = "Ja"; break;
				case "no": pattern = "Nein"; break;
				case "close": pattern = "Schließen"; break;
				case "updateDone": pattern = "Upgedatete Anwendung kann neugestartet werden"; break;
				case "restartNow": pattern = "Jetzt neustarten"; break;
				default: pattern = "MessageKey unbekannt: " + resourceKey + " Argumente: " + Utilities.join(arguments, ", ");
			}
			return MessageFormat.format(pattern, arguments);
		} else {
			String pattern;
			switch(resourceKey) {
				case "noNewerVersion": pattern = "There is no newer version available for {0}.\nThe current local version is {1}."; break;
				case "newVersion": pattern = "New version {0} is available for {1}.\nThe current local version is {2}."; break;
				case "installUpdate": pattern = "Install update?"; break;
				case "enterUsername": pattern = "Please enter username"; break;
				case "enterPassword": pattern = "Please enter password"; break;
				case "yes": pattern = "Yes"; break;
				case "no": pattern = "No"; break;
				case "close": pattern = "Close"; break;
				case "updateDone": pattern = "Updated application may be restarted"; break;
				case "restartNow": pattern = "Restart now"; break;
				default: pattern = "MessageKey unknown: " + resourceKey + " arguments: " + Utilities.join(arguments, ", ");
			}
			return MessageFormat.format(pattern, arguments);
		}
	}
}
