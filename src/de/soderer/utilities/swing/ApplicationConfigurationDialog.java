package de.soderer.utilities.swing;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Image;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.List;

import javax.imageio.ImageIO;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JFileChooser;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JTextField;

import de.soderer.utilities.ConfigurationProperties;
import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.FileUtilities;
import de.soderer.utilities.LangResources;
import de.soderer.utilities.Result;
import de.soderer.utilities.SystemUtilities;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.Version;
import de.soderer.utilities.appupdate.ApplicationUpdateUtilities;

public class ApplicationConfigurationDialog extends ModalDialog<Result> {
	private static final long serialVersionUID = 422519084107817691L;

	public static final String CONFIG_LANGUAGE = "Application.Language";

	private final UpdateableGuiApplication updateParent;
	private final String applicationName;
	private final String applicationClassName;
	private final Version applicationVersion;
	private final LocalDateTime applicationBuildTime;
	private final ConfigurationProperties applicationConfiguration;
	private final byte[] iconImageDataWindows;
	private final Image iconImageLinux;
	private final String versioninfoDownloadURL;
	private final String trustedUpdateCaCertificate;
	private List<String> availableLanguages = Utilities.getList("de", "en");
	private String language;

	private JPanel configurationValuesPanel;

	private JComboBox<String> languageCombo;

	public String getLanguage() {
		return language;
	}

	public void setLanguage(final String language) {
		this.language = language;
		if (language != null) {
			languageCombo.setSelectedItem(language);
		}
	}

	public List<String> getAvailableLanguages() {
		return availableLanguages;
	}

	public ApplicationConfigurationDialog setAvailableLanguages(final List<String> availableLanguages) {
		this.availableLanguages = availableLanguages;

		languageCombo.removeAll();
		for (final String availableLanguage : availableLanguages) {
			languageCombo.addItem(availableLanguage);
		}
		if (language != null) {
			languageCombo.setSelectedItem(language);
		}

		return this;
	}

	public ApplicationConfigurationDialog(final UpdateableGuiApplication updateParent, final String applicationName, final String applicationClassName, final Version applicationVersion, final LocalDateTime applicationBuildTime, final ConfigurationProperties applicationConfiguration, final byte[] iconImageDataWindows, final Image iconImageLinux, final String versioninfoDownloadURL, final String trustedUpdateCaCertificate) throws Exception {
		super(updateParent, applicationName + " (Version " + applicationVersion.toString() + ")");

		this.updateParent = updateParent;
		this.applicationName = applicationName;
		this.applicationClassName = applicationClassName;
		this.applicationVersion = applicationVersion;
		this.applicationBuildTime = applicationBuildTime;
		this.applicationConfiguration = applicationConfiguration;
		language = applicationConfiguration.get(CONFIG_LANGUAGE);
		this.iconImageDataWindows = iconImageDataWindows;
		this.iconImageLinux = iconImageLinux;
		this.versioninfoDownloadURL = versioninfoDownloadURL;
		this.trustedUpdateCaCertificate = trustedUpdateCaCertificate;

		if (iconImageLinux != null) {
			setIconImage(iconImageLinux);
		}

		setLocationRelativeTo(null);

		setResizable(false);

		final JPanel panel = new JPanel();
		panel.setLayout(new BoxLayout(panel, BoxLayout.PAGE_AXIS));

		add(panel);

		setLayout(new BoxLayout(getContentPane(), BoxLayout.PAGE_AXIS));

		// Parameter Panel
		final JPanel parameterPanel = new JPanel();
		parameterPanel.setLayout(new BoxLayout(parameterPanel, BoxLayout.LINE_AXIS));

		// CheckBox parameter Panel
		final JPanel mandatoryParameterPanel = new JPanel();
		mandatoryParameterPanel.setLayout(new BoxLayout(mandatoryParameterPanel, BoxLayout.PAGE_AXIS));

		panel.add(createConfigurationValuesPanel());

		panel.add(createAdministrativePanel());

		panel.add(createMainButtonPanel());

		pack();

		setLocationRelativeTo(updateParent);

		returnValue = Result.ERROR;
	}

	private JPanel createConfigurationValuesPanel() {
		configurationValuesPanel = new JPanel();
		configurationValuesPanel.setLayout(new BoxLayout(configurationValuesPanel, BoxLayout.Y_AXIS));

		configurationValuesPanel.add(createBuildTimePanel());
		configurationValuesPanel.add(createLanguagePanel());

		return configurationValuesPanel;
	}

	private JPanel createBuildTimePanel() {
		final JPanel buildTimePanel = new JPanel();
		buildTimePanel.setLayout(new FlowLayout(FlowLayout.RIGHT));

		final JLabel buildTimeLabel = new JLabel(LangResources.get("buildTime"));
		buildTimePanel.add(buildTimeLabel);
		final JTextField buildTimeText = new JTextField();
		buildTimeText.setEditable(false);
		buildTimeText.setText(DateUtilities.formatDate(DateUtilities.YYYY_MM_DD_HHMMSS, applicationBuildTime));
		buildTimePanel.add(buildTimeText, BorderLayout.EAST);

		return buildTimePanel;
	}

	private JPanel createLanguagePanel() {
		final JPanel languagePanel = new JPanel();
		languagePanel.setLayout(new FlowLayout(FlowLayout.RIGHT));

		final JLabel languageLabel = new JLabel(LangResources.get("language"));
		languagePanel.add(languageLabel);
		languageCombo = new JComboBox<>();
		languageCombo.setToolTipText(LangResources.get("language_help"));
		for (final String availableLanguage : availableLanguages) {
			languageCombo.addItem(availableLanguage);
		}
		if (language != null) {
			languageCombo.setSelectedItem(language);
		}
		languageCombo.addItemListener(new ItemListener() {
			@Override
			public void itemStateChanged(final ItemEvent event) {
				language = (String) event.getItem();
			}
		});
		languagePanel.add(languageCombo, BorderLayout.EAST);

		return languagePanel;
	}

	private JPanel createAdministrativePanel() {
		final JPanel administrativeButtonPanel = new JPanel();
		administrativeButtonPanel.setLayout(new BoxLayout(administrativeButtonPanel, BoxLayout.LINE_AXIS));

		administrativeButtonPanel.add(Box.createRigidArea(new Dimension(5, 36)));

		final JButton createDesktopLinkButton = new JButton(LangResources.get("createDesktopLink"));
		createDesktopLinkButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent event) {
				try {
					String applicationJarFileName = SystemUtilities.getCurrentlyRunningJarFilePath();
					if (Utilities.isBlank(applicationJarFileName)) {
						applicationJarFileName = openJarFilePathDialog();
					}

					if (Utilities.isBlank(applicationJarFileName)) {
						return;
					}

					final String javaHome = System.getProperty("java.home");

					if (SystemUtilities.isWindowsSystem()) {
						final String desktopLinkFilePath = System.getProperty("user.home") + File.separator + "Desktop" + File.separator + applicationName + ".lnk";
						if (new File(desktopLinkFilePath).exists()) {
							new File(desktopLinkFilePath).delete();
						}

						final String iconPath = System.getProperty("user.home") + File.separator + applicationName + ".ico";
						if (new File(iconPath).exists()) {
							new File(iconPath).delete();
						}

						final String tempScriptFile = System.getProperty("java.io.tmpdir") + File.separator + applicationName + "_createlink.vbs";
						if (new File(tempScriptFile).exists()) {
							new File(tempScriptFile).delete();
						}

						if (iconImageDataWindows != null) {
							FileUtilities.write(new File(iconPath), iconImageDataWindows);
						}

						final StringBuilder data = new StringBuilder();
						data.append("set shortcut = CreateObject(\"WScript.Shell\").CreateShortcut(\"" + desktopLinkFilePath + "\")\n");
						data.append("shortcut.Description = \"" + applicationName + "\"\n");
						if (Utilities.isNotBlank(javaHome)) {
							data.append("shortcut.TargetPath = \"" + javaHome + "\\bin\\javaw.exe\"\n");
							data.append("shortcut.Arguments = \"-jar \"\"" + applicationJarFileName + "\"\"\"\n");
						} else {
							data.append("shortcut.TargetPath = \"" + applicationJarFileName + "\"\n");
						}
						data.append("shortcut.IconLocation = \"" + iconPath + "\"\n");
						data.append("shortcut.Save\n");

						try {
							FileUtilities.writeStringToFile(new File(tempScriptFile), data.toString(), StandardCharsets.UTF_8);

							Runtime.getRuntime().exec("wscript \"" + tempScriptFile + "\"");

							JOptionPane.showMessageDialog(getParent(), LangResources.get("desktopLinkWasCreatedAt", desktopLinkFilePath), LangResources.get("createDesktopLink"), JOptionPane.PLAIN_MESSAGE);

							new File(tempScriptFile).delete();
						} catch (final IOException e) {
							JOptionPane.showMessageDialog(getParent(), LangResources.get("errorMessage", e.getMessage()), LangResources.get("createDesktopLink"), JOptionPane.ERROR_MESSAGE);
						}
					} else if (SystemUtilities.isLinuxSystem()) {
						String desktopLinkFilePath;
						String iconPath;
						final File localShareApplicationsDirectory = new File(System.getProperty("user.home") + "/.local/share/applications");
						if (localShareApplicationsDirectory.exists()) {
							desktopLinkFilePath = localShareApplicationsDirectory.getAbsolutePath() + File.separator + applicationName + ".desktop";
							iconPath = localShareApplicationsDirectory.getAbsolutePath() + File.separator + applicationName + ".png";
						} else {
							desktopLinkFilePath = System.getProperty("user.home") + File.separator + "Desktop" + File.separator + applicationName + ".desktop";
							iconPath = System.getProperty("user.home") + File.separator + applicationName + ".png";
						}
						if (new File(desktopLinkFilePath).exists()) {
							new File(desktopLinkFilePath).delete();
						}
						if (new File(iconPath).exists()) {
							new File(iconPath).delete();
						}

						final StringBuilder data = new StringBuilder();
						data.append("[Desktop Entry]\n");
						data.append("Encoding=UTF-8\n");
						data.append("Name=" + applicationName + "\n");

						if (iconImageLinux != null) {
							ImageIO.write((BufferedImage) iconImageLinux, "png", new File(iconPath));
							data.append("Icon=" + iconPath + "\n");
						}

						// %U-parameter allows drag and drop of files on starter bar in unity
						// Set environment parameter --add-opens=java.desktop/sun.awt.X11=ALL-UNNAMED to allow application to be pinned in linux gnome applications dock
						data.append("Exec=java -jar --add-opens=java.desktop/sun.awt.X11=ALL-UNNAMED " + applicationJarFileName + " %U\n");
						data.append("Terminal=false\n");
						data.append("Type=Application\n");

						// StartupWMClass must be used for Display-Class init by the main application, so the Launcher can group all windows of the same application
						data.append("StartupWMClass=" + applicationClassName + "\n");

						try {
							FileUtilities.writeStringToFile(new File(desktopLinkFilePath), data.toString(), StandardCharsets.UTF_8);

							new File(desktopLinkFilePath).setExecutable(true);

							JOptionPane.showMessageDialog(getParent(), LangResources.get("desktopLinkWasCreatedAt", desktopLinkFilePath), LangResources.get("createDesktopLink"), JOptionPane.PLAIN_MESSAGE);
						} catch (final IOException e) {
							JOptionPane.showMessageDialog(getParent(), LangResources.get("errorMessage", e.getMessage()), LangResources.get("createDesktopLink"), JOptionPane.ERROR_MESSAGE);
						}
					}
				} catch (final Exception e) {
					JOptionPane.showMessageDialog(getParent(), LangResources.get("errorMessage", e.getMessage()), LangResources.get("createDesktopLink"), JOptionPane.ERROR_MESSAGE);
				}
			}
		});
		administrativeButtonPanel.add(createDesktopLinkButton);

		administrativeButtonPanel.add(Box.createRigidArea(new Dimension(5, 36)));

		final JButton updateButton = new JButton(LangResources.get("checkupdate"));
		updateButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent event) {
				try {
					ApplicationUpdateUtilities.executeUpdate(updateParent, versioninfoDownloadURL, applicationName, applicationVersion, trustedUpdateCaCertificate, null, null, "gui", true);
				} catch (final Exception e) {
					new QuestionDialog(getOwner(), applicationName + " ERROR", "ERROR:\n" + e.getMessage()).setBackgroundColor(SwingColor.LightRed).open();
				}
			}
		});
		administrativeButtonPanel.add(updateButton);

		administrativeButtonPanel.add(Box.createRigidArea(new Dimension(5, 36)));

		return administrativeButtonPanel;
	}

	private JPanel createMainButtonPanel() {
		final JPanel mainButtonPanel = new JPanel();
		mainButtonPanel.setLayout(new BoxLayout(mainButtonPanel, BoxLayout.LINE_AXIS));

		mainButtonPanel.add(Box.createRigidArea(new Dimension(5, 36)));

		// Ok Button
		final JButton okButton = new JButton(LangResources.get("ok"));
		okButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent event) {
				applicationConfiguration.set(CONFIG_LANGUAGE, language);
				returnValue = Result.OK;
				dispose();
			}
		});
		mainButtonPanel.add(okButton);

		mainButtonPanel.add(Box.createRigidArea(new Dimension(5, 36)));

		// Close Button
		final JButton closeButton = new JButton(LangResources.get("cancel"));
		closeButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent event) {
				returnValue = Result.CANCELED;
				dispose();
			}
		});
		mainButtonPanel.add(closeButton);

		mainButtonPanel.add(Box.createRigidArea(new Dimension(5, 36)));

		return mainButtonPanel;
	}

	private String openJarFilePathDialog() {
		String applicationJarFileName;
		final JFileChooser fileChooser = new JFileChooser();
		fileChooser.setDialogTitle(LangResources.get("createDesktopLink"));
		if (JFileChooser.APPROVE_OPTION == fileChooser.showOpenDialog(this)) {
			applicationJarFileName = fileChooser.getSelectedFile().getAbsolutePath();
		} else {
			applicationJarFileName = null;
		}
		return applicationJarFileName;
	}
}
