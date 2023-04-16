package de.soderer.dbimport;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Image;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.awt.event.KeyAdapter;
import java.awt.event.KeyEvent;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

import javax.imageio.ImageIO;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JFileChooser;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.WindowConstants;

import de.soderer.dbimport.DbImportDefinition.DataType;
import de.soderer.dbimport.DbImportDefinition.DuplicateMode;
import de.soderer.dbimport.DbImportDefinition.ImportMode;
import de.soderer.utilities.ConfigurationProperties;
import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.ExceptionUtilities;
import de.soderer.utilities.FileUtilities;
import de.soderer.utilities.IoUtilities;
import de.soderer.utilities.LangResources;
import de.soderer.utilities.NetworkUtilities;
import de.soderer.utilities.Result;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.VersionInfo;
import de.soderer.utilities.appupdate.ApplicationUpdateUtilities;
import de.soderer.utilities.collection.CaseInsensitiveMap;
import de.soderer.utilities.db.DbColumnType;
import de.soderer.utilities.db.DbDriverSupplier;
import de.soderer.utilities.db.DbUtilities;
import de.soderer.utilities.db.DbUtilities.DbVendor;
import de.soderer.utilities.http.HttpUtilities;
import de.soderer.utilities.swing.ApplicationConfigurationDialog;
import de.soderer.utilities.swing.DualProgressDialog;
import de.soderer.utilities.swing.ProgressDialog;
import de.soderer.utilities.swing.QuestionDialog;
import de.soderer.utilities.swing.SecurePreferencesDialog;
import de.soderer.utilities.swing.SwingColor;
import de.soderer.utilities.swing.UpdateableGuiApplication;

/**
 * The GUI for DbImport.
 */
public class DbImportGui extends UpdateableGuiApplication {
	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = 5969613637206441880L;

	public static final File KEYSTORE_FILE = new File(System.getProperty("user.home") + File.separator + "." + DbImport.APPLICATION_NAME + File.separator + "." + DbImport.APPLICATION_NAME + ".keystore");
	public static final String CONFIG_DAILY_UPDATE_CHECK = "DailyUpdateCheck";
	public static final String CONFIG_NEXT_DAILY_UPDATE_CHECK = "NextDailyUpdateCheck";

	private final ConfigurationProperties applicationConfiguration;

	/** The db type combo. */
	private final JComboBox<String> dbTypeCombo;

	private final JButton connectionCheckButton;

	/** The host field. */
	private final JTextField hostField;

	/** The db name field. */
	private final JTextField dbNameField;

	/** The user field. */
	private final JTextField userField;

	/** The password field. */
	private final JPasswordField passwordField;

	/** The secure connection box. */
	private final JCheckBox secureConnectionBox;

	/** The trustStoreFile field. */
	private final JTextField trustStoreFilePathField;

	/** The trustStorePassword field. */
	private final JPasswordField trustStorePasswordField;

	private final JButton trustStoreFileButton;

	private final JButton createTrustStoreFileButton;

	/** The tablename field. */
	private final JTextField tableNameField;

	/** The data type combo. */
	private final JComboBox<String> dataTypeCombo;

	/** The file log box. */
	private final JCheckBox fileLogBox;

	private final JCheckBox inlineDataBox;

	/** The importFilePath field. */
	private final JTextArea importFilePathOrDataField;

	/** The zipPassword field. */
	private final JPasswordField zipPasswordField;

	private final JTextField dataPathField;

	private final JTextField structureFilePathField;

	private final JButton structureFileButton;

	private final JButton createCompleteStructureFileButton;

	private final JTextField schemaFilePathField;

	private final JButton schemaFileButton;

	/** The separator combo. */
	private final JComboBox<String> separatorCombo;

	/** The string quote combo. */
	private final JComboBox<String> stringQuoteCombo;

	/** The escape string quote combo. */
	private final JComboBox<String> escapeStringQuoteCombo;

	/** The null value string combo. */
	private final JComboBox<String> nullValueStringCombo;

	/** The encoding combo. */
	private final JComboBox<String> encodingCombo;

	private final JTextField keyColumnsField;

	/** The mapping field. */
	private final JTextArea mappingField;

	/** The additionalInsertValues field. */
	private final JTextArea additionalInsertValuesField;

	/** The additionalUpdateValues field. */
	private final JTextArea additionalUpdateValuesField;

	/** The field for databases timezone */
	private final JComboBox<String> databaseTimezoneCombo;

	/** The field for datafiles timezone */
	private final JComboBox<String> importDataTimezoneCombo;

	/** The no headers box. */
	private final JCheckBox noHeadersBox;

	private final JCheckBox allowUnderfilledLinesBox;

	private final JCheckBox removeSurplusEmptyTrailingColumnsBox;

	private final JCheckBox createTableBox;

	private final JCheckBox onlyCommitOnFullSuccessBox;

	private final JCheckBox createNewIndexIfNeededBox;

	private final JCheckBox deactivateForeignKeyConstraintsBox;

	private final JCheckBox updateWithNullDataBox;

	private final JCheckBox trimDataBox;

	private final JCheckBox logErroneousDataBox;

	private final JComboBox<String> importModeCombo;

	private final JComboBox<String> duplicateModeCombo;

	private final JButton mappingButton;

	/** The field for DateFormat */
	private final JTextField importDateFormatField;

	/** The field for DateTimeFormat */
	private final JTextField importDateTimeFormatField;

	/** The temporary preferences password. */
	private char[] temporaryPreferencesPassword = null;

	/**
	 * Instantiates a new db csv import gui.
	 *
	 * @param dbImportDefinition
	 *            the db csv import definition
	 * @throws Exception
	 *             the exception
	 */
	public DbImportGui(final DbImportDefinition dbImportDefinition) throws Exception {
		super(DbImport.APPLICATION_NAME, DbImport.VERSION, KEYSTORE_FILE);

		setTitle(DbImport.APPLICATION_NAME + " (Version " + DbImport.VERSION.toString() + ")");

		applicationConfiguration = new ConfigurationProperties(DbImport.APPLICATION_NAME, true);
		DbImportGui.setupDefaultConfig(applicationConfiguration);
		if ("de".equalsIgnoreCase(applicationConfiguration.get(ApplicationConfigurationDialog.CONFIG_LANGUAGE))) {
			Locale.setDefault(Locale.GERMAN);
		} else {
			Locale.setDefault(Locale.ENGLISH);
		}

		if (dailyUpdateCheckIsPending()) {
			setDailyUpdateCheckStatus(true);
			try {
				if (ApplicationUpdateUtilities.checkForNewVersionAvailable(this, DbImport.VERSIONINFO_DOWNLOAD_URL, DbImport.APPLICATION_NAME, VersionInfo.getApplicationVersion()) != null) {
					ApplicationUpdateUtilities.executeUpdate(this, DbImport.VERSIONINFO_DOWNLOAD_URL, DbImport.APPLICATION_NAME, DbImport.VERSION, DbImport.TRUSTED_UPDATE_CA_CERTIFICATE, null, null, "gui", true);
				}
			} catch (final Exception e) {
				new QuestionDialog(this, DbImport.APPLICATION_NAME + " " + LangResources.get("updateCheck") + " ERROR", LangResources.get("error.cannotCheckForUpdate") + "\n" + "ERROR:\n" + e.getMessage()).setBackgroundColor(SwingColor.LightRed).open();
			}
		}

		try (InputStream imageIconStream = getClass().getClassLoader().getResourceAsStream("DbImport_Icon.png")) {
			final BufferedImage imageIcon = ImageIO.read(imageIconStream);
			setIconImage(imageIcon);
		}

		final DbImportGui dbImportGui = this;

		setLocationRelativeTo(null);

		setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
		setLayout(new BoxLayout(getContentPane(), BoxLayout.PAGE_AXIS));

		// Parameter Panel
		final JPanel parameterPanel = new JPanel();
		parameterPanel.setLayout(new BoxLayout(parameterPanel, BoxLayout.LINE_AXIS));

		// Mandatory parameter Panel
		final JPanel mandatoryParameterPanel = new JPanel();
		mandatoryParameterPanel.setLayout(new BoxLayout(mandatoryParameterPanel, BoxLayout.PAGE_AXIS));

		// DBType Pane
		final JPanel dbTypePanel = new JPanel();
		dbTypePanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		final JLabel dbTypeLabel = new JLabel(LangResources.get("dbtype"));
		dbTypePanel.add(dbTypeLabel);
		dbTypeCombo = new JComboBox<>();
		dbTypeCombo.setToolTipText(LangResources.get("dbtype_help"));
		for (final DbVendor dbVendor : DbVendor.values()) {
			dbTypeCombo.addItem(dbVendor.toString());
		}
		dbTypeCombo.addItemListener(new ItemListener() {
			@Override
			public void itemStateChanged(final ItemEvent event) {
				checkButtonStatus();
			}
		});
		dbTypePanel.add(dbTypeCombo, BorderLayout.EAST);

		connectionCheckButton = new JButton(LangResources.get("connectionCheck"));
		connectionCheckButton.setPreferredSize(new Dimension(150, dbTypeCombo.getPreferredSize().height));
		connectionCheckButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent event) {
				try (Connection connection = DbUtilities.createConnection(dbImportDefinition, false)) {
					new QuestionDialog(dbImportGui, DbImport.APPLICATION_NAME + " OK", "OK").setBackgroundColor(SwingColor.Green).open();
				} catch (final Exception e) {
					new QuestionDialog(dbImportGui, DbImport.APPLICATION_NAME + " ERROR", "ERROR:\n" + e.getMessage()).setBackgroundColor(SwingColor.LightRed).open();
				}
			}
		});
		dbTypePanel.add(connectionCheckButton);

		mandatoryParameterPanel.add(dbTypePanel);

		// Host Panel
		final JPanel hostPanel = new JPanel();
		hostPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		final JLabel hostLabel = new JLabel(LangResources.get("host"));
		hostPanel.add(hostLabel);
		hostField = new JTextField();
		hostField.setToolTipText(LangResources.get("host_help"));
		hostField.setPreferredSize(new Dimension(200, hostField.getPreferredSize().height));
		hostField.addKeyListener(new KeyAdapter() {
			@Override
			public void keyReleased(final KeyEvent event) {
				checkButtonStatus();
			}
		});
		hostPanel.add(hostField);
		mandatoryParameterPanel.add(hostPanel);

		// DB name Panel
		final JPanel dbNamePanel = new JPanel();
		dbNamePanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		final JLabel dbNameLabel = new JLabel(LangResources.get("dbname"));
		dbNamePanel.add(dbNameLabel);
		dbNameField = new JTextField();
		dbNameField.setToolTipText(LangResources.get("dbname_help"));
		dbNameField.setPreferredSize(new Dimension(200, dbNameField.getPreferredSize().height));
		dbNameField.addKeyListener(new KeyAdapter() {
			@Override
			public void keyReleased(final KeyEvent event) {
				checkButtonStatus();
			}
		});
		dbNamePanel.add(dbNameField);
		mandatoryParameterPanel.add(dbNamePanel);

		// User Panel
		final JPanel userPanel = new JPanel();
		userPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		final JLabel userLabel = new JLabel(LangResources.get("user"));
		userPanel.add(userLabel);
		userField = new JTextField();
		userField.setToolTipText(LangResources.get("user_help"));
		userField.setPreferredSize(new Dimension(200, userField.getPreferredSize().height));
		userField.addKeyListener(new KeyAdapter() {
			@Override
			public void keyReleased(final KeyEvent event) {
				checkButtonStatus();
			}
		});
		userPanel.add(userField);
		mandatoryParameterPanel.add(userPanel);

		// Password Panel
		final JPanel passwordPanel = new JPanel();
		passwordPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		final JLabel passwordLabel = new JLabel(LangResources.get("password"));
		passwordPanel.add(passwordLabel);
		passwordField = new JPasswordField();
		passwordField.setToolTipText(LangResources.get("password_help"));
		passwordField.setPreferredSize(new Dimension(200, passwordField.getPreferredSize().height));
		passwordField.addKeyListener(new KeyAdapter() {
			@Override
			public void keyReleased(final KeyEvent event) {
				checkButtonStatus();
			}
		});
		passwordPanel.add(passwordField);
		mandatoryParameterPanel.add(passwordPanel);

		// SecureConnection Panel
		final JPanel secureConnectionPanel = new JPanel();
		secureConnectionPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		final JLabel secureConnectionLabel = new JLabel(LangResources.get("secureConnection"));
		secureConnectionPanel.add(secureConnectionLabel);
		secureConnectionBox = new JCheckBox();
		secureConnectionBox.setToolTipText(LangResources.get("secureConnection_help"));
		secureConnectionBox.setPreferredSize(new Dimension(200, secureConnectionBox.getPreferredSize().height));
		secureConnectionBox.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent e) {
				checkButtonStatus();
			}
		});
		secureConnectionPanel.add(secureConnectionBox);
		mandatoryParameterPanel.add(secureConnectionPanel);

		// TrustStoreFile Panel
		final JPanel trustStoreFilePathPanel = new JPanel();
		trustStoreFilePathPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		final JLabel trustStoreFilePathLabel = new JLabel(LangResources.get("trustStoreFile"));
		trustStoreFilePathPanel.add(trustStoreFilePathLabel);
		trustStoreFilePathField = new JTextField();
		trustStoreFilePathField.setToolTipText(LangResources.get("trustStoreFile_help"));
		trustStoreFilePathField.setPreferredSize(new Dimension(130, trustStoreFilePathField.getPreferredSize().height));
		trustStoreFilePathField.addKeyListener(new KeyAdapter() {
			@Override
			public void keyReleased(final KeyEvent event) {
				checkButtonStatus();
			}
		});
		trustStoreFilePathPanel.add(trustStoreFilePathField);

		trustStoreFileButton = new JButton("...");
		trustStoreFileButton.setPreferredSize(new Dimension(20, trustStoreFilePathField.getPreferredSize().height));
		trustStoreFileButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent event) {
				try {
					final File trustStoreFile = selectFile(trustStoreFilePathField.getText(), LangResources.get("trustStoreFile"));
					if (trustStoreFile != null) {
						trustStoreFilePathField.setText(trustStoreFile.getAbsolutePath());
						checkButtonStatus();
					}
				} catch (final Exception e) {
					new QuestionDialog(dbImportGui, DbImport.APPLICATION_NAME + " ERROR", "ERROR:\n" + e.getMessage()).setBackgroundColor(SwingColor.LightRed).open();
				}
			}
		});
		trustStoreFilePathPanel.add(trustStoreFileButton);

		createTrustStoreFileButton = new JButton("+");
		createTrustStoreFileButton.setToolTipText(LangResources.get("createTrustStoreFile_help"));
		createTrustStoreFileButton.setPreferredSize(new Dimension(40, trustStoreFilePathField.getPreferredSize().height));
		createTrustStoreFileButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent event) {
				try {
					if (Utilities.isBlank((trustStoreFilePathField.getText()))) {
						final File trustStoreFile = selectFile(trustStoreFilePathField.getText(), LangResources.get("trustStoreFile"));
						if (trustStoreFile != null) {
							trustStoreFilePathField.setText(trustStoreFile.getAbsolutePath());
						}
					}

					if (Utilities.isNotBlank((trustStoreFilePathField.getText()))) {
						if (new File(trustStoreFilePathField.getText()).exists()) {
							new QuestionDialog(dbImportGui, DbImport.APPLICATION_NAME + " ERROR", "ERROR:\n" + "File already exists: '" + trustStoreFilePathField.getText() + "'").setBackgroundColor(SwingColor.LightRed).open();
						} else {
							HttpUtilities.createTrustStoreFile(hostField.getText(), DbVendor.getDbVendorByName((String) dbTypeCombo.getSelectedItem()).getDefaultPort(), new File(trustStoreFilePathField.getText()), trustStorePasswordField.getPassword());
							new QuestionDialog(dbImportGui, DbImport.APPLICATION_NAME + " OK", "OK").setBackgroundColor(SwingColor.Green).open();
							checkButtonStatus();
						}
					}
				} catch (final Exception e) {
					new QuestionDialog(dbImportGui, DbImport.APPLICATION_NAME + " ERROR", "ERROR:\n" + e.getMessage()).setBackgroundColor(SwingColor.LightRed).open();
				}
			}
		});
		trustStoreFilePathPanel.add(createTrustStoreFileButton);
		mandatoryParameterPanel.add(trustStoreFilePathPanel);

		// TrustStorePassword Panel
		final JPanel trustStorePasswordPanel = new JPanel();
		trustStorePasswordPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		final JLabel trustStorePasswordLabel = new JLabel(LangResources.get("trustStorePassword"));
		trustStorePasswordPanel.add(trustStorePasswordLabel);
		trustStorePasswordField = new JPasswordField();
		trustStorePasswordField.setToolTipText(LangResources.get("trustStorePassword_help"));
		trustStorePasswordField.setPreferredSize(new Dimension(200, trustStorePasswordField.getPreferredSize().height));
		trustStorePasswordField.addKeyListener(new KeyAdapter() {
			@Override
			public void keyReleased(final KeyEvent event) {
				checkButtonStatus();
			}
		});
		trustStorePasswordPanel.add(trustStorePasswordField);
		mandatoryParameterPanel.add(trustStorePasswordPanel);

		// TableName Panel
		final JPanel tableNamePanel = new JPanel();
		tableNamePanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		final JLabel tableNameLabel = new JLabel(LangResources.get("tableName"));
		tableNamePanel.add(tableNameLabel);
		tableNameField = new JTextField();
		tableNameField.setToolTipText(LangResources.get("tableName_help"));
		tableNameField.setPreferredSize(new Dimension(200, tableNameField.getPreferredSize().height));
		tableNamePanel.add(tableNameField);
		mandatoryParameterPanel.add(tableNamePanel);

		// Import type Pane
		final JPanel dataTypePanel = new JPanel();
		dataTypePanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		final JLabel dataTypeLabel = new JLabel(LangResources.get("dataType"));
		dataTypePanel.add(dataTypeLabel);
		dataTypeCombo = new JComboBox<>();
		dataTypeCombo.setToolTipText(LangResources.get("dataType_help"));
		dataTypeCombo.setPreferredSize(new Dimension(200, dataTypeCombo.getPreferredSize().height));
		for (final DataType dataType : DataType.values()) {
			dataTypeCombo.addItem(dataType.toString());
		}
		dataTypeCombo.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent event) {
				checkButtonStatus();
			}
		});
		dataTypePanel.add(dataTypeCombo, BorderLayout.EAST);
		mandatoryParameterPanel.add(dataTypePanel);

		// Inputpath Panel
		final JPanel importFilePathPanel = new JPanel();
		importFilePathPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		final JLabel inputFilePathLabel = new JLabel(LangResources.get("importFilePath"));
		importFilePathPanel.add(inputFilePathLabel);
		importFilePathOrDataField = new JTextArea();
		importFilePathOrDataField.setToolTipText(LangResources.get("importFilePath_help"));
		final JScrollPane importFilePathOrDataScrollpane = new JScrollPane(importFilePathOrDataField);
		importFilePathOrDataScrollpane.setPreferredSize(new Dimension(175, 50));
		importFilePathPanel.add(importFilePathOrDataScrollpane);
		mandatoryParameterPanel.add(importFilePathPanel);

		final JButton browseButton = new JButton("...");
		browseButton.setPreferredSize(new Dimension(20, 50));
		browseButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent event) {
				try {
					final File importFile = selectFile(importFilePathOrDataField.getText(), LangResources.get("importFilePath"));
					if (importFile != null) {
						importFilePathOrDataField.setText(importFile.getAbsolutePath());
						inlineDataBox.setSelected(false);
					}
				} catch (final Exception e) {
					new QuestionDialog(dbImportGui, DbImport.APPLICATION_NAME + " ERROR", "ERROR:\n" + e.getMessage()).setBackgroundColor(SwingColor.LightRed).open();
				}
			}
		});
		importFilePathPanel.add(browseButton);

		// zipPassword panel
		final JPanel zipPasswordPanel = new JPanel();
		zipPasswordPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		final JLabel zipPasswordLabel = new JLabel(LangResources.get("zipPassword"));
		zipPasswordPanel.add(zipPasswordLabel);
		zipPasswordField = new JPasswordField();
		zipPasswordField.setToolTipText(LangResources.get("zipPassword_help"));
		zipPasswordField.setPreferredSize(new Dimension(200, zipPasswordField.getPreferredSize().height));
		zipPasswordField.addKeyListener(new KeyAdapter() {
			@Override
			public void keyReleased(final KeyEvent event) {
				checkButtonStatus();
			}
		});
		zipPasswordPanel.add(zipPasswordField);
		mandatoryParameterPanel.add(zipPasswordPanel);

		// Data path panel
		final JPanel dataPathPanel = new JPanel();
		dataPathPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		final JLabel dataPathLabel = new JLabel(LangResources.get("dataPath"));
		dataPathPanel.add(dataPathLabel);
		dataPathField = new JTextField();
		dataPathField.setToolTipText(LangResources.get("dataPath_help"));
		dataPathField.setPreferredSize(new Dimension(200, dataPathField.getPreferredSize().height));
		dataPathPanel.add(dataPathField);
		mandatoryParameterPanel.add(dataPathPanel);

		// Structure file panel
		final JPanel structureFilePathPanel = new JPanel();
		structureFilePathPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		final JLabel structureFilePathLabel = new JLabel(LangResources.get("structureFilePath"));
		structureFilePathPanel.add(structureFilePathLabel);
		structureFilePathField = new JTextField();
		structureFilePathField.setToolTipText(LangResources.get("structureFilePath_help"));
		structureFilePathField.setPreferredSize(new Dimension(130, structureFilePathField.getPreferredSize().height));
		structureFilePathField.addKeyListener(new KeyAdapter() {
			@Override
			public void keyReleased(final KeyEvent event) {
				checkButtonStatus();
			}
		});
		structureFilePathPanel.add(structureFilePathField);

		structureFileButton = new JButton("...");
		structureFileButton.setPreferredSize(new Dimension(20, structureFilePathField.getPreferredSize().height));
		structureFileButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent event) {
				try {
					final File structureFile = selectFile(structureFilePathField.getText(), LangResources.get("structureFilePath"));
					if (structureFile != null) {
						structureFilePathField.setText(structureFile.getAbsolutePath());
					}
				} catch (final Exception e) {
					new QuestionDialog(dbImportGui, DbImport.APPLICATION_NAME + " ERROR", "ERROR:\n" + e.getMessage()).setBackgroundColor(SwingColor.LightRed).open();
				}
			}
		});
		structureFilePathPanel.add(structureFileButton);

		createCompleteStructureFileButton = new JButton("+");
		createCompleteStructureFileButton.setToolTipText(LangResources.get("createCompleteStructure_help"));
		createCompleteStructureFileButton.setPreferredSize(new Dimension(40, structureFilePathField.getPreferredSize().height));
		createCompleteStructureFileButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent event) {
				try {
					if (Utilities.isBlank((structureFilePathField.getText()))) {
						final File structureFile = selectFile(structureFilePathField.getText(), LangResources.get("createCompleteStructure"));
						if (structureFile != null) {
							structureFilePathField.setText(structureFile.getAbsolutePath());
						}
					}

					if (Utilities.isNotBlank((structureFilePathField.getText()))) {
						if (!new File(structureFilePathField.getText()).exists()) {
							new QuestionDialog(dbImportGui, DbImport.APPLICATION_NAME + " ERROR", "ERROR:\n" + "File does not exist: '" + structureFilePathField.getText() + "'").setBackgroundColor(SwingColor.LightRed).open();
						} else {
							try (FileInputStream jsonStructureDataInputStream = new FileInputStream(structureFilePathField.getText());
									Connection connection = DbUtilities.createConnection(dbImportDefinition, true)) {
								final DbStructureWorker worker = new DbStructureWorker(
										null,
										getConfigurationAsDefinition(),
										jsonStructureDataInputStream);
								final ProgressDialog<DbStructureWorker> progressDialog = new ProgressDialog<>(dbImportGui, DbImport.APPLICATION_NAME, null, worker);
								worker.setParent(progressDialog);
								final Result result = progressDialog.open();

								if (result == Result.CANCELED) {
									new QuestionDialog(dbImportGui, DbImport.APPLICATION_NAME, LangResources.get("error.canceledbyuser")).setBackgroundColor(SwingColor.Yellow).open();
								} else if (result == Result.ERROR) {
									final Exception e = worker.getError();
									if (e instanceof DbImportException) {
										new QuestionDialog(dbImportGui, DbImport.APPLICATION_NAME + " ERROR", "ERROR:\n" + ((DbImportException) e).getMessage()).setBackgroundColor(SwingColor.LightRed).open();
									} else {
										final String stacktrace = ExceptionUtilities.getStackTrace(e);
										new QuestionDialog(dbImportGui, DbImport.APPLICATION_NAME + " ERROR", "ERROR:\n" + e.getClass().getSimpleName() + ":\n" + e.getMessage() + "\n\n" + stacktrace).setBackgroundColor(SwingColor.LightRed).open();
									}
								} else {
									new QuestionDialog(dbImportGui, DbImport.APPLICATION_NAME + " OK", "Structure creation result:\n\t" + worker.getCreatedTables() + " created tables\n\t" + worker.getCreatedColumns() + " added columns").setBackgroundColor(SwingColor.Green).open();
								}
							}
						}
					}
				} catch (final Exception e) {
					new QuestionDialog(dbImportGui, DbImport.APPLICATION_NAME + " ERROR", "ERROR:\n" + e.getMessage()).setBackgroundColor(SwingColor.LightRed).open();
				}
			}
		});
		structureFilePathPanel.add(createCompleteStructureFileButton);
		mandatoryParameterPanel.add(structureFilePathPanel);

		// Schema file panel
		final JPanel schemaFilePathPanel = new JPanel();
		schemaFilePathPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		final JLabel schemaFilePathLabel = new JLabel(LangResources.get("schemaFilePath"));
		schemaFilePathPanel.add(schemaFilePathLabel);
		schemaFilePathField = new JTextField();
		schemaFilePathField.setToolTipText(LangResources.get("schemaFilePath_help"));
		schemaFilePathField.setPreferredSize(new Dimension(175, schemaFilePathField.getPreferredSize().height));
		schemaFilePathPanel.add(schemaFilePathField);

		schemaFileButton = new JButton("...");
		schemaFileButton.setPreferredSize(new Dimension(20, schemaFilePathField.getPreferredSize().height));
		schemaFileButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent event) {
				try {
					final File schemaFile = selectFile(schemaFilePathField.getText(), LangResources.get("schemaFilePath"));
					if (schemaFile != null) {
						schemaFilePathField.setText(schemaFile.getAbsolutePath());
					}
				} catch (final Exception e) {
					new QuestionDialog(dbImportGui, DbImport.APPLICATION_NAME + " ERROR", "ERROR:\n" + e.getMessage()).setBackgroundColor(SwingColor.LightRed).open();
				}
			}
		});
		schemaFilePathPanel.add(schemaFileButton);
		mandatoryParameterPanel.add(schemaFilePathPanel);

		// Encoding Pane
		final JPanel encodingPanel = new JPanel();
		encodingPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		final JLabel encodingLabel = new JLabel(LangResources.get("encoding"));
		encodingPanel.add(encodingLabel);
		encodingCombo = new JComboBox<>();
		encodingCombo.setToolTipText(LangResources.get("encoding_help"));
		encodingCombo.setPreferredSize(new Dimension(200, encodingCombo.getPreferredSize().height));
		encodingCombo.addItem(StandardCharsets.UTF_8.name());
		encodingCombo.addItem(StandardCharsets.ISO_8859_1.name());
		encodingCombo.addItem("ISO-8859-15");
		encodingCombo.setEditable(true);
		encodingPanel.add(encodingCombo);
		mandatoryParameterPanel.add(encodingPanel);

		// Separator Pane
		final JPanel separatorPanel = new JPanel();
		separatorPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		final JLabel separatorLabel = new JLabel(LangResources.get("separator"));
		separatorPanel.add(separatorLabel);
		separatorCombo = new JComboBox<>();
		separatorCombo.setToolTipText(LangResources.get("separator_help"));
		separatorCombo.setPreferredSize(new Dimension(200, separatorCombo.getPreferredSize().height));
		separatorCombo.addItem(";");
		separatorCombo.addItem(",");
		separatorCombo.setEditable(true);
		separatorPanel.add(separatorCombo);
		mandatoryParameterPanel.add(separatorPanel);

		// StringQuote Pane
		final JPanel stringQuotePanel = new JPanel();
		stringQuotePanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		final JLabel stringQuoteLabel = new JLabel(LangResources.get("stringquote"));
		stringQuotePanel.add(stringQuoteLabel);
		stringQuoteCombo = new JComboBox<>();
		stringQuoteCombo.setToolTipText(LangResources.get("stringquote_help"));
		stringQuoteCombo.setPreferredSize(new Dimension(200, stringQuoteCombo.getPreferredSize().height));
		stringQuoteCombo.addItem("\"");
		stringQuoteCombo.addItem("'");
		stringQuoteCombo.setEditable(true);
		stringQuotePanel.add(stringQuoteCombo);
		mandatoryParameterPanel.add(stringQuotePanel);

		// escapeStringQuote Pane
		final JPanel escapeStringQuotePanel = new JPanel();
		escapeStringQuotePanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		final JLabel escapeStringQuoteLabel = new JLabel(LangResources.get("escapestringquote"));
		escapeStringQuotePanel.add(escapeStringQuoteLabel);
		escapeStringQuoteCombo = new JComboBox<>();
		escapeStringQuoteCombo.setToolTipText(LangResources.get("escapestringquote_help"));
		escapeStringQuoteCombo.setPreferredSize(new Dimension(200, escapeStringQuoteCombo.getPreferredSize().height));
		escapeStringQuoteCombo.addItem("\"");
		escapeStringQuoteCombo.addItem("'");
		escapeStringQuoteCombo.addItem("\\");
		escapeStringQuoteCombo.setEditable(true);
		escapeStringQuotePanel.add(escapeStringQuoteCombo);
		mandatoryParameterPanel.add(escapeStringQuotePanel);

		// NullValueString Pane
		final JPanel nullValueStringPanel = new JPanel();
		nullValueStringPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		final JLabel nullValueStringLabel = new JLabel(LangResources.get("nullvaluetext"));
		nullValueStringPanel.add(nullValueStringLabel);
		nullValueStringCombo = new JComboBox<>();
		nullValueStringCombo.setToolTipText(LangResources.get("nullvaluetext_help"));
		nullValueStringCombo.setPreferredSize(new Dimension(200, nullValueStringCombo.getPreferredSize().height));
		nullValueStringCombo.addItem("");
		nullValueStringCombo.addItem("NULL");
		nullValueStringCombo.addItem("Null");
		nullValueStringCombo.addItem("null");
		nullValueStringCombo.addItem("<null>");
		nullValueStringCombo.setEditable(true);
		nullValueStringPanel.add(nullValueStringCombo);
		mandatoryParameterPanel.add(nullValueStringPanel);

		// Importmode Panel
		final JPanel importModePanel = new JPanel();
		importModePanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		final JLabel importModeLabel = new JLabel(LangResources.get("importMode"));
		importModePanel.add(importModeLabel);
		importModeCombo = new JComboBox<>();
		importModeCombo.setToolTipText(LangResources.get("importMode_help"));
		importModeCombo.setPreferredSize(new Dimension(200, importModeCombo.getPreferredSize().height));
		importModeCombo.addItem("CLEARINSERT");
		importModeCombo.addItem("INSERT");
		importModeCombo.addItem("UPDATE");
		importModeCombo.addItem("UPSERT");
		importModeCombo.setEditable(false);
		importModeCombo.addItemListener(new ItemListener() {
			@Override
			public void itemStateChanged(final ItemEvent event) {
				checkButtonStatus();
			}
		});
		importModePanel.add(importModeCombo);
		mandatoryParameterPanel.add(importModePanel);

		// Duplicatemode Panel
		final JPanel duplicateModePanel = new JPanel();
		duplicateModePanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		final JLabel duplicateModeLabel = new JLabel(LangResources.get("duplicateMode"));
		duplicateModePanel.add(duplicateModeLabel);
		duplicateModeCombo = new JComboBox<>();
		duplicateModeCombo.setToolTipText(LangResources.get("duplicateMode_help"));
		duplicateModeCombo.setPreferredSize(new Dimension(200, duplicateModeCombo.getPreferredSize().height));
		for (final DuplicateMode duplicateMode : DuplicateMode.values()) {
			duplicateModeCombo.addItem(duplicateMode.toString());
		}
		duplicateModeCombo.setEditable(false);
		duplicateModeCombo.addItemListener(new ItemListener() {
			@Override
			public void itemStateChanged(final ItemEvent event) {
				checkButtonStatus();
			}
		});
		duplicateModePanel.add(duplicateModeCombo);
		mandatoryParameterPanel.add(duplicateModePanel);

		// Key columns Panel
		final JPanel keyColumnsPanel = new JPanel();
		keyColumnsPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		final JLabel keycolumnsLabel = new JLabel(LangResources.get("keyColumns"));
		keyColumnsPanel.add(keycolumnsLabel);
		keyColumnsField = new JTextField();
		keyColumnsField.setToolTipText(LangResources.get("keyColumns_help"));
		keyColumnsField.setPreferredSize(new Dimension(200, keyColumnsField.getPreferredSize().height));
		keyColumnsPanel.add(keyColumnsField);
		mandatoryParameterPanel.add(keyColumnsPanel);

		// Mapping Panel
		final JPanel mappingPanel = new JPanel();
		mappingPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		final JLabel mappingLabel = new JLabel(LangResources.get("mapping"));
		mappingPanel.add(mappingLabel);
		mappingField = new JTextArea();
		mappingField.setToolTipText(LangResources.get("mapping_help"));
		final JScrollPane mappingScrollpane = new JScrollPane(mappingField);
		mappingScrollpane.setPreferredSize(new Dimension(200, 75));
		mappingPanel.add(mappingScrollpane);
		mandatoryParameterPanel.add(mappingPanel);

		// Additional Insert values Panel
		final JPanel additionalInsertValuesPanel = new JPanel();
		additionalInsertValuesPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		final JLabel additionalInsertValuesLabel = new JLabel(LangResources.get("additionalInsertValues"));
		additionalInsertValuesPanel.add(additionalInsertValuesLabel);
		additionalInsertValuesField = new JTextArea();
		additionalInsertValuesField.setToolTipText(LangResources.get("additionalInsertValues_help"));
		final JScrollPane additionalInsertValuesScrollpane = new JScrollPane(additionalInsertValuesField);
		additionalInsertValuesScrollpane.setPreferredSize(new Dimension(200, 35));
		additionalInsertValuesPanel.add(additionalInsertValuesScrollpane);
		mandatoryParameterPanel.add(additionalInsertValuesPanel);

		// Additional Update values Panel
		final JPanel additionalUpdateValuesPanel = new JPanel();
		additionalUpdateValuesPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		final JLabel additionalUpdateValuesLabel = new JLabel(LangResources.get("additionalUpdateValues"));
		additionalUpdateValuesPanel.add(additionalUpdateValuesLabel);
		additionalUpdateValuesField = new JTextArea();
		additionalUpdateValuesField.setToolTipText(LangResources.get("additionalUpdateValues_help"));
		final JScrollPane additionalUpdateValuesScrollpane = new JScrollPane(additionalUpdateValuesField);
		additionalUpdateValuesScrollpane.setPreferredSize(new Dimension(200, 35));
		additionalUpdateValuesPanel.add(additionalUpdateValuesScrollpane);
		mandatoryParameterPanel.add(additionalUpdateValuesPanel);

		// Database timezone Panel
		final JPanel databaseTimezonePanel = new JPanel();
		databaseTimezonePanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		final JLabel databaseTimezoneLabel = new JLabel(LangResources.get("databaseTimezone"));
		databaseTimezonePanel.add(databaseTimezoneLabel);
		databaseTimezoneCombo = new JComboBox<>();
		databaseTimezoneCombo.setToolTipText(LangResources.get("databaseTimezone_help"));
		databaseTimezoneCombo.setPreferredSize(new Dimension(200, databaseTimezoneCombo.getPreferredSize().height));
		for (final String databaseTimezone : TimeZone.getAvailableIDs()) {
			databaseTimezoneCombo.addItem(databaseTimezone);
		}
		databaseTimezoneCombo.setSelectedItem(TimeZone.getDefault().getID());
		databaseTimezoneCombo.setEditable(false);
		databaseTimezoneCombo.addItemListener(new ItemListener() {
			@Override
			public void itemStateChanged(final ItemEvent event) {
				checkButtonStatus();
			}
		});
		databaseTimezonePanel.add(databaseTimezoneCombo);
		mandatoryParameterPanel.add(databaseTimezonePanel);

		// Import data timezone Panel
		final JPanel importDataTimezonePanel = new JPanel();
		importDataTimezonePanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		final JLabel importDataTimezoneLabel = new JLabel(LangResources.get("importDataTimezone"));
		importDataTimezonePanel.add(importDataTimezoneLabel);
		importDataTimezoneCombo = new JComboBox<>();
		importDataTimezoneCombo.setToolTipText(LangResources.get("importDataTimezone_help"));
		importDataTimezoneCombo.setPreferredSize(new Dimension(200, importDataTimezoneCombo.getPreferredSize().height));
		for (final String importDataTimezone : TimeZone.getAvailableIDs()) {
			importDataTimezoneCombo.addItem(importDataTimezone);
		}
		importDataTimezoneCombo.setSelectedItem(TimeZone.getDefault().getID());
		importDataTimezoneCombo.setEditable(false);
		importDataTimezoneCombo.addItemListener(new ItemListener() {
			@Override
			public void itemStateChanged(final ItemEvent event) {
				checkButtonStatus();
			}
		});
		importDataTimezonePanel.add(importDataTimezoneCombo);
		mandatoryParameterPanel.add(importDataTimezonePanel);

		// Import date format
		final JPanel importDateFormatPanel = new JPanel();
		importDateFormatPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		final JLabel importDateFormatLabel = new JLabel(LangResources.get("importDateFormat"));
		importDateFormatPanel.add(importDateFormatLabel);
		importDateFormatField = new JTextField();
		importDateFormatField.setToolTipText(LangResources.get("importDateFormat_help"));
		importDateFormatField.setPreferredSize(new Dimension(200, importDateFormatField.getPreferredSize().height));
		importDateFormatPanel.add(importDateFormatField);
		mandatoryParameterPanel.add(importDateFormatPanel);

		// Import datetime format
		final JPanel importDateTimeFormatPanel = new JPanel();
		importDateTimeFormatPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		final JLabel importDateTimeFormatLabel = new JLabel(LangResources.get("importDateTimeFormat"));
		importDateTimeFormatPanel.add(importDateTimeFormatLabel);
		importDateTimeFormatField = new JTextField();
		importDateTimeFormatField.setToolTipText(LangResources.get("importDateTimeFormat_help"));
		importDateTimeFormatField.setPreferredSize(new Dimension(200, importDateTimeFormatField.getPreferredSize().height));
		importDateTimeFormatPanel.add(importDateTimeFormatField);
		mandatoryParameterPanel.add(importDateTimeFormatPanel);

		// Optional parameters Panel
		final JPanel optionalParametersPanel = new JPanel();
		optionalParametersPanel.setLayout(new BoxLayout(optionalParametersPanel, BoxLayout.PAGE_AXIS));

		inlineDataBox = new JCheckBox(LangResources.get("inlineData"));
		inlineDataBox.setToolTipText(LangResources.get("inlineData_help"));
		optionalParametersPanel.add(inlineDataBox);

		fileLogBox = new JCheckBox(LangResources.get("filelog"));
		fileLogBox.setToolTipText(LangResources.get("filelog_help"));
		optionalParametersPanel.add(fileLogBox);

		allowUnderfilledLinesBox = new JCheckBox(LangResources.get("allowUnderfilledLines"));
		allowUnderfilledLinesBox.setToolTipText(LangResources.get("allowUnderfilledLines_help"));
		optionalParametersPanel.add(allowUnderfilledLinesBox);

		removeSurplusEmptyTrailingColumnsBox = new JCheckBox(LangResources.get("removeSurplusEmptyTrailingColumns"));
		removeSurplusEmptyTrailingColumnsBox.setToolTipText(LangResources.get("removeSurplusEmptyTrailingColumns_help"));
		optionalParametersPanel.add(removeSurplusEmptyTrailingColumnsBox);

		noHeadersBox = new JCheckBox(LangResources.get("noheaders"));
		noHeadersBox.setToolTipText(LangResources.get("noheaders_help"));
		optionalParametersPanel.add(noHeadersBox);

		createTableBox = new JCheckBox(LangResources.get("createTable"));
		createTableBox.setToolTipText(LangResources.get("createTable_help"));
		optionalParametersPanel.add(createTableBox);

		onlyCommitOnFullSuccessBox = new JCheckBox(LangResources.get("onlyCommitOnFullSuccess"));
		onlyCommitOnFullSuccessBox.setToolTipText(LangResources.get("onlyCommitOnFullSuccess_help"));
		optionalParametersPanel.add(onlyCommitOnFullSuccessBox);

		createNewIndexIfNeededBox = new JCheckBox(LangResources.get("createNewIndexIfNeeded"));
		createNewIndexIfNeededBox.setToolTipText(LangResources.get("createNewIndexIfNeeded_help"));
		optionalParametersPanel.add(createNewIndexIfNeededBox);

		deactivateForeignKeyConstraintsBox = new JCheckBox(LangResources.get("deactivateForeignKeyConstraints"));
		deactivateForeignKeyConstraintsBox.setToolTipText(LangResources.get("deactivateForeignKeyConstraints_help"));
		optionalParametersPanel.add(deactivateForeignKeyConstraintsBox);

		updateWithNullDataBox = new JCheckBox(LangResources.get("updateWithNullData"));
		updateWithNullDataBox.setToolTipText(LangResources.get("updateWithNullData_help"));
		optionalParametersPanel.add(updateWithNullDataBox);

		trimDataBox = new JCheckBox(LangResources.get("trimData"));
		trimDataBox.setToolTipText(LangResources.get("trimData_help"));
		optionalParametersPanel.add(trimDataBox);

		logErroneousDataBox = new JCheckBox(LangResources.get("logErroneousData"));
		logErroneousDataBox.setToolTipText(LangResources.get("logErroneousData_help"));
		optionalParametersPanel.add(logErroneousDataBox);

		// Button Pane
		final JPanel buttonPanel = new JPanel();
		buttonPanel.setLayout(new BoxLayout(buttonPanel, BoxLayout.LINE_AXIS));

		// Start Button
		final JButton startButton = new JButton(LangResources.get("import"));
		startButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent event) {
				try {
					importData(getConfigurationAsDefinition(), dbImportGui);
				} catch (final Exception e) {
					new QuestionDialog(dbImportGui, DbImport.APPLICATION_NAME + " ERROR", "ERROR:\n" + e.getMessage()).setBackgroundColor(SwingColor.LightRed).open();
				}
			}
		});
		buttonPanel.add(startButton);

		buttonPanel.add(Box.createRigidArea(new Dimension(5, 0)));

		// Mapping Button
		mappingButton = new JButton(LangResources.get("createMapping"));
		mappingButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent event) {
				try {
					createMapping(getConfigurationAsDefinition(), dbImportGui);
				} catch (final Exception e) {
					new QuestionDialog(dbImportGui, DbImport.APPLICATION_NAME + " ERROR", "ERROR:\n" + e.getMessage()).setBackgroundColor(SwingColor.LightRed).open();
				}
			}
		});
		buttonPanel.add(mappingButton);

		// Preferences Button
		final JButton preferencesButton = new JButton(LangResources.get("preferences"));
		preferencesButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent event) {
				try {
					final SecurePreferencesDialog credentialsDialog = new SecurePreferencesDialog(dbImportGui, DbImport.APPLICATION_NAME + " " + LangResources.get("preferences"),
							LangResources.get("preferences_text"), DbImport.SECURE_PREFERENCES_FILE, LangResources.get("load"), LangResources.get("create"), LangResources.get("update"),
							LangResources.get("delete"), LangResources.get("preferences_save"), LangResources.get("cancel"), LangResources.get("preferences_password_text"),
							LangResources.get("password"), LangResources.get("ok"), LangResources.get("cancel"));

					credentialsDialog.setCurrentDataEntry(getConfigurationAsDefinition());
					credentialsDialog.setPassword(temporaryPreferencesPassword);
					credentialsDialog.open();
					if (credentialsDialog.getCurrentDataEntry() != null) {
						setConfigurationByDefinition((DbImportDefinition) credentialsDialog.getCurrentDataEntry());
					}

					temporaryPreferencesPassword = credentialsDialog.getPassword();
				} catch (final Exception e) {
					temporaryPreferencesPassword = null;
					new QuestionDialog(dbImportGui, DbImport.APPLICATION_NAME + " ERROR", "ERROR:\n" + e.getMessage()).setBackgroundColor(SwingColor.LightRed).open();
				}
			}
		});
		buttonPanel.add(preferencesButton);

		buttonPanel.add(Box.createRigidArea(new Dimension(5, 0)));

		// Configuration Button
		final JButton configurationButton = new JButton(LangResources.get("configuration"));
		configurationButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent event) {
				try {
					byte[] iconData;
					try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream("DbImport.ico")) {
						iconData = IoUtilities.toByteArray(inputStream);
					}

					Image iconImage;
					try (InputStream imageIconStream = getClass().getClassLoader().getResourceAsStream("DbImport_Icon.png")) {
						iconImage = ImageIO.read(getClass().getClassLoader().getResource("DbImport_Icon.png"));
					}

					final ApplicationConfigurationDialog applicationConfigurationDialog = new ApplicationConfigurationDialog(dbImportGui, DbImport.APPLICATION_NAME, DbImport.APPLICATION_STARTUPCLASS_NAME, DbImport.VERSION, DbImport.VERSION_BUILDTIME, applicationConfiguration, iconData, iconImage, DbImport.VERSIONINFO_DOWNLOAD_URL, DbImport.TRUSTED_UPDATE_CA_CERTIFICATE);
					final Result result = applicationConfigurationDialog.open();
					if (result == Result.OK) {
						applicationConfiguration.save();
					}
				} catch (final Exception e) {
					new QuestionDialog(dbImportGui, DbImport.APPLICATION_NAME + " ERROR", "ERROR:\n" + e.getMessage()).setBackgroundColor(SwingColor.LightRed).open();
				}
			}
		});
		buttonPanel.add(configurationButton);

		buttonPanel.add(Box.createRigidArea(new Dimension(5, 0)));

		// Close Button
		final JButton closeButton = new JButton(LangResources.get("close"));
		closeButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent event) {
				dispose();
				System.exit(0);
			}
		});
		buttonPanel.add(closeButton);

		final JScrollPane mandatoryParameterScrollPane = new JScrollPane(mandatoryParameterPanel);
		mandatoryParameterScrollPane.setPreferredSize(new Dimension(475, 400));
		mandatoryParameterScrollPane.getVerticalScrollBar().setUnitIncrement(8);
		parameterPanel.add(mandatoryParameterScrollPane);

		optionalParametersPanel.setPreferredSize(new Dimension(250, 400));
		parameterPanel.add(optionalParametersPanel);
		add(parameterPanel);
		add(Box.createRigidArea(new Dimension(0, 5)));
		add(buttonPanel);

		setConfigurationByDefinition(dbImportDefinition);

		checkButtonStatus();

		add(Box.createRigidArea(new Dimension(0, 5)));

		pack();

		setLocationRelativeTo(null);
		setResizable(false);
	}

	protected void createMapping(final DbImportDefinition configurationAsDefinition, final DbImportGui dbImportGui) throws Exception {
		if (Utilities.isBlank(importFilePathOrDataField.getText())) {
			throw new Exception("ImportFilePath is needed for mapping");
		} else if (Utilities.isBlank(tableNameField.getText())) {
			throw new Exception("TableName is needed for mapping");
		}

		try (Connection connection = DbUtilities.createConnection(configurationAsDefinition, true)) {
			CaseInsensitiveMap<DbColumnType> columnTypes = null;
			if (DbUtilities.checkTableExist(connection, configurationAsDefinition.getTableName())) {
				columnTypes = DbUtilities.getColumnDataTypes(connection, configurationAsDefinition.getTableName());
				final List<String> keyColumns = new ArrayList<>(DbUtilities.getPrimaryKeyColumns(connection, configurationAsDefinition.getTableName()));
				Collections.sort(keyColumns);
				if (Utilities.isBlank(keyColumnsField.getText())) {
					keyColumnsField.setText(Utilities.join(keyColumns, ", "));
				}
			} else if (!configurationAsDefinition.isCreateTable()) {
				throw new Exception("Destination table does not exist: " + configurationAsDefinition.getTableName());
			}

			if (!configurationAsDefinition.isInlineData()) {
				if (configurationAsDefinition.getImportFilePathOrData().contains("?") || configurationAsDefinition.getImportFilePathOrData().contains("*")) {
					throw new Exception("ImportFilePath may not contain wildcards for create mapping: " + configurationAsDefinition.getImportFilePathOrData());
				}
			}

			final DbImportWorker worker = configurationAsDefinition.getConfiguredWorker(null, true, configurationAsDefinition.getTableName(), configurationAsDefinition.getImportFilePathOrData());
			final ProgressDialog<DbImportWorker> progressDialog = new ProgressDialog<>(this, DbImport.APPLICATION_NAME, null, worker);
			final Result result = progressDialog.open();

			if (result == Result.CANCELED) {
				new QuestionDialog(dbImportGui, DbImport.APPLICATION_NAME, LangResources.get("error.canceledbyuser")).setBackgroundColor(SwingColor.Yellow).open();
			} else if (result == Result.ERROR) {
				final Exception e = worker.getError();
				if (e instanceof DbImportException) {
					new QuestionDialog(dbImportGui, DbImport.APPLICATION_NAME + " ERROR", "ERROR:\n" + ((DbImportException) e).getMessage()).setBackgroundColor(SwingColor.LightRed).open();
				} else {
					final String stacktrace = ExceptionUtilities.getStackTrace(e);
					new QuestionDialog(dbImportGui, DbImport.APPLICATION_NAME + " ERROR", "ERROR:\n" + e.getClass().getSimpleName() + ":\n" + e.getMessage() + "\n\n" + stacktrace).setBackgroundColor(SwingColor.LightRed).open();
				}
			} else {
				final List<String> dataColumns = worker.getDataPropertyNames();
				if (columnTypes != null) {
					final DbImportMappingDialog columnMappingDialog = new DbImportMappingDialog(this, DbImport.APPLICATION_NAME + " " + LangResources.get("mapping"), columnTypes, dataColumns, configurationAsDefinition.getKeycolumns());
					columnMappingDialog.setMappingString(mappingField.getText());
					columnMappingDialog.open();
					mappingField.setText(columnMappingDialog.getMappingString());
				} else {
					String mappingString = "";
					for (final String dataColumn : dataColumns) {
						if (mappingString.length() > 0) {
							mappingString += "\n";
						}
						mappingString += dataColumn.toLowerCase() + "=\"" + dataColumn + "\"";
					}
					mappingField.setText(mappingString);
				}
			}
		}
	}

	/**
	 * Gets the configuration as definition.
	 *
	 * @return the configuration as definition
	 * @throws Exception
	 *             the exception
	 */
	private DbImportDefinition getConfigurationAsDefinition() throws Exception {
		final DbImportDefinition dbImportDefinition = new DbImportDefinition();

		dbImportDefinition.setDbVendor(DbVendor.getDbVendorByName((String) dbTypeCombo.getSelectedItem()));
		dbImportDefinition.setHostnameAndPort(hostField.isEnabled() ? hostField.getText() : null);
		dbImportDefinition.setDbName(dbNameField.getText());
		dbImportDefinition.setUsername(userField.isEnabled() ? userField.getText() : null);
		dbImportDefinition.setPassword(passwordField.isEnabled() ? passwordField.getPassword() : null);
		dbImportDefinition.setSecureConnection(secureConnectionBox.isEnabled() && secureConnectionBox.isSelected());
		dbImportDefinition.setTrustStoreFile(trustStoreFilePathField.isEnabled() ? new File(trustStoreFilePathField.getText()) : null);
		dbImportDefinition.setTableName(tableNameField.getText());
		dbImportDefinition.setImportFilePathOrData(importFilePathOrDataField.getText(), false);
		dbImportDefinition.setDataType(DataType.getFromString((String) dataTypeCombo.getSelectedItem()));
		dbImportDefinition.setLog(fileLogBox.isSelected());
		dbImportDefinition.setEncoding(Charset.forName((String) encodingCombo.getSelectedItem()));
		dbImportDefinition.setSeparator(((String) separatorCombo.getSelectedItem()).charAt(0));
		dbImportDefinition.setStringQuote(Utilities.isNotEmpty((String) stringQuoteCombo.getSelectedItem()) ? ((String) stringQuoteCombo.getSelectedItem()).charAt(0) : null);
		dbImportDefinition.setEscapeStringQuote(((String) escapeStringQuoteCombo.getSelectedItem()).charAt(0));
		dbImportDefinition.setNoHeaders(noHeadersBox.isSelected());
		dbImportDefinition.setNullValueString((String) nullValueStringCombo.getSelectedItem());
		dbImportDefinition.setCompleteCommit(onlyCommitOnFullSuccessBox.isSelected());
		dbImportDefinition.setCreateNewIndexIfNeeded(createNewIndexIfNeededBox.isSelected());
		dbImportDefinition.setDeactivateForeignKeyConstraints(deactivateForeignKeyConstraintsBox.isEnabled() && deactivateForeignKeyConstraintsBox.isSelected());
		dbImportDefinition.setAllowUnderfilledLines(allowUnderfilledLinesBox.isSelected());
		dbImportDefinition.setRemoveSurplusEmptyTrailingColumns(removeSurplusEmptyTrailingColumnsBox.isSelected());
		dbImportDefinition.setImportMode(ImportMode.getFromString(((String) importModeCombo.getSelectedItem())));
		dbImportDefinition.setDuplicateMode(DuplicateMode.getFromString(((String) duplicateModeCombo.getSelectedItem())));
		dbImportDefinition.setUpdateNullData(updateWithNullDataBox.isSelected());
		dbImportDefinition.setKeycolumns(Utilities.splitAndTrimList(keyColumnsField.getText()));
		dbImportDefinition.setCreateTable(createTableBox.isSelected());
		dbImportDefinition.setStructureFilePath(structureFilePathField.getText());
		if (schemaFilePathField.isEnabled() && Utilities.isNotBlank(schemaFilePathField.getText())) {
			dbImportDefinition.setSchemaFilePath(schemaFilePathField.getText());
		}
		dbImportDefinition.setMapping(mappingField.getText());
		dbImportDefinition.setAdditionalInsertValues(additionalInsertValuesField.getText());
		dbImportDefinition.setAdditionalUpdateValues(additionalUpdateValuesField.getText());
		dbImportDefinition.setTrimData(trimDataBox.isSelected());
		dbImportDefinition.setLogErroneousData(logErroneousDataBox.isSelected());
		// null stands for no usage of zip password, but GUI field text is always not null, so use empty field as deactivation of zip password
		dbImportDefinition.setZipPassword(Utilities.isEmpty(zipPasswordField.getPassword()) ? null : zipPasswordField.getPassword());
		dbImportDefinition.setDatabaseTimeZone((String) databaseTimezoneCombo.getSelectedItem());
		dbImportDefinition.setImportDataTimeZone((String) importDataTimezoneCombo.getSelectedItem());

		if (Utilities.isNotBlank(importDateFormatField.getText()) && importDateFormatField.isEnabled()) {
			dbImportDefinition.setDateFormat(importDateFormatField.getText());
		}

		if (Utilities.isNotBlank(importDateTimeFormatField.getText()) && importDateTimeFormatField.isEnabled()) {
			dbImportDefinition.setDateTimeFormat(importDateTimeFormatField.getText());
		}

		return dbImportDefinition;
	}

	/**
	 * Sets the configuration by definition.
	 *
	 * @param dbImportDefinition
	 *            the new configuration by definition
	 * @throws Exception
	 *             the exception
	 */
	private void setConfigurationByDefinition(final DbImportDefinition dbImportDefinition) throws Exception {
		for (int i = 0; i < dbTypeCombo.getItemCount(); i++) {
			if (DbUtilities.DbVendor.getDbVendorByName(dbTypeCombo.getItemAt(i)) == dbImportDefinition.getDbVendor()) {
				dbTypeCombo.setSelectedIndex(i);
				break;
			}
		}

		hostField.setText(dbImportDefinition.getHostnameAndPort());
		dbNameField.setText(dbImportDefinition.getDbName());
		userField.setText(dbImportDefinition.getUsername());
		passwordField.setText(dbImportDefinition.getPassword() == null ? "" : new String(dbImportDefinition.getPassword()));
		secureConnectionBox.setSelected(dbImportDefinition.isSecureConnection());
		trustStoreFilePathField.setText(dbImportDefinition.getTrustStoreFile() == null ? "" : dbImportDefinition.getTrustStoreFile().getAbsolutePath());
		zipPasswordField.setText(dbImportDefinition.getZipPassword() == null ? "" : new String(dbImportDefinition.getZipPassword()));

		tableNameField.setText(dbImportDefinition.getTableName());
		importFilePathOrDataField.setText(dbImportDefinition.getImportFilePathOrData());

		for (int i = 0; i < dataTypeCombo.getItemCount(); i++) {
			if (dataTypeCombo.getItemAt(i).equalsIgnoreCase(dbImportDefinition.getDataType().toString())) {
				dataTypeCombo.setSelectedIndex(i);
				break;
			}
		}

		fileLogBox.setSelected(dbImportDefinition.isLog());

		boolean encodingFound = false;
		for (int i = 0; i < encodingCombo.getItemCount(); i++) {
			if (encodingCombo.getItemAt(i).equalsIgnoreCase(dbImportDefinition.getEncoding().name())) {
				encodingCombo.setSelectedIndex(i);
				encodingFound = true;
				break;
			}
		}
		if (!encodingFound) {
			encodingCombo.setSelectedItem(dbImportDefinition.getEncoding());
		}

		boolean separatorFound = false;
		for (int i = 0; i < separatorCombo.getItemCount(); i++) {
			if (separatorCombo.getItemAt(i).equalsIgnoreCase(Character.toString(dbImportDefinition.getSeparator()))) {
				separatorCombo.setSelectedIndex(i);
				separatorFound = true;
				break;
			}
		}
		if (!separatorFound) {
			separatorCombo.setSelectedItem(Character.toString(dbImportDefinition.getSeparator()));
		}

		boolean stringQuoteFound = false;
		if (dbImportDefinition.getStringQuote() == null) {
			stringQuoteCombo.setSelectedItem("");
			stringQuoteFound = true;
		} else {
			for (int i = 0; i < stringQuoteCombo.getItemCount(); i++) {
				if (stringQuoteCombo.getItemAt(i).equalsIgnoreCase(Character.toString(dbImportDefinition.getStringQuote()))) {
					stringQuoteCombo.setSelectedIndex(i);
					stringQuoteFound = true;
					break;
				}
			}
		}
		if (!stringQuoteFound) {
			stringQuoteCombo.setSelectedItem(Character.toString(dbImportDefinition.getStringQuote()));
		}

		boolean escapeStringQuoteFound = false;
		for (int i = 0; i < escapeStringQuoteCombo.getItemCount(); i++) {
			if (escapeStringQuoteCombo.getItemAt(i).equalsIgnoreCase(Character.toString(dbImportDefinition.getEscapeStringQuote()))) {
				escapeStringQuoteCombo.setSelectedIndex(i);
				escapeStringQuoteFound = true;
				break;
			}
		}
		if (!escapeStringQuoteFound) {
			escapeStringQuoteCombo.setSelectedItem(Character.toString(dbImportDefinition.getEscapeStringQuote()));
		}

		noHeadersBox.setSelected(dbImportDefinition.isNoHeaders());

		boolean nullValueStringFound = false;
		for (int i = 0; i < nullValueStringCombo.getItemCount(); i++) {
			if (nullValueStringCombo.getItemAt(i).equals(dbImportDefinition.getNullValueString())) {
				nullValueStringCombo.setSelectedIndex(i);
				nullValueStringFound = true;
				break;
			}
		}
		if (!nullValueStringFound) {
			nullValueStringCombo.setSelectedItem(dbImportDefinition.getNullValueString());
		}

		onlyCommitOnFullSuccessBox.setSelected(dbImportDefinition.isCompleteCommit());
		createNewIndexIfNeededBox.setSelected(dbImportDefinition.isCreateNewIndexIfNeeded());
		deactivateForeignKeyConstraintsBox.setSelected(dbImportDefinition.isDeactivateForeignKeyConstraints());
		allowUnderfilledLinesBox.setSelected(dbImportDefinition.isAllowUnderfilledLines());
		removeSurplusEmptyTrailingColumnsBox.setSelected(dbImportDefinition.isRemoveSurplusEmptyTrailingColumns());

		boolean importModeFound = false;
		for (int i = 0; i < importModeCombo.getItemCount(); i++) {
			if (importModeCombo.getItemAt(i).equalsIgnoreCase(dbImportDefinition.getImportMode().toString())) {
				importModeCombo.setSelectedIndex(i);
				importModeFound = true;
				break;
			}
		}
		if (!importModeFound) {
			throw new Exception("Invalid import mode");
		}

		boolean duplicateModeFound = false;
		for (int i = 0; i < duplicateModeCombo.getItemCount(); i++) {
			if (duplicateModeCombo.getItemAt(i).equalsIgnoreCase(dbImportDefinition.getDuplicateMode().toString())) {
				duplicateModeCombo.setSelectedIndex(i);
				duplicateModeFound = true;
				break;
			}
		}
		if (!duplicateModeFound) {
			throw new Exception("Invalid duplicate mode");
		}

		importDateFormatField.setText(dbImportDefinition.getDateFormat());
		importDateTimeFormatField.setText(dbImportDefinition.getDateTimeFormat());

		updateWithNullDataBox.setSelected(dbImportDefinition.isUpdateNullData());

		keyColumnsField.setText(Utilities.join(dbImportDefinition.getKeycolumns(), ", "));

		createTableBox.setSelected(dbImportDefinition.isCreateTable());

		mappingField.setText(dbImportDefinition.getMapping());

		additionalInsertValuesField.setText(dbImportDefinition.getAdditionalInsertValues());

		additionalUpdateValuesField.setText(dbImportDefinition.getAdditionalUpdateValues());

		databaseTimezoneCombo.setSelectedItem(dbImportDefinition.getDatabaseTimeZone());

		importDataTimezoneCombo.setSelectedItem(dbImportDefinition.getImportDataTimeZone());

		trimDataBox.setSelected(dbImportDefinition.isTrimData());

		logErroneousDataBox.setSelected(dbImportDefinition.isLogErroneousData());

		checkButtonStatus();
	}

	/**
	 * Check button status.
	 */
	private void checkButtonStatus() {
		if (DbVendor.SQLite.toString().equalsIgnoreCase((String) dbTypeCombo.getSelectedItem())
				|| DbVendor.Derby.toString().equalsIgnoreCase((String) dbTypeCombo.getSelectedItem())) {
			hostField.setEnabled(false);
			userField.setEnabled(false);
			passwordField.setEnabled(false);
			secureConnectionBox.setEnabled(false);
			trustStoreFilePathField.setEnabled(false);
			trustStoreFileButton.setEnabled(false);
			createTrustStoreFileButton.setEnabled(false);
			trustStorePasswordField.setEnabled(false);

			connectionCheckButton.setEnabled(Utilities.isNotBlank(dbNameField.getText()));
		} else {
			hostField.setEnabled(true);
			userField.setEnabled(true);
			passwordField.setEnabled(true);
			secureConnectionBox.setEnabled(
					DbVendor.Oracle.toString().equalsIgnoreCase((String) dbTypeCombo.getSelectedItem())
					|| DbVendor.MySQL.toString().equalsIgnoreCase((String) dbTypeCombo.getSelectedItem())
					|| DbVendor.MariaDB.toString().equalsIgnoreCase((String) dbTypeCombo.getSelectedItem()));
			trustStoreFilePathField.setEnabled(secureConnectionBox.isEnabled() && secureConnectionBox.isSelected());
			trustStoreFileButton.setEnabled(secureConnectionBox.isEnabled() && secureConnectionBox.isSelected());
			createTrustStoreFileButton.setEnabled(DbVendor.Oracle.toString().equalsIgnoreCase((String) dbTypeCombo.getSelectedItem())
					&& secureConnectionBox.isEnabled() && secureConnectionBox.isSelected() && Utilities.isNotBlank(hostField.getText()));
			trustStorePasswordField.setEnabled(secureConnectionBox.isEnabled() && secureConnectionBox.isSelected() && Utilities.isNotBlank(trustStoreFilePathField.getText()));

			connectionCheckButton.setEnabled(
					Utilities.isNotBlank(dbNameField.getText())
					&& Utilities.isNotBlank(hostField.getText())
					&& Utilities.isNotBlank(userField.getText()) || DbVendor.Cassandra.toString().equalsIgnoreCase((String) dbTypeCombo.getSelectedItem())
					&& Utilities.isNotBlank(passwordField.getPassword()) || DbVendor.Cassandra.toString().equalsIgnoreCase((String) dbTypeCombo.getSelectedItem()));
		}

		if (DataType.CSV.toString().equalsIgnoreCase((String) dataTypeCombo.getSelectedItem())) {
			separatorCombo.setEnabled(true);
			stringQuoteCombo.setEnabled(true);
			escapeStringQuoteCombo.setEnabled(true);
			noHeadersBox.setEnabled(true);
			nullValueStringCombo.setEnabled(true);
			allowUnderfilledLinesBox.setEnabled(true);
			removeSurplusEmptyTrailingColumnsBox.setEnabled(true);
			tableNameField.setEnabled(true);
			createTableBox.setEnabled(true);
			updateWithNullDataBox.setEnabled(true);
			trimDataBox.setEnabled(true);
			importModeCombo.setEnabled(true);
			duplicateModeCombo.setEnabled(true);
			keyColumnsField.setEnabled(true);
			mappingField.setEnabled(true);
			additionalInsertValuesField.setEnabled(true);
			additionalUpdateValuesField.setEnabled(true);
			dataPathField.setEnabled(false);
			schemaFilePathField.setEnabled(false);
			schemaFileButton.setEnabled(false);
		} else if (DataType.JSON.toString().equalsIgnoreCase((String) dataTypeCombo.getSelectedItem())) {
			separatorCombo.setEnabled(false);
			stringQuoteCombo.setEnabled(false);
			escapeStringQuoteCombo.setEnabled(false);
			noHeadersBox.setEnabled(false);
			nullValueStringCombo.setEnabled(false);
			allowUnderfilledLinesBox.setEnabled(false);
			removeSurplusEmptyTrailingColumnsBox.setEnabled(false);
			tableNameField.setEnabled(true);
			createTableBox.setEnabled(true);
			updateWithNullDataBox.setEnabled(true);
			trimDataBox.setEnabled(true);
			importModeCombo.setEnabled(true);
			duplicateModeCombo.setEnabled(true);
			keyColumnsField.setEnabled(true);
			mappingField.setEnabled(true);
			additionalInsertValuesField.setEnabled(true);
			additionalUpdateValuesField.setEnabled(true);
			dataPathField.setEnabled(true);
			schemaFilePathField.setEnabled(true);
			schemaFileButton.setEnabled(true);
		} else if (DataType.XML.toString().equalsIgnoreCase((String) dataTypeCombo.getSelectedItem())) {
			separatorCombo.setEnabled(false);
			stringQuoteCombo.setEnabled(false);
			escapeStringQuoteCombo.setEnabled(false);
			noHeadersBox.setEnabled(false);
			nullValueStringCombo.setEnabled(true);
			allowUnderfilledLinesBox.setEnabled(false);
			removeSurplusEmptyTrailingColumnsBox.setEnabled(false);
			tableNameField.setEnabled(true);
			createTableBox.setEnabled(true);
			updateWithNullDataBox.setEnabled(true);
			trimDataBox.setEnabled(true);
			importModeCombo.setEnabled(true);
			duplicateModeCombo.setEnabled(true);
			keyColumnsField.setEnabled(true);
			mappingField.setEnabled(true);
			additionalInsertValuesField.setEnabled(true);
			additionalUpdateValuesField.setEnabled(true);
			dataPathField.setEnabled(true);
			schemaFilePathField.setEnabled(true);
			schemaFileButton.setEnabled(true);
		} else if (DataType.SQL.toString().equalsIgnoreCase((String) dataTypeCombo.getSelectedItem())) {
			separatorCombo.setEnabled(false);
			stringQuoteCombo.setEnabled(false);
			escapeStringQuoteCombo.setEnabled(false);
			noHeadersBox.setEnabled(false);
			nullValueStringCombo.setEnabled(false);
			allowUnderfilledLinesBox.setEnabled(false);
			removeSurplusEmptyTrailingColumnsBox.setEnabled(false);
			tableNameField.setEnabled(false);
			createTableBox.setEnabled(false);
			updateWithNullDataBox.setEnabled(false);
			trimDataBox.setEnabled(false);
			importModeCombo.setEnabled(false);
			duplicateModeCombo.setEnabled(false);
			keyColumnsField.setEnabled(false);
			mappingField.setEnabled(false);
			additionalInsertValuesField.setEnabled(false);
			additionalUpdateValuesField.setEnabled(false);
			dataPathField.setEnabled(false);
			schemaFilePathField.setEnabled(false);
			schemaFileButton.setEnabled(false);
		} else if (DataType.EXCEL.toString().equalsIgnoreCase((String) dataTypeCombo.getSelectedItem())) {
			separatorCombo.setEnabled(false);
			stringQuoteCombo.setEnabled(false);
			escapeStringQuoteCombo.setEnabled(false);
			noHeadersBox.setEnabled(true);
			nullValueStringCombo.setEnabled(true);
			allowUnderfilledLinesBox.setEnabled(true);
			removeSurplusEmptyTrailingColumnsBox.setEnabled(false);
			tableNameField.setEnabled(true);
			createTableBox.setEnabled(true);
			updateWithNullDataBox.setEnabled(true);
			trimDataBox.setEnabled(true);
			importModeCombo.setEnabled(true);
			duplicateModeCombo.setEnabled(true);
			keyColumnsField.setEnabled(true);
			mappingField.setEnabled(true);
			additionalInsertValuesField.setEnabled(true);
			additionalUpdateValuesField.setEnabled(true);
			dataPathField.setEnabled(true);
			schemaFilePathField.setEnabled(false);
			schemaFileButton.setEnabled(true);
		} else if (DataType.ODS.toString().equalsIgnoreCase((String) dataTypeCombo.getSelectedItem())) {
			separatorCombo.setEnabled(false);
			stringQuoteCombo.setEnabled(false);
			escapeStringQuoteCombo.setEnabled(false);
			noHeadersBox.setEnabled(true);
			nullValueStringCombo.setEnabled(true);
			allowUnderfilledLinesBox.setEnabled(true);
			removeSurplusEmptyTrailingColumnsBox.setEnabled(false);
			tableNameField.setEnabled(true);
			createTableBox.setEnabled(true);
			updateWithNullDataBox.setEnabled(true);
			trimDataBox.setEnabled(true);
			importModeCombo.setEnabled(true);
			duplicateModeCombo.setEnabled(true);
			keyColumnsField.setEnabled(true);
			mappingField.setEnabled(true);
			additionalInsertValuesField.setEnabled(true);
			additionalUpdateValuesField.setEnabled(true);
			dataPathField.setEnabled(true);
			schemaFilePathField.setEnabled(false);
			schemaFileButton.setEnabled(true);
		}
		mappingField.setBackground(mappingField.isEnabled() ? Color.WHITE : Color.LIGHT_GRAY);

		if (DbVendor.Oracle.toString().equalsIgnoreCase((String) dbTypeCombo.getSelectedItem())
				|| DbVendor.MySQL.toString().equalsIgnoreCase((String) dbTypeCombo.getSelectedItem())
				|| DbVendor.MariaDB.toString().equalsIgnoreCase((String) dbTypeCombo.getSelectedItem())) {
			deactivateForeignKeyConstraintsBox.setEnabled(true);
		} else {
			deactivateForeignKeyConstraintsBox.setEnabled(false);
		}

		if (additionalInsertValuesField.isEnabled()) {
			additionalInsertValuesField.setEnabled(
					ImportMode.CLEARINSERT.toString().equalsIgnoreCase((String) importModeCombo.getSelectedItem())
					|| ImportMode.INSERT.toString().equalsIgnoreCase((String) importModeCombo.getSelectedItem())
					|| ImportMode.UPSERT.toString().equalsIgnoreCase((String) importModeCombo.getSelectedItem()));
		}
		additionalInsertValuesField.setBackground(additionalInsertValuesField.isEnabled() ? Color.WHITE : Color.LIGHT_GRAY);

		if (additionalUpdateValuesField.isEnabled()) {
			additionalUpdateValuesField.setEnabled(
					ImportMode.UPDATE.toString().equalsIgnoreCase((String) importModeCombo.getSelectedItem())
					|| ImportMode.UPSERT.toString().equalsIgnoreCase((String) importModeCombo.getSelectedItem()));
		}
		additionalUpdateValuesField.setBackground(additionalUpdateValuesField.isEnabled() ? Color.WHITE : Color.LIGHT_GRAY);

		createCompleteStructureFileButton.setEnabled(Utilities.isNotBlank(structureFilePathField.getText()));
	}

	/**
	 * Import.
	 *
	 * @param dbImportDefinition
	 *            the db csv import definition
	 * @param dbImportGui
	 *            the db csv import gui
	 * @throws Exception
	 */
	private void importData(final DbImportDefinition dbImportDefinition, final DbImportGui dbImportGui) throws Exception {
		try {
			dbImportDefinition.checkParameters();
			if (!new DbDriverSupplier(this, dbImportDefinition.getDbVendor()).supplyDriver(DbImport.APPLICATION_NAME, DbImport.CONFIGURATION_FILE)) {
				throw new Exception("Cannot aquire db driver for db vendor: " + dbImportDefinition.getDbVendor());
			}
		} catch (final Exception e) {
			new QuestionDialog(dbImportGui, DbImport.APPLICATION_NAME + " ERROR", "ERROR:\n" + e.getMessage()).setBackgroundColor(SwingColor.LightRed).open();
			return;
		}

		if (!dbImportDefinition.isInlineData()) {
			if (dbImportDefinition.getImportFilePathOrData().contains("?") || dbImportDefinition.getImportFilePathOrData().contains("*") || "*".equals(dbImportDefinition.getTableName())) {
				final int lastSeparator = Math.max(dbImportDefinition.getImportFilePathOrData().lastIndexOf("/"), dbImportDefinition.getImportFilePathOrData().lastIndexOf("\\"));
				String directoryPath = dbImportDefinition.getImportFilePathOrData().substring(0, lastSeparator);
				directoryPath = Utilities.replaceUsersHome(directoryPath);
				final String filePattern = dbImportDefinition.getImportFilePathOrData().substring(lastSeparator + 1);
				if (directoryPath.contains("?") || directoryPath.contains("*")) {
					throw new DbImportException("Import directory path contains wildcards, but wildcards only allowed for filenames: " + (directoryPath));
				} else if (!new File(directoryPath).exists()) {
					throw new DbImportException("Import path does not exist: " + (directoryPath));
				} else if (!new File((directoryPath)).isDirectory()) {
					throw new DbImportException("Import path is not a directory: " + (directoryPath));
				} else {
					final List<File> filesToImport = FileUtilities.getFilesByPattern(new File(directoryPath), filePattern.replace(".", "\\.").replace("?", ".").replace("*", ".*"), false);
					if (filesToImport.size() == 0) {
						throw new DbImportException("Import file pattern has no matching files: " + (directoryPath));
					} else {
						Collections.sort(filesToImport);
						multiImportFiles(dbImportGui, dbImportDefinition, filesToImport, dbImportDefinition.getTableName());
					}
				}
			} else {
				if (!new File(dbImportDefinition.getImportFilePathOrData()).exists()) {
					throw new DbImportException("Import file does not exist: " + (dbImportDefinition.getImportFilePathOrData()));
				} else if (new File(dbImportDefinition.getImportFilePathOrData()).isDirectory()) {
					throw new DbImportException("Import path is a directory: " + (dbImportDefinition.getImportFilePathOrData()));
				} else {
					importFileOrData(dbImportGui, dbImportDefinition, dbImportDefinition.getTableName(), dbImportDefinition.getImportFilePathOrData());
				}
			}

		} else {
			importFileOrData(dbImportGui, dbImportDefinition, dbImportDefinition.getTableName(), dbImportDefinition.getImportFilePathOrData());
		}
	}

	private void importFileOrData(final DbImportGui dbImportGui, final DbImportDefinition dbImportDefinition, final String tableNameToImport, final String filePathOrImportData) throws Exception {
		try {
			final DbImportWorker worker = dbImportDefinition.getConfiguredWorker(null, false, tableNameToImport, filePathOrImportData);
			final ProgressDialog<DbImportWorker> progressDialog = new ProgressDialog<>(this, DbImport.APPLICATION_NAME, null, worker);
			final Result result = progressDialog.open();

			if (result == Result.CANCELED) {
				new QuestionDialog(dbImportGui, DbImport.APPLICATION_NAME, LangResources.get("error.canceledbyuser")).setBackgroundColor(SwingColor.Yellow).open();
			} else if (result == Result.ERROR) {
				final Exception e = worker.getError();
				if (e instanceof DbImportException) {
					new QuestionDialog(dbImportGui, DbImport.APPLICATION_NAME + " ERROR", "ERROR:\n" + ((DbImportException) e).getMessage()).setBackgroundColor(SwingColor.LightRed).open();
				} else {
					final String stacktrace = ExceptionUtilities.getStackTrace(e);
					new QuestionDialog(dbImportGui, DbImport.APPLICATION_NAME + " ERROR", "ERROR:\n" + e.getClass().getSimpleName() + ":\n" + e.getMessage() + "\n\n" + stacktrace).setBackgroundColor(SwingColor.LightRed).open();
				}
			} else {
				final LocalDateTime start = worker.getStartTime();
				final LocalDateTime end = worker.getEndTime();

				String resultText = LangResources.get("start") + ": " + DateUtilities.formatDate(DateUtilities.YYYY_MM_DD_HHMMSS, start) + "\n"
						+ LangResources.get("end") + ": " + DateUtilities.formatDate(DateUtilities.YYYY_MM_DD_HHMMSS, end) + "\n"
						+ LangResources.get("timeelapsed") + ": " + DateUtilities.getHumanReadableTimespan(Duration.between(start, end), true);

				resultText += "\n" + worker.getResultStatistics();

				Color backgroundColor = null;
				if (worker.getNotImportedItems().size() > 0) {
					backgroundColor = SwingColor.LightRed;
				}
				new QuestionDialog(dbImportGui, DbImport.APPLICATION_NAME, LangResources.get("result") + ":\n" + resultText).setBackgroundColor(backgroundColor).open();
			}
		} catch (final Exception e) {
			new QuestionDialog(dbImportGui, DbImport.APPLICATION_NAME + " ERROR", "ERROR:\n" + e.getMessage()).setBackgroundColor(SwingColor.LightRed).open();
		}
	}

	private void multiImportFiles(final DbImportGui dbImportGui, final DbImportDefinition dbImportDefinition, final List<File> filesToImport, final String tableName) throws Exception {
		try {
			final DbImportMultiWorker worker = new DbImportMultiWorker(dbImportDefinition, filesToImport, tableName);
			final DualProgressDialog<DbImportMultiWorker> progressDialog = new DualProgressDialog<>(this, DbImport.APPLICATION_NAME, "{0} / {1}", worker);
			final Result result = progressDialog.open();

			if (result == Result.CANCELED) {
				new QuestionDialog(dbImportGui, DbImport.APPLICATION_NAME, LangResources.get("error.canceledbyuser")).setBackgroundColor(SwingColor.Yellow).open();
			} else if (result == Result.ERROR) {
				final Exception e = worker.getError();
				if (e instanceof DbImportException) {
					new QuestionDialog(dbImportGui, DbImport.APPLICATION_NAME + " ERROR", "ERROR:\n" + ((DbImportException) e).getMessage()).setBackgroundColor(SwingColor.LightRed).open();
				} else {
					final String stacktrace = ExceptionUtilities.getStackTrace(e);
					new QuestionDialog(dbImportGui, DbImport.APPLICATION_NAME + " ERROR", "ERROR:\n" + e.getClass().getSimpleName() + ":\n" + e.getMessage() + "\n\n" + stacktrace).setBackgroundColor(SwingColor.LightRed).open();
				}
			} else {
				Color backgroundColor = null;
				if (worker.wasErrorneous()) {
					backgroundColor = SwingColor.LightRed;
				}
				new QuestionDialog(dbImportGui, DbImport.APPLICATION_NAME, LangResources.get("result") + ":\n" + worker.getResult()).setBackgroundColor(backgroundColor).open();
			}
		} catch (final Exception e) {
			new QuestionDialog(dbImportGui, DbImport.APPLICATION_NAME + " ERROR", "ERROR:\n" + e.getMessage()).setBackgroundColor(SwingColor.LightRed).open();
		}
	}

	private File selectFile(String basePath, final String text) {
		if (Utilities.isBlank(basePath)) {
			basePath = System.getProperty("user.home");
		} else if (basePath.contains(File.separator)) {
			basePath = basePath.substring(0, basePath.lastIndexOf(File.separator));
		}

		final JFileChooser fileChooser = new JFileChooser(basePath);
		fileChooser.setDialogTitle(DbImport.APPLICATION_NAME + " " + text);
		if (JFileChooser.APPROVE_OPTION == fileChooser.showOpenDialog(this)) {
			return fileChooser.getSelectedFile();
		} else {
			return null;
		}
	}

	public static void setupDefaultConfig(final ConfigurationProperties applicationConfiguration) {
		if (Utilities.isBlank(applicationConfiguration.get(ApplicationConfigurationDialog.CONFIG_LANGUAGE))) {
			applicationConfiguration.set(ApplicationConfigurationDialog.CONFIG_LANGUAGE, Locale.getDefault().getLanguage());
		}
	}

	@Override
	protected void setDailyUpdateCheckStatus(final boolean checkboxStatus) {
		applicationConfiguration.set(CONFIG_DAILY_UPDATE_CHECK, checkboxStatus);
		applicationConfiguration.set(CONFIG_NEXT_DAILY_UPDATE_CHECK, LocalDateTime.now().plusDays(1));
		applicationConfiguration.save();
	}

	@Override
	protected Boolean isDailyUpdateCheckActivated() {
		return applicationConfiguration.getBoolean(CONFIG_DAILY_UPDATE_CHECK);
	}

	protected boolean dailyUpdateCheckIsPending() {
		return applicationConfiguration.getBoolean(CONFIG_DAILY_UPDATE_CHECK)
				&& (applicationConfiguration.getDate(CONFIG_NEXT_DAILY_UPDATE_CHECK) == null || applicationConfiguration.getDate(CONFIG_NEXT_DAILY_UPDATE_CHECK).isBefore(LocalDateTime.now()))
				&& NetworkUtilities.checkForNetworkConnection();
	}
}
