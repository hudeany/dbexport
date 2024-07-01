package de.soderer.dbexport;

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
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Locale;
import java.util.TimeZone;

import javax.imageio.ImageIO;
import javax.swing.BorderFactory;
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

import de.soderer.dbexport.DbExportDefinition.DataType;
import de.soderer.dbexport.worker.AbstractDbExportWorker;
import de.soderer.utilities.ConfigurationProperties;
import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.ExceptionUtilities;
import de.soderer.utilities.FileCompressionType;
import de.soderer.utilities.IoUtilities;
import de.soderer.utilities.LangResources;
import de.soderer.utilities.NetworkUtilities;
import de.soderer.utilities.Result;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.VersionInfo;
import de.soderer.utilities.appupdate.ApplicationUpdateUtilities;
import de.soderer.utilities.db.DbDriverSupplier;
import de.soderer.utilities.db.DbUtilities;
import de.soderer.utilities.db.DbUtilities.DbVendor;
import de.soderer.utilities.http.HttpUtilities;
import de.soderer.utilities.http.ProxyConfiguration;
import de.soderer.utilities.http.ProxyConfiguration.ProxyConfigurationType;
import de.soderer.utilities.swing.ApplicationConfigurationDialog;
import de.soderer.utilities.swing.DualProgressDialog;
import de.soderer.utilities.swing.ProgressDialog;
import de.soderer.utilities.swing.QuestionDialog;
import de.soderer.utilities.swing.SecurePreferencesDialog;
import de.soderer.utilities.swing.SwingColor;
import de.soderer.utilities.swing.UpdateableGuiApplication;

/**
 * The GUI for DbExport.
 */
public class DbExportGui extends UpdateableGuiApplication {
	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = 5969613637206441880L;

	public static final File KEYSTORE_FILE = new File(System.getProperty("user.home") + File.separator + "." + DbExport.APPLICATION_NAME + File.separator + "." + DbExport.APPLICATION_NAME + ".keystore");
	public static final String CONFIG_DAILY_UPDATE_CHECK = "DailyUpdateCheck";
	public static final String CONFIG_NEXT_DAILY_UPDATE_CHECK = "NextDailyUpdateCheck";

	private final ConfigurationProperties applicationConfiguration;

	/** The database type combo. */
	private final JComboBox<String> dbTypeCombo;

	private final JButton connectionCheckButton;

	/** The host field. */
	private final JTextField hostField;

	/** The database name field. */
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

	/** The data type combo. */
	private final JComboBox<String> dataTypeCombo;

	/** The file log box. */
	private final JCheckBox fileLogBox;

	/** The outputpath field. */
	private final JTextField outputpathField;

	/** The zipPassword field. */
	private final JPasswordField zipPasswordField;

	/** The kdbxPassword field. */
	private final JPasswordField kdbxPasswordField;

	/** The separator combo. */
	private final JComboBox<String> separatorCombo;

	/** The string quote combo. */
	private final JComboBox<String> stringQuoteCombo;

	/** The string quote escape character combo. */
	private final JComboBox<String> stringQuoteEscapeCombo;

	/** The indentation combo. */
	private final JComboBox<String> indentationCombo;

	/** The null value string combo. */
	private final JComboBox<String> nullValueStringCombo;

	/** The encoding combo. */
	private final JComboBox<String> encodingCombo;

	/** The locale combo. */
	private final JComboBox<String> localeCombo;

	/** The statement field. */
	private final JTextArea statementField;

	/** The compressionType combo. */
	private final JComboBox<String> compressionTypeCombo;

	/** The useZipCrypto box. */
	private final JCheckBox useZipCryptoBox;

	/** The blobfiles box. */
	private final JCheckBox blobfilesBox;

	/** The clobfiles box. */
	private final JCheckBox clobfilesBox;

	/** The always quote box. */
	private final JCheckBox alwaysQuoteBox;

	/** The beautify box. */
	private final JCheckBox beautifyBox;

	/** The export structure box. */
	private final JCheckBox exportStructureBox;

	/** The no headers box. */
	private final JCheckBox noHeadersBox;

	private final JCheckBox createOutputDirectoyIfNotExistsBox;

	private final JCheckBox replaceAlreadyExistingFilesBox;

	/** The temporary preferences password. */
	private char[] temporaryPreferencesPassword = null;

	/** The field for databases timezone */
	private final JComboBox<String> databaseTimezoneCombo;

	/** The field for datafiles timezone */
	private final JComboBox<String> exportDataTimezoneCombo;

	/** The field for DateFormat */
	private final JTextField exportDateFormatField;

	/** The field for DateTimeFormat */
	private final JTextField exportDateTimeFormatField;

	/**
	 * Instantiates a new database csv export gui.
	 *
	 * @param dbExportDefinition
	 *            the database csv export definition
	 * @throws Exception
	 *             the exception
	 */
	public DbExportGui(final DbExportDefinition dbExportDefinition) throws Exception {
		super(DbExport.APPLICATION_NAME, DbExport.VERSION, KEYSTORE_FILE);

		setTitle(DbExport.APPLICATION_NAME + " (Version " + DbExport.VERSION.toString() + ")");

		applicationConfiguration = new ConfigurationProperties(DbExport.APPLICATION_NAME, true);
		DbExportGui.setupDefaultConfig(applicationConfiguration);
		if ("de".equalsIgnoreCase(applicationConfiguration.get(ApplicationConfigurationDialog.CONFIG_LANGUAGE))) {
			Locale.setDefault(Locale.GERMAN);
		} else {
			Locale.setDefault(Locale.ENGLISH);
		}

		final ProxyConfigurationType proxyConfigurationType = ProxyConfigurationType.getFromString(applicationConfiguration.get(ApplicationConfigurationDialog.CONFIG_PROXY_CONFIGURATION_TYPE));
		final String proxyUrl = applicationConfiguration.get(ApplicationConfigurationDialog.CONFIG_PROXY_URL);
		final ProxyConfiguration proxyConfiguration = new ProxyConfiguration(proxyConfigurationType, proxyUrl);

		if (dailyUpdateCheckIsPending()) {
			setDailyUpdateCheckStatus(true);
			try {
				if (ApplicationUpdateUtilities.checkForNewVersionAvailable(this, DbExport.VERSIONINFO_DOWNLOAD_URL, proxyConfiguration, DbExport.APPLICATION_NAME, VersionInfo.getApplicationVersion()) != null) {
					ApplicationUpdateUtilities.executeUpdate(this, DbExport.VERSIONINFO_DOWNLOAD_URL, proxyConfiguration, DbExport.APPLICATION_NAME, DbExport.VERSION, DbExport.TRUSTED_UPDATE_CA_CERTIFICATES, null, null, "gui", true);
				}
			} catch (final Exception e) {
				new QuestionDialog(this, DbExport.APPLICATION_NAME + " " + LangResources.get("updateCheck") + " ERROR", LangResources.get("error.cannotCheckForUpdate") + "\n" + "ERROR:\n" + e.getMessage()).setBackgroundColor(SwingColor.LightRed).open();
			}
		}

		try (InputStream imageIconStream = this.getClass().getClassLoader().getResourceAsStream("DbExport_Icon.png")) {
			final BufferedImage imageIcon = ImageIO.read(imageIconStream);
			setIconImage(imageIcon);
		}

		final DbExportGui dbExportGui = this;

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
				try (Connection connection = DbUtilities.createConnection(getConfigurationAsDefinition(), false)) {
					new QuestionDialog(dbExportGui, DbExport.APPLICATION_NAME + " OK", "OK").setBackgroundColor(SwingColor.Green).open();
				} catch (final Exception e) {
					new QuestionDialog(dbExportGui, DbExport.APPLICATION_NAME + " ERROR", "ERROR:\n" + e.getMessage()).setBackgroundColor(SwingColor.LightRed).open();
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

		// Database name Panel
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
					new QuestionDialog(dbExportGui, DbExport.APPLICATION_NAME + " ERROR", "ERROR:\n" + e.getMessage()).setBackgroundColor(SwingColor.LightRed).open();
				}
			}
		});
		trustStoreFilePathPanel.add(trustStoreFileButton);
		mandatoryParameterPanel.add(trustStoreFilePathPanel);

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
							new QuestionDialog(dbExportGui, DbExport.APPLICATION_NAME + " ERROR", "ERROR:\n" + "File already exists: '" + trustStoreFilePathField.getText() + "'").setBackgroundColor(SwingColor.LightRed).open();
						} else {
							HttpUtilities.createTrustStoreFile(hostField.getText(), DbVendor.getDbVendorByName((String) dbTypeCombo.getSelectedItem()).getDefaultPort(), new File(trustStoreFilePathField.getText()), trustStorePasswordField.getPassword(), null);
							new QuestionDialog(dbExportGui, DbExport.APPLICATION_NAME + " OK", "OK").setBackgroundColor(SwingColor.Green).open();
							checkButtonStatus();
						}
					}
				} catch (final Exception e) {
					new QuestionDialog(dbExportGui, DbExport.APPLICATION_NAME + " ERROR", "ERROR:\n" + e.getMessage()).setBackgroundColor(SwingColor.LightRed).open();
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

		// Data type Pane
		final JPanel dataTypePanel = new JPanel();
		dataTypePanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		final JLabel dataTypeLabel = new JLabel(LangResources.get("datatype"));
		dataTypePanel.add(dataTypeLabel);
		dataTypeCombo = new JComboBox<>();
		dataTypeCombo.setToolTipText(LangResources.get("datatype_help"));
		dataTypeCombo.setPreferredSize(new Dimension(200, dataTypeCombo.getPreferredSize().height));
		for (final DataType dataType : DataType.values()) {
			dataTypeCombo.addItem(dataType.toString());
		}
		dataTypeCombo.addItemListener(new ItemListener() {
			@Override
			public void itemStateChanged(final ItemEvent event) {
				if ("JSON".equalsIgnoreCase(((String) dataTypeCombo.getSelectedItem())) || "XML".equalsIgnoreCase(((String) dataTypeCombo.getSelectedItem()))) {
					beautifyBox.setSelected(true);
				} else {
					beautifyBox.setSelected(false);
				}
				checkButtonStatus();
			}
		});
		dataTypePanel.add(dataTypeCombo, BorderLayout.EAST);
		mandatoryParameterPanel.add(dataTypePanel);

		// Outputpath Panel
		final JPanel outputpathPanel = new JPanel();
		outputpathPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		final JLabel outputpathLabel = new JLabel(LangResources.get("outputpath"));
		outputpathPanel.add(outputpathLabel);
		outputpathField = new JTextField();
		outputpathField.setToolTipText(LangResources.get("outputpath_help"));
		outputpathField.setPreferredSize(new Dimension(200, outputpathField.getPreferredSize().height));
		outputpathField.setBorder(BorderFactory.createLineBorder(Color.GRAY));
		outputpathPanel.add(outputpathField);
		mandatoryParameterPanel.add(outputpathPanel);

		// CompressionType Pane
		final JPanel compressionTypePanel = new JPanel();
		compressionTypePanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		final JLabel compressionTypeLabel = new JLabel(LangResources.get("compression"));
		compressionTypePanel.add(compressionTypeLabel);
		compressionTypeCombo = new JComboBox<>();
		compressionTypeCombo.setToolTipText(LangResources.get("compression_help"));
		compressionTypeCombo.setPreferredSize(new Dimension(200, compressionTypeCombo.getPreferredSize().height));
		compressionTypeCombo.addItem(LangResources.get("None"));
		compressionTypeCombo.addItem("Zip");
		compressionTypeCombo.addItem("TarGz");
		compressionTypeCombo.addItem("Tgz");
		compressionTypeCombo.addItem("Gz");
		compressionTypeCombo.addKeyListener(new KeyAdapter() {
			@Override
			public void keyReleased(final KeyEvent event) {
				checkButtonStatus();
			}
		});
		compressionTypePanel.add(compressionTypeCombo);
		mandatoryParameterPanel.add(compressionTypePanel);

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

		// kdbxPassword panel
		final JPanel kdbxPasswordPanel = new JPanel();
		kdbxPasswordPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		final JLabel kdbxPasswordLabel = new JLabel(LangResources.get("kdbxPassword"));
		kdbxPasswordPanel.add(kdbxPasswordLabel);
		kdbxPasswordField = new JPasswordField();
		kdbxPasswordField.setToolTipText(LangResources.get("kdbxPassword_help"));
		kdbxPasswordField.setPreferredSize(new Dimension(200, kdbxPasswordField.getPreferredSize().height));
		kdbxPasswordField.addKeyListener(new KeyAdapter() {
			@Override
			public void keyReleased(final KeyEvent event) {
				checkButtonStatus();
			}
		});
		kdbxPasswordPanel.add(kdbxPasswordField);
		mandatoryParameterPanel.add(kdbxPasswordPanel);

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

		// StringQuoteEscape Pane
		final JPanel stringQuoteEscapePanel = new JPanel();
		stringQuoteEscapePanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		final JLabel stringQuoteEscapeLabel = new JLabel(LangResources.get("stringquoteescape"));
		stringQuoteEscapePanel.add(stringQuoteEscapeLabel);
		stringQuoteEscapeCombo = new JComboBox<>();
		stringQuoteEscapeCombo.setToolTipText(LangResources.get("stringquoteescape_help"));
		stringQuoteEscapeCombo.setPreferredSize(new Dimension(200, stringQuoteEscapeCombo.getPreferredSize().height));
		stringQuoteEscapeCombo.addItem("\"");
		stringQuoteEscapeCombo.addItem("'");
		stringQuoteEscapeCombo.setEditable(true);
		stringQuoteEscapePanel.add(stringQuoteEscapeCombo);
		mandatoryParameterPanel.add(stringQuoteEscapePanel);

		// Indentation Pane
		final JPanel indentationPanel = new JPanel();
		indentationPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		final JLabel indentationLabel = new JLabel(LangResources.get("indentation"));
		indentationPanel.add(indentationLabel);
		indentationCombo = new JComboBox<>();
		indentationCombo.setToolTipText(LangResources.get("indentation_help"));
		indentationCombo.setPreferredSize(new Dimension(200, indentationCombo.getPreferredSize().height));
		indentationCombo.addItem("TAB");
		indentationCombo.addItem("BLANK");
		indentationCombo.addItem("DOUBLEBLANK");
		indentationCombo.setEditable(true);
		indentationPanel.add(indentationCombo);
		mandatoryParameterPanel.add(indentationPanel);

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
		nullValueStringCombo.setEditable(true);
		nullValueStringPanel.add(nullValueStringCombo);
		mandatoryParameterPanel.add(nullValueStringPanel);

		// Locale Panel
		final JPanel localePanel = new JPanel();
		localePanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		final JLabel localeLabel = new JLabel(LangResources.get("locale"));
		localePanel.add(localeLabel);
		localeCombo = new JComboBox<>();
		localeCombo.setToolTipText(LangResources.get("locale_help"));
		localeCombo.setPreferredSize(new Dimension(200, localeCombo.getPreferredSize().height));
		localeCombo.addItem("DE");
		localeCombo.addItem("EN");
		localeCombo.setEditable(true);
		localeCombo.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent e) {
				try {
					final Locale locale = new Locale((String) localeCombo.getSelectedItem());
					exportDateFormatField.setText(DateUtilities.getDateFormatPattern(locale));
					exportDateTimeFormatField.setText(DateUtilities.getDateTimeFormatWithSecondsPattern(locale));
				} catch (@SuppressWarnings("unused") final Exception e1) {
					exportDateFormatField.setText("");
					exportDateTimeFormatField.setText("");
				}
			}
		});
		localePanel.add(localeCombo);
		mandatoryParameterPanel.add(localePanel);

		// Export date format
		final JPanel exportDateFormatPanel = new JPanel();
		exportDateFormatPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		final JLabel exportDateFormatLabel = new JLabel(LangResources.get("exportDateFormat"));
		exportDateFormatPanel.add(exportDateFormatLabel);
		exportDateFormatField = new JTextField();
		exportDateFormatField.setToolTipText(LangResources.get("exportDateFormat_help"));
		exportDateFormatField.setPreferredSize(new Dimension(200, exportDateFormatField.getPreferredSize().height));
		exportDateFormatPanel.add(exportDateFormatField);
		mandatoryParameterPanel.add(exportDateFormatPanel);

		// Export datetime format
		final JPanel exportDateTimeFormatPanel = new JPanel();
		exportDateTimeFormatPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		final JLabel exportDateTimeFormatLabel = new JLabel(LangResources.get("exportDateTimeFormat"));
		exportDateTimeFormatPanel.add(exportDateTimeFormatLabel);
		exportDateTimeFormatField = new JTextField();
		exportDateTimeFormatField.setToolTipText(LangResources.get("exportDateTimeFormat_help"));
		exportDateTimeFormatField.setPreferredSize(new Dimension(200, exportDateTimeFormatField.getPreferredSize().height));
		exportDateTimeFormatPanel.add(exportDateTimeFormatField);
		mandatoryParameterPanel.add(exportDateTimeFormatPanel);

		// Statement Panel
		final JPanel statementPanel = new JPanel();
		statementPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		final JLabel statementLabel = new JLabel(LangResources.get("statement"));
		statementPanel.add(statementLabel);
		statementField = new JTextArea();
		statementField.setToolTipText(LangResources.get("statement_help"));
		final JScrollPane statementScrollpane = new JScrollPane(statementField);
		statementScrollpane.setPreferredSize(new Dimension(200, 100));
		statementPanel.add(statementScrollpane);
		mandatoryParameterPanel.add(statementPanel);

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

		// Export data timezone Panel
		final JPanel exportDataTimezonePanel = new JPanel();
		exportDataTimezonePanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		final JLabel exportDataTimezoneLabel = new JLabel(LangResources.get("exportDataTimezone"));
		exportDataTimezonePanel.add(exportDataTimezoneLabel);
		exportDataTimezoneCombo = new JComboBox<>();
		exportDataTimezoneCombo.setToolTipText(LangResources.get("exportDataTimezone_help"));
		exportDataTimezoneCombo.setPreferredSize(new Dimension(200, exportDataTimezoneCombo.getPreferredSize().height));
		for (final String exportDataTimezone : TimeZone.getAvailableIDs()) {
			exportDataTimezoneCombo.addItem(exportDataTimezone);
		}
		exportDataTimezoneCombo.setSelectedItem(TimeZone.getDefault().getID());
		exportDataTimezoneCombo.setEditable(false);
		exportDataTimezoneCombo.addItemListener(new ItemListener() {
			@Override
			public void itemStateChanged(final ItemEvent event) {
				checkButtonStatus();
			}
		});
		exportDataTimezonePanel.add(exportDataTimezoneCombo);
		mandatoryParameterPanel.add(exportDataTimezonePanel);

		// Optional parameters Panel
		final JPanel optionalParametersPanel = new JPanel();
		optionalParametersPanel.setLayout(new BoxLayout(optionalParametersPanel, BoxLayout.PAGE_AXIS));

		fileLogBox = new JCheckBox(LangResources.get("filelog"));
		fileLogBox.setToolTipText(LangResources.get("filelog_help"));
		optionalParametersPanel.add(fileLogBox);

		useZipCryptoBox = new JCheckBox(LangResources.get("useZipCrypto"));
		useZipCryptoBox.setToolTipText(LangResources.get("useZipCrypto_help"));
		optionalParametersPanel.add(useZipCryptoBox);

		alwaysQuoteBox = new JCheckBox(LangResources.get("alwaysquote"));
		alwaysQuoteBox.setToolTipText(LangResources.get("alwaysquote_help"));
		optionalParametersPanel.add(alwaysQuoteBox);

		blobfilesBox = new JCheckBox(LangResources.get("blobfiles"));
		blobfilesBox.setToolTipText(LangResources.get("blobfiles_help"));
		optionalParametersPanel.add(blobfilesBox);

		clobfilesBox = new JCheckBox(LangResources.get("clobfiles"));
		clobfilesBox.setToolTipText(LangResources.get("clobfiles_help"));
		optionalParametersPanel.add(clobfilesBox);

		beautifyBox = new JCheckBox(LangResources.get("beautify"));
		beautifyBox.setToolTipText(LangResources.get("beautify_help"));
		optionalParametersPanel.add(beautifyBox);

		noHeadersBox = new JCheckBox(LangResources.get("noheaders"));
		noHeadersBox.setToolTipText(LangResources.get("noheaders_help"));
		optionalParametersPanel.add(noHeadersBox);

		exportStructureBox = new JCheckBox(LangResources.get("exportstructure"));
		exportStructureBox.setToolTipText(LangResources.get("exportstructure_help"));
		optionalParametersPanel.add(exportStructureBox);

		createOutputDirectoyIfNotExistsBox = new JCheckBox(LangResources.get("createOutputDirectoyIfNotExists"));
		createOutputDirectoyIfNotExistsBox.setToolTipText(LangResources.get("createOutputDirectoyIfNotExists_help"));
		optionalParametersPanel.add(createOutputDirectoyIfNotExistsBox);

		replaceAlreadyExistingFilesBox = new JCheckBox(LangResources.get("replaceAlreadyExistingFiles"));
		replaceAlreadyExistingFilesBox.setToolTipText(LangResources.get("replaceAlreadyExistingFiles_help"));
		optionalParametersPanel.add(replaceAlreadyExistingFilesBox);

		// Button Panel
		final JPanel buttonPanel = new JPanel();
		buttonPanel.setLayout(new BoxLayout(buttonPanel, BoxLayout.LINE_AXIS));

		// Start Button
		final JButton startButton = new JButton(LangResources.get("export"));
		startButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent event) {
				try {
					export(getConfigurationAsDefinition(), dbExportGui);
				} catch (final Exception e) {
					new QuestionDialog(dbExportGui, DbExport.APPLICATION_NAME + " ERROR", "ERROR:\n" + e.getMessage()).setBackgroundColor(SwingColor.LightRed).open();
				}
			}
		});
		buttonPanel.add(startButton);

		buttonPanel.add(Box.createRigidArea(new Dimension(5, 0)));

		// Preferences Button
		final JButton preferencesButton = new JButton(LangResources.get("preferences"));
		preferencesButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent event) {
				try {
					final SecurePreferencesDialog credentialsDialog = new SecurePreferencesDialog(dbExportGui, DbExport.APPLICATION_NAME + " " + LangResources.get("preferences"),
							LangResources.get("preferences_text"), DbExport.SECURE_PREFERENCES_FILE, LangResources.get("load"), LangResources.get("create"), LangResources.get("update"),
							LangResources.get("delete"), LangResources.get("preferences_save"), LangResources.get("cancel"), LangResources.get("preferences_password_text"),
							LangResources.get("password"), LangResources.get("ok"), LangResources.get("cancel"));

					credentialsDialog.setCurrentDataEntry(getConfigurationAsDefinition());
					credentialsDialog.setPassword(temporaryPreferencesPassword);
					credentialsDialog.open();
					if (credentialsDialog.getCurrentDataEntry() != null) {
						setConfigurationByDefinition((DbExportDefinition) credentialsDialog.getCurrentDataEntry());
					}

					temporaryPreferencesPassword = credentialsDialog.getPassword();
				} catch (final Exception e) {
					temporaryPreferencesPassword = null;
					new QuestionDialog(dbExportGui, DbExport.APPLICATION_NAME + " ERROR", "ERROR:\n" + e.getMessage()).setBackgroundColor(SwingColor.LightRed).open();
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
					try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream("DbExport.ico")) {
						iconData = IoUtilities.toByteArray(inputStream);
					}

					Image iconImage;
					try (InputStream imageIconStream = getClass().getClassLoader().getResourceAsStream("DbExport_Icon.png")) {
						iconImage = ImageIO.read(getClass().getClassLoader().getResource("DbExport_Icon.png"));
					}

					final ApplicationConfigurationDialog applicationConfigurationDialog = new ApplicationConfigurationDialog(dbExportGui, DbExport.APPLICATION_NAME, DbExport.APPLICATION_STARTUPCLASS_NAME, DbExport.VERSION, DbExport.VERSION_BUILDTIME, applicationConfiguration, iconData, iconImage, DbExport.VERSIONINFO_DOWNLOAD_URL, DbExport.TRUSTED_UPDATE_CA_CERTIFICATES);
					final Result result = applicationConfigurationDialog.open();
					if (result != null && result == Result.OK) {
						applicationConfiguration.save();
					}
				} catch (final Exception e) {
					new QuestionDialog(dbExportGui, DbExport.APPLICATION_NAME + " ERROR", "ERROR:\n" + e.getMessage()).setBackgroundColor(SwingColor.LightRed).open();
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
		mandatoryParameterScrollPane.setPreferredSize(new Dimension(500, 400));
		mandatoryParameterScrollPane.getVerticalScrollBar().setUnitIncrement(8);
		parameterPanel.add(mandatoryParameterScrollPane);

		optionalParametersPanel.setPreferredSize(new Dimension(300, 400));
		parameterPanel.add(optionalParametersPanel);
		add(parameterPanel);
		add(Box.createRigidArea(new Dimension(0, 5)));
		add(buttonPanel);

		setConfigurationByDefinition(dbExportDefinition);

		checkButtonStatus();

		add(Box.createRigidArea(new Dimension(0, 5)));

		pack();

		setLocationRelativeTo(null);
		setResizable(false);
	}

	/**
	 * Gets the configuration as definition.
	 *
	 * @return the configuration as definition
	 * @throws Exception
	 *             the exception
	 */
	private DbExportDefinition getConfigurationAsDefinition() throws Exception {
		final DbExportDefinition dbExportDefinition = new DbExportDefinition();

		dbExportDefinition.setDbVendor((String) dbTypeCombo.getSelectedItem());
		dbExportDefinition.setHostnameAndPort(hostField.isEnabled() ? hostField.getText() : null);
		dbExportDefinition.setDbName(dbNameField.getText());
		dbExportDefinition.setUsername(userField.isEnabled() ? userField.getText() : null);
		dbExportDefinition.setPassword(passwordField.isEnabled() ? passwordField.getPassword() : null);
		dbExportDefinition.setOutputpath(outputpathField.getText());
		dbExportDefinition.setSqlStatementOrTablelist(statementField.getText());

		dbExportDefinition.setDataType((String) dataTypeCombo.getSelectedItem());

		dbExportDefinition.setLog(fileLogBox.isSelected());
		FileCompressionType fileCompressionType;
		try {
			fileCompressionType = FileCompressionType.getFromString((String) compressionTypeCombo.getSelectedItem());
		} catch (@SuppressWarnings("unused") final Exception e) {
			fileCompressionType = null;
		}
		dbExportDefinition.setCompression(fileCompressionType);
		// null stands for no usage of zip password, but GUI field text is always not null, so use empty field as deactivation of zip password
		dbExportDefinition.setZipPassword(Utilities.isEmpty(zipPasswordField.getPassword()) ? null : zipPasswordField.getPassword());
		dbExportDefinition.setKdbxPassword(Utilities.isEmpty(kdbxPasswordField.getPassword()) ? null : kdbxPasswordField.getPassword());
		dbExportDefinition.setAlwaysQuote(alwaysQuoteBox.isEnabled() ? alwaysQuoteBox.isSelected() : false);
		dbExportDefinition.setCreateBlobFiles(blobfilesBox.isSelected());
		dbExportDefinition.setCreateClobFiles(clobfilesBox.isSelected());
		dbExportDefinition.setBeautify(beautifyBox.isEnabled() ? beautifyBox.isSelected() : false);
		dbExportDefinition.setExportStructure(exportStructureBox.isSelected());
		dbExportDefinition.setNoHeaders(noHeadersBox.isEnabled() ? noHeadersBox.isSelected() : false);
		dbExportDefinition.setEncoding(Charset.forName((String) encodingCombo.getSelectedItem()));
		dbExportDefinition.setSeparator(((String) separatorCombo.getSelectedItem()).charAt(0));
		dbExportDefinition.setStringQuote(((String) stringQuoteCombo.getSelectedItem()).charAt(0));
		dbExportDefinition.setStringQuoteEscapeCharacter(((String) stringQuoteEscapeCombo.getSelectedItem()).charAt(0));
		String indentationString;
		if ("TAB".equalsIgnoreCase((String) indentationCombo.getSelectedItem())) {
			indentationString = "\t";
		} else if ("BLANK".equalsIgnoreCase((String) indentationCombo.getSelectedItem())) {
			indentationString = " ";
		} else if ("DOUBLEBLANK".equalsIgnoreCase((String) indentationCombo.getSelectedItem())) {
			indentationString = "  ";
		} else {
			indentationString = (String) indentationCombo.getSelectedItem();
		}
		dbExportDefinition.setIndentation(indentationString);
		final Locale locale = new Locale((String) localeCombo.getSelectedItem());
		dbExportDefinition.setDateFormatLocale(localeCombo.isEnabled() ? locale : null);

		if (Utilities.isNotBlank(exportDateFormatField.getText()) && exportDateFormatField.isEnabled()) {
			dbExportDefinition.setDateFormat(exportDateFormatField.getText());
		}

		if (Utilities.isNotBlank(exportDateTimeFormatField.getText()) && exportDateTimeFormatField.isEnabled()) {
			dbExportDefinition.setDateTimeFormat(exportDateTimeFormatField.getText());
		}

		dbExportDefinition.setNullValueString((String) nullValueStringCombo.getSelectedItem());

		dbExportDefinition.setDatabaseTimeZone((String) databaseTimezoneCombo.getSelectedItem());
		dbExportDefinition.setExportDataTimeZone((String) exportDataTimezoneCombo.getSelectedItem());

		dbExportDefinition.setCreateOutputDirectoyIfNotExists(createOutputDirectoyIfNotExistsBox.isSelected());
		dbExportDefinition.setReplaceAlreadyExistingFiles(replaceAlreadyExistingFilesBox.isSelected());

		return dbExportDefinition;
	}

	/**
	 * Sets the configuration by definition.
	 *
	 * @param dbExportDefinition
	 *            the new configuration by definition
	 * @throws Exception
	 *             the exception
	 */
	private void setConfigurationByDefinition(final DbExportDefinition dbExportDefinition) throws Exception {
		for (int i = 0; i < dbTypeCombo.getItemCount(); i++) {
			if (DbUtilities.DbVendor.getDbVendorByName(dbTypeCombo.getItemAt(i)) == dbExportDefinition.getDbVendor()) {
				dbTypeCombo.setSelectedIndex(i);
				break;
			}
		}

		hostField.setText(dbExportDefinition.getHostnameAndPort());
		dbNameField.setText(dbExportDefinition.getDbName());
		userField.setText(dbExportDefinition.getUsername());
		passwordField.setText(dbExportDefinition.getPassword() == null ? "" : new String(dbExportDefinition.getPassword()));
		outputpathField.setText(dbExportDefinition.getOutputpath());
		statementField.setText(dbExportDefinition.getSqlStatementOrTablelist());

		for (int i = 0; i < dataTypeCombo.getItemCount(); i++) {
			if (dataTypeCombo.getItemAt(i).equalsIgnoreCase(dbExportDefinition.getDataType().toString())) {
				dataTypeCombo.setSelectedIndex(i);
				break;
			}
		}

		fileLogBox.setSelected(dbExportDefinition.isLog());

		boolean compressionTypeFound = false;
		if (dbExportDefinition.getCompression() != null) {
			for (int i = 0; i < compressionTypeCombo.getItemCount(); i++) {
				if (compressionTypeCombo.getItemAt(i).equalsIgnoreCase(dbExportDefinition.getCompression().name())) {
					compressionTypeCombo.setSelectedIndex(i);
					compressionTypeFound = true;
					break;
				}
			}
		}
		if (!compressionTypeFound) {
			compressionTypeCombo.setSelectedIndex(0);
		}

		zipPasswordField.setText(dbExportDefinition.getZipPassword() == null ? "" : new String(dbExportDefinition.getZipPassword()));
		kdbxPasswordField.setText(dbExportDefinition.getKdbxPassword() == null ? "" : new String(dbExportDefinition.getKdbxPassword()));
		alwaysQuoteBox.setSelected(dbExportDefinition.isAlwaysQuote());
		blobfilesBox.setSelected(dbExportDefinition.isCreateBlobFiles());
		clobfilesBox.setSelected(dbExportDefinition.isCreateClobFiles());
		beautifyBox.setSelected(dbExportDefinition.isBeautify());
		exportStructureBox.setSelected(dbExportDefinition.isExportStructure());
		noHeadersBox.setSelected(dbExportDefinition.isNoHeaders());
		createOutputDirectoyIfNotExistsBox.setSelected(dbExportDefinition.isCreateOutputDirectoyIfNotExists());
		replaceAlreadyExistingFilesBox.setSelected(dbExportDefinition.isReplaceAlreadyExistingFiles());

		boolean encodingFound = false;
		for (int i = 0; i < encodingCombo.getItemCount(); i++) {
			if (encodingCombo.getItemAt(i).equalsIgnoreCase(dbExportDefinition.getEncoding().name())) {
				encodingCombo.setSelectedIndex(i);
				encodingFound = true;
				break;
			}
		}
		if (!encodingFound) {
			encodingCombo.setSelectedItem(dbExportDefinition.getEncoding());
		}

		boolean separatorFound = false;
		for (int i = 0; i < separatorCombo.getItemCount(); i++) {
			if (separatorCombo.getItemAt(i).equalsIgnoreCase(Character.toString(dbExportDefinition.getSeparator()))) {
				separatorCombo.setSelectedIndex(i);
				separatorFound = true;
				break;
			}
		}
		if (!separatorFound) {
			separatorCombo.setSelectedItem(Character.toString(dbExportDefinition.getSeparator()));
		}

		boolean stringQuoteFound = false;
		for (int i = 0; i < stringQuoteCombo.getItemCount(); i++) {
			if (stringQuoteCombo.getItemAt(i).equalsIgnoreCase(Character.toString(dbExportDefinition.getStringQuote()))) {
				stringQuoteCombo.setSelectedIndex(i);
				stringQuoteFound = true;
				break;
			}
		}
		if (!stringQuoteFound) {
			stringQuoteCombo.setSelectedItem(Character.toString(dbExportDefinition.getStringQuote()));
		}

		boolean stringQuoteEscapeFound = false;
		for (int i = 0; i < stringQuoteEscapeCombo.getItemCount(); i++) {
			if (stringQuoteEscapeCombo.getItemAt(i).equalsIgnoreCase(Character.toString(dbExportDefinition.getStringQuoteEscapeCharacter()))) {
				stringQuoteEscapeCombo.setSelectedIndex(i);
				stringQuoteEscapeFound = true;
				break;
			}
		}
		if (!stringQuoteEscapeFound) {
			stringQuoteEscapeCombo.setSelectedItem(Character.toString(dbExportDefinition.getStringQuoteEscapeCharacter()));
		}

		if ("\t".equals(dbExportDefinition.getIndentation())) {
			indentationCombo.setSelectedIndex(0);
		} else if (" ".equals(dbExportDefinition.getIndentation())) {
			indentationCombo.setSelectedIndex(1);
		} else if ("  ".equals(dbExportDefinition.getIndentation())) {
			indentationCombo.setSelectedIndex(2);
		} else {
			boolean indentationFound = false;
			for (int i = 0; i < indentationCombo.getItemCount(); i++) {
				if (indentationCombo.getItemAt(i).equalsIgnoreCase(dbExportDefinition.getIndentation())) {
					indentationCombo.setSelectedIndex(i);
					indentationFound = true;
					break;
				}
			}
			if (!indentationFound) {
				indentationCombo.setSelectedItem(dbExportDefinition.getIndentation());
			}
		}

		boolean foundLocale = false;
		for (int i = 0; i < localeCombo.getItemCount(); i++) {
			if (localeCombo.getItemAt(i).equalsIgnoreCase(dbExportDefinition.getDateFormatLocale().getLanguage())) {
				localeCombo.setSelectedIndex(i);
				foundLocale = true;
				break;
			}
		}
		if (!foundLocale) {
			localeCombo.setSelectedItem(dbExportDefinition.getDateFormatLocale().getLanguage());
		}

		exportDateFormatField.setText(dbExportDefinition.getDateFormat());
		exportDateTimeFormatField.setText(dbExportDefinition.getDateTimeFormat());

		if ("".equals(dbExportDefinition.getNullValueString())) {
			nullValueStringCombo.setSelectedIndex(0);
		} else if ("NULL".equals(dbExportDefinition.getNullValueString())) {
			nullValueStringCombo.setSelectedIndex(1);
		} else if ("Null".equals(dbExportDefinition.getNullValueString())) {
			nullValueStringCombo.setSelectedIndex(2);
		} else if ("null".equals(dbExportDefinition.getNullValueString())) {
			nullValueStringCombo.setSelectedIndex(3);
		} else {
			nullValueStringCombo.setSelectedItem(dbExportDefinition.getNullValueString());
		}

		databaseTimezoneCombo.setSelectedItem(dbExportDefinition.getDatabaseTimeZone());

		exportDataTimezoneCombo.setSelectedItem(dbExportDefinition.getExportDataTimeZone());

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

		switch (DataType.getFromString((String) dataTypeCombo.getSelectedItem())) {
			case JSON:
				separatorCombo.setEnabled(false);
				stringQuoteCombo.setEnabled(false);
				alwaysQuoteBox.setEnabled(false);
				noHeadersBox.setEnabled(false);
				beautifyBox.setEnabled(true);
				indentationCombo.setEnabled(true);
				nullValueStringCombo.setEnabled(false);
				kdbxPasswordField.setEnabled(false);
				localeCombo.setEnabled(false);
				break;
			case XML:
				separatorCombo.setEnabled(false);
				stringQuoteCombo.setEnabled(false);
				alwaysQuoteBox.setEnabled(false);
				noHeadersBox.setEnabled(false);
				beautifyBox.setEnabled(true);
				indentationCombo.setEnabled(true);
				nullValueStringCombo.setEnabled(true);
				kdbxPasswordField.setEnabled(false);
				localeCombo.setEnabled(true);
				break;
			case KDBX:
				separatorCombo.setEnabled(false);
				stringQuoteCombo.setEnabled(false);
				alwaysQuoteBox.setEnabled(false);
				noHeadersBox.setEnabled(false);
				beautifyBox.setEnabled(true);
				indentationCombo.setEnabled(true);
				nullValueStringCombo.setEnabled(true);
				kdbxPasswordField.setEnabled(true);
				localeCombo.setEnabled(true);
				break;
			case SQL:
				separatorCombo.setEnabled(false);
				stringQuoteCombo.setEnabled(false);
				alwaysQuoteBox.setEnabled(false);
				noHeadersBox.setEnabled(false);
				beautifyBox.setEnabled(false);
				indentationCombo.setEnabled(false);
				nullValueStringCombo.setEnabled(false);
				kdbxPasswordField.setEnabled(false);
				localeCombo.setEnabled(true);
				break;
			case VCF:
				separatorCombo.setEnabled(false);
				stringQuoteCombo.setEnabled(false);
				alwaysQuoteBox.setEnabled(false);
				noHeadersBox.setEnabled(false);
				beautifyBox.setEnabled(false);
				indentationCombo.setEnabled(false);
				nullValueStringCombo.setEnabled(false);
				kdbxPasswordField.setEnabled(false);
				localeCombo.setEnabled(true);
				break;
			case CSV:
			default:
				separatorCombo.setEnabled(true);
				stringQuoteCombo.setEnabled(true);
				alwaysQuoteBox.setEnabled(true);
				noHeadersBox.setEnabled(true);
				beautifyBox.setEnabled(true);
				indentationCombo.setEnabled(false);
				nullValueStringCombo.setEnabled(true);
				kdbxPasswordField.setEnabled(false);
				localeCombo.setEnabled(true);
				break;
		}

		if (DbVendor.SQLite.toString().equalsIgnoreCase((String) dbTypeCombo.getSelectedItem())) {
			localeCombo.setEnabled(false);
		}
	}

	/**
	 * Export.
	 *
	 * @param dbExportDefinition
	 *            the database csv export definition
	 * @param dbExportGui
	 *            the database csv export gui
	 */
	private void export(final DbExportDefinition dbExportDefinition, final DbExportGui dbExportGui) {
		try {
			dbExportDefinition.checkParameters();
			if (!new DbDriverSupplier(this, dbExportDefinition.getDbVendor()).supplyDriver(DbExport.APPLICATION_NAME, DbExport.CONFIGURATION_FILE)) {
				throw new Exception("Cannot aquire database driver for database vendor: " + dbExportDefinition.getDbVendor());
			}

			// The worker parent is set later by the opened DualProgressDialog
			final AbstractDbExportWorker worker = dbExportDefinition.getConfiguredWorker(null);

			final Result result;
			if (worker.isSingleExport()) {
				final ProgressDialog<AbstractDbExportWorker> progressDialog = new ProgressDialog<>(dbExportGui, DbExport.APPLICATION_NAME, null, worker);
				result = progressDialog.open();
			} else {
				final DualProgressDialog<AbstractDbExportWorker> progressDialog = new DualProgressDialog<>(dbExportGui, DbExport.APPLICATION_NAME, null, worker);
				result = progressDialog.open();
			}

			if (result == Result.CANCELED) {
				new QuestionDialog(dbExportGui, DbExport.APPLICATION_NAME, LangResources.get("error.canceledbyuser")).setBackgroundColor(SwingColor.Yellow).open();
			} else if (result == Result.ERROR) {
				final Exception e = worker.getError();
				if (e instanceof DbExportException) {
					new QuestionDialog(dbExportGui, DbExport.APPLICATION_NAME + " ERROR", "ERROR:\n" + ((DbExportException) e).getMessage()).setBackgroundColor(SwingColor.LightRed).open();
				} else {
					final String stacktrace = ExceptionUtilities.getStackTrace(e);
					new QuestionDialog(dbExportGui, DbExport.APPLICATION_NAME + " ERROR", "ERROR:\n" + e.getClass().getSimpleName() + ":\n" + e.getMessage() + "\n\n" + stacktrace).setBackgroundColor(SwingColor.LightRed).open();
				}
			} else {
				final LocalDateTime start = worker.getStartTime();
				final LocalDateTime end = worker.getEndTime();
				final long itemsDone = worker.getItemsDone();

				String resultText = LangResources.get("start") + ": " + DateUtilities.formatDate(DateUtilities.YYYY_MM_DD_HHMMSS, start) + "\n" + LangResources.get("end") + ": " + DateUtilities.formatDate(DateUtilities.YYYY_MM_DD_HHMMSS, end) + "\n" + LangResources.get("timeelapsed") + ": "
						+ DateUtilities.getHumanReadableTimespan(Duration.between(start, end), true);

				if (dbExportDefinition.getSqlStatementOrTablelist().toLowerCase().startsWith("select ")
						|| dbExportDefinition.getSqlStatementOrTablelist().toLowerCase().startsWith("select\t")
						|| dbExportDefinition.getSqlStatementOrTablelist().toLowerCase().startsWith("select\n")
						|| dbExportDefinition.getSqlStatementOrTablelist().toLowerCase().startsWith("select\r")) {
					resultText += "\n" + LangResources.get("exported") + " 1 select";
				} else {
					resultText += "\n" + LangResources.get("exportedtables") + ": " + Utilities.getHumanReadableInteger(itemsDone, null, Locale.getDefault());
				}

				resultText += "\n" + LangResources.get("exportedlines") + ": " + Utilities.getHumanReadableInteger((long) worker.getOverallExportedLines(), null, Locale.getDefault());

				if ("console".equalsIgnoreCase(dbExportDefinition.getOutputpath())) {
					new QuestionDialog(dbExportGui, DbExport.APPLICATION_NAME, LangResources.get("result") + ":\n" + resultText).open();
				} else if ("gui".equalsIgnoreCase(dbExportDefinition.getOutputpath())) {
					resultText = new String(worker.getGuiOutputStream().toByteArray(), StandardCharsets.UTF_8) + "\n" + resultText;
					new QuestionDialog(dbExportGui, DbExport.APPLICATION_NAME, resultText).open();
				} else {
					resultText += "\n" + LangResources.get("exporteddataamount") + ": " + Utilities.getHumanReadableNumber(worker.getOverallExportedDataAmountRaw(), "Byte", false, 5, false, Locale.getDefault());
					if (dbExportDefinition.getCompression() != null
							|| Utilities.endsWithIgnoreCase(dbExportDefinition.getOutputpath(), ".zip")
							|| Utilities.endsWithIgnoreCase(dbExportDefinition.getOutputpath(), ".tar.gz")
							|| Utilities.endsWithIgnoreCase(dbExportDefinition.getOutputpath(), ".tgz")
							|| Utilities.endsWithIgnoreCase(dbExportDefinition.getOutputpath(), ".gz")) {
						resultText += "\n" + LangResources.get("exporteddataamountcompressed") + ": " + Utilities.getHumanReadableNumber(worker.getOverallExportedDataAmountCompressed(), "Byte", false, 5, false, Locale.getDefault());
					}
					resultText += "\n" + LangResources.get("exportSpeed") + ": " + Utilities.getHumanReadableSpeed(worker.getStartTime(), worker.getEndTime(), worker.getOverallExportedDataAmountRaw() * 8, "Bit", true, Locale.getDefault());
					new QuestionDialog(dbExportGui, DbExport.APPLICATION_NAME, LangResources.get("result") + ":\n" + resultText).open();
				}
			}
		} catch (final Exception e) {
			new QuestionDialog(dbExportGui, DbExport.APPLICATION_NAME + " ERROR", "ERROR:\n" + e.getMessage()).setBackgroundColor(SwingColor.LightRed).open();
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

	private File selectFile(String basePath, final String text) {
		if (Utilities.isBlank(basePath)) {
			basePath = System.getProperty("user.home");
		} else if (basePath.contains(File.separator)) {
			basePath = basePath.substring(0, basePath.lastIndexOf(File.separator));
		}

		final JFileChooser fileChooser = new JFileChooser(basePath);
		fileChooser.setDialogTitle(DbExport.APPLICATION_NAME + " " + text);
		if (JFileChooser.APPROVE_OPTION == fileChooser.showOpenDialog(this)) {
			return fileChooser.getSelectedFile();
		} else {
			return null;
		}
	}
}
