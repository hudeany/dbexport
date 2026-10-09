package de.soderer.dbexport;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Image;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

import javax.imageio.ImageIO;
import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JCheckBox;
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
import de.soderer.network.NetworkUtilities;
import de.soderer.network.trustmanager.TrustManagerUtilities;
import de.soderer.pac.PacScriptParser;
import de.soderer.pac.utilities.ProxyConfiguration.ProxyConfigurationType;
import de.soderer.utilities.ConfigurationProperties;
import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.ExceptionUtilities;
import de.soderer.utilities.FileCompressionType;
import de.soderer.utilities.IoUtilities;
import de.soderer.utilities.LangResources;
import de.soderer.utilities.Result;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.VersionInfo;
import de.soderer.utilities.appupdate.ApplicationUpdateUtilities;
import de.soderer.utilities.db.DbUtilities;
import de.soderer.utilities.db.data.DbVendor;
import de.soderer.utilities.swing.ApplicationConfigurationDialog;
import de.soderer.utilities.swing.DropDown;
import de.soderer.utilities.swing.DualProgressDialog;
import de.soderer.utilities.swing.ProgressDialog;
import de.soderer.utilities.swing.QuestionDialog;
import de.soderer.utilities.swing.SecurePreferencesDialog;
import de.soderer.utilities.swing.SwingColor;
import de.soderer.utilities.swing.UpdateableGuiApplication;

/**
 * The GUI for DbExport.
 *
 * @serial exclude
 */
public class DbExportGui extends UpdateableGuiApplication {
	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = 5969613637206441880L;

	/**
	 * KeyStore file of the application in the user's configuration directory.
	 */
	public static final File KEYSTORE_FILE = new File(System.getProperty("user.home") + File.separator + "." + DbExport.APPLICATION_NAME + File.separator + "." + DbExport.APPLICATION_NAME + ".keystore");

	private final ConfigurationProperties applicationConfiguration;

	/** The database type combo. */
	private final DropDown dbTypeCombo;

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
	private final DropDown dataTypeCombo;

	/** The file log box. */
	private final JCheckBox fileLogBox;

	/** The outputpath field. */
	private final JTextField outputpathField;

	/** The zipPassword field. */
	private final JPasswordField zipPasswordField;

	/** The kdbxPassword field. */
	private final JPasswordField kdbxPasswordField;

	/** The separator combo. */
	private final DropDown separatorCombo;

	/** The string quote combo. */
	private final DropDown stringQuoteCombo;

	/** The string quote escape character combo. */
	private final DropDown stringQuoteEscapeCombo;

	/** The indentation combo. */
	private final DropDown indentationCombo;

	/** The null value string combo. */
	private final DropDown nullValueStringCombo;

	/** The encoding combo. */
	private final DropDown encodingCombo;

	/** The locale combo. */
	private final DropDown localeCombo;

	/** The statement field. */
	private final JTextArea statementField;

	/** The compressionType combo. */
	private final DropDown compressionTypeCombo;

	/** The useZipCrypto box. */
	private final JCheckBox useZipCryptoBox;

	/** The blobfiles box. */
	private final JCheckBox blobfilesBox;

	/** The clobfiles box. */
	private final JCheckBox clobfilesBox;

	/** The always quote box. */
	private final JCheckBox alwaysQuoteBox;

	/** The checkbox to toggle usage of escape sequences (e.g. \n, \t) when writing csv string values. */
	private final JCheckBox interpretEscapeSequencesBox;

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
	private final DropDown databaseTimezoneCombo;

	/** The field for datafiles timezone */
	private final DropDown exportDataTimezoneCombo;

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
		if ("de".equalsIgnoreCase(applicationConfiguration.get(ConfigurationProperties.CONFIG_KEY_LANGUAGE))) {
			Locale.setDefault(Locale.GERMAN);
		} else {
			Locale.setDefault(Locale.ENGLISH);
		}

		if (!applicationConfiguration.containsKey(ConfigurationProperties.CONFIG_KEY_PROXY_CONFIGURATION_TYPE)) {
			if (PacScriptParser.findPacFileUrlByWpad() != null) {
				applicationConfiguration.set(ConfigurationProperties.CONFIG_KEY_PROXY_CONFIGURATION_TYPE, ProxyConfigurationType.WPAD.name());
			} else {
				applicationConfiguration.set(ConfigurationProperties.CONFIG_KEY_PROXY_CONFIGURATION_TYPE, ProxyConfigurationType.None.name());
			}
			applicationConfiguration.save();
		}

		if (dailyUpdateCheckIsPending()) {
			setDailyUpdateCheckStatus(true);
			try {
				if (ApplicationUpdateUtilities.checkForNewVersionAvailable(DbExport.VERSIONINFO_DOWNLOAD_URL, applicationConfiguration.getProxyConfiguration(), DbExport.APPLICATION_NAME, VersionInfo.getApplicationVersion()) != null) {
					final List<String> appParameters = new ArrayList<>();
					appParameters.add("gui");
					ApplicationUpdateUtilities.executeUpdate(this, DbExport.VERSIONINFO_DOWNLOAD_URL, applicationConfiguration.getProxyConfiguration(), DbExport.APPLICATION_NAME, DbExport.VERSION, DbExport.TRUSTED_UPDATE_CA_CERTIFICATES, null, null, appParameters, true, false);
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
		dbTypeCombo = new DropDown();
		dbTypeCombo.setToolTipText(LangResources.get("dbtype_help"));
		// Fixed list: no custom values (like the former non-editable combo box)
		dbTypeCombo.setCaseSensitive(false);
		dbTypeCombo.setMatchMode(DropDown.MatchMode.CONTAINS);
		dbTypeCombo.setAllowCustomValues(false);
		for (final DbVendor dbVendor : DbVendor.values()) {
			dbTypeCombo.addItem(dbVendor.toString());
		}
		// The former combo box preselected the first item automatically, DropDown does not
		dbTypeCombo.setText(dbTypeCombo.getItems()[0]);
		dbTypeCombo.addActionListener(event -> checkButtonStatus());
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
							TrustManagerUtilities.createTrustStoreFile(hostField.getText(), DbVendor.getDbVendorByName(getRequiredSelectedItem(dbTypeCombo, "dbtype")).getDefaultPort(), new File(trustStoreFilePathField.getText()), trustStorePasswordField.getPassword(), null);
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
		dataTypeCombo = new DropDown();
		dataTypeCombo.setToolTipText(LangResources.get("datatype_help"));
		dataTypeCombo.setPreferredSize(new Dimension(200, dataTypeCombo.getPreferredSize().height));
		// Fixed list: no custom values (like the former non-editable combo box)
		dataTypeCombo.setCaseSensitive(false);
		dataTypeCombo.setMatchMode(DropDown.MatchMode.STARTS_WITH);
		dataTypeCombo.setAllowCustomValues(false);
		for (final DataType dataType : DataType.values()) {
			dataTypeCombo.addItem(dataType.toString());
		}
		dataTypeCombo.setText(dataTypeCombo.getItems()[0]);
		// Only on accepted values, so the beautify checkbox is not toggled with every keystroke.
		// Anonymous class instead of a lambda: a lambda must not read the blank final field
		// "beautifyBox" before it is assigned later in this constructor, an anonymous class may.
		dataTypeCombo.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent event) {
				final String selectedDataType = getSelectedItem(dataTypeCombo);
				beautifyBox.setSelected("JSON".equalsIgnoreCase(selectedDataType) || "XML".equalsIgnoreCase(selectedDataType));
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
		compressionTypeCombo = new DropDown();
		compressionTypeCombo.setToolTipText(LangResources.get("compression_help"));
		compressionTypeCombo.setPreferredSize(new Dimension(200, compressionTypeCombo.getPreferredSize().height));
		// Fixed list: no custom values (like the former non-editable combo box)
		compressionTypeCombo.setCaseSensitive(false);
		compressionTypeCombo.setMatchMode(DropDown.MatchMode.STARTS_WITH);
		compressionTypeCombo.setAllowCustomValues(false);
		compressionTypeCombo.addItem(LangResources.get("None"));
		compressionTypeCombo.addItem("Zip");
		compressionTypeCombo.addItem("TarGz");
		compressionTypeCombo.addItem("Tgz");
		compressionTypeCombo.addItem("Gz");
		compressionTypeCombo.setText(compressionTypeCombo.getItems()[0]);
		compressionTypeCombo.addActionListener(event -> checkButtonStatus());
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
		encodingCombo = new DropDown();
		encodingCombo.setToolTipText(LangResources.get("encoding_help"));
		encodingCombo.setPreferredSize(new Dimension(200, encodingCombo.getPreferredSize().height));
		encodingCombo.addItem(StandardCharsets.UTF_8.name());
		encodingCombo.addItem(StandardCharsets.ISO_8859_1.name());
		encodingCombo.addItem("ISO-8859-15");
		// Editable like the former combo box: presets, but any other value may be typed in
		encodingCombo.setCaseSensitive(false);
		encodingCombo.setMatchMode(DropDown.MatchMode.CONTAINS);
		encodingCombo.setAllowCustomValues(true);
		encodingCombo.setText(StandardCharsets.UTF_8.name());
		encodingPanel.add(encodingCombo);
		mandatoryParameterPanel.add(encodingPanel);

		// Separator Pane
		final JPanel separatorPanel = new JPanel();
		separatorPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		final JLabel separatorLabel = new JLabel(LangResources.get("separator"));
		separatorPanel.add(separatorLabel);
		separatorCombo = new DropDown();
		separatorCombo.setToolTipText(LangResources.get("separator_help"));
		separatorCombo.setPreferredSize(new Dimension(200, separatorCombo.getPreferredSize().height));
		separatorCombo.addItem(";");
		separatorCombo.addItem(",");
		// Editable like the former combo box: presets, but any other value may be typed in
		separatorCombo.setAllowCustomValues(true);
		separatorCombo.setText(";");
		separatorPanel.add(separatorCombo);
		mandatoryParameterPanel.add(separatorPanel);

		// StringQuote Pane
		final JPanel stringQuotePanel = new JPanel();
		stringQuotePanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		final JLabel stringQuoteLabel = new JLabel(LangResources.get("stringquote"));
		stringQuotePanel.add(stringQuoteLabel);
		stringQuoteCombo = new DropDown();
		stringQuoteCombo.setToolTipText(LangResources.get("stringquote_help"));
		stringQuoteCombo.setPreferredSize(new Dimension(200, stringQuoteCombo.getPreferredSize().height));
		stringQuoteCombo.addItem("\"");
		stringQuoteCombo.addItem("'");
		// Editable like the former combo box: presets, but any other value may be typed in
		stringQuoteCombo.setAllowCustomValues(true);
		stringQuoteCombo.setText("\"");
		stringQuotePanel.add(stringQuoteCombo);
		mandatoryParameterPanel.add(stringQuotePanel);

		// StringQuoteEscape Pane
		final JPanel stringQuoteEscapePanel = new JPanel();
		stringQuoteEscapePanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		final JLabel stringQuoteEscapeLabel = new JLabel(LangResources.get("stringquoteescape"));
		stringQuoteEscapePanel.add(stringQuoteEscapeLabel);
		stringQuoteEscapeCombo = new DropDown();
		stringQuoteEscapeCombo.setToolTipText(LangResources.get("stringquoteescape_help"));
		stringQuoteEscapeCombo.setPreferredSize(new Dimension(200, stringQuoteEscapeCombo.getPreferredSize().height));
		stringQuoteEscapeCombo.addItem("\"");
		stringQuoteEscapeCombo.addItem("'");
		// Editable like the former combo box: presets, but any other value may be typed in
		stringQuoteEscapeCombo.setAllowCustomValues(true);
		stringQuoteEscapeCombo.setText("\"");
		stringQuoteEscapePanel.add(stringQuoteEscapeCombo);
		mandatoryParameterPanel.add(stringQuoteEscapePanel);

		// Indentation Pane
		final JPanel indentationPanel = new JPanel();
		indentationPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		final JLabel indentationLabel = new JLabel(LangResources.get("indentation"));
		indentationPanel.add(indentationLabel);
		indentationCombo = new DropDown();
		indentationCombo.setToolTipText(LangResources.get("indentation_help"));
		indentationCombo.setPreferredSize(new Dimension(200, indentationCombo.getPreferredSize().height));
		indentationCombo.addItem("TAB");
		indentationCombo.addItem("BLANK");
		indentationCombo.addItem("DOUBLEBLANK");
		// Editable like the former combo box: presets, but any other value may be typed in
		indentationCombo.setCaseSensitive(false);
		indentationCombo.setMatchMode(DropDown.MatchMode.STARTS_WITH);
		indentationCombo.setAllowCustomValues(true);
		indentationCombo.setText("TAB");
		indentationPanel.add(indentationCombo);
		mandatoryParameterPanel.add(indentationPanel);

		// NullValueString Pane
		final JPanel nullValueStringPanel = new JPanel();
		nullValueStringPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		final JLabel nullValueStringLabel = new JLabel(LangResources.get("nullvaluetext"));
		nullValueStringPanel.add(nullValueStringLabel);
		nullValueStringCombo = new DropDown();
		nullValueStringCombo.setToolTipText(LangResources.get("nullvaluetext_help"));
		nullValueStringCombo.setPreferredSize(new Dimension(200, nullValueStringCombo.getPreferredSize().height));
		nullValueStringCombo.addItem("");
		nullValueStringCombo.addItem("NULL");
		nullValueStringCombo.addItem("Null");
		nullValueStringCombo.addItem("null");
		// Editable like the former combo box: presets, but any other value may be typed in
		// Case-sensitive, because "NULL", "Null" and "null" are different values here
		nullValueStringCombo.setCaseSensitive(true);
		nullValueStringCombo.setMatchMode(DropDown.MatchMode.STARTS_WITH);
		nullValueStringCombo.setAllowCustomValues(true);
		nullValueStringCombo.setText("");
		nullValueStringPanel.add(nullValueStringCombo);
		mandatoryParameterPanel.add(nullValueStringPanel);

		// Locale Panel
		final JPanel localePanel = new JPanel();
		localePanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		final JLabel localeLabel = new JLabel(LangResources.get("locale"));
		localePanel.add(localeLabel);
		localeCombo = new DropDown();
		localeCombo.setToolTipText(LangResources.get("locale_help"));
		localeCombo.setPreferredSize(new Dimension(200, localeCombo.getPreferredSize().height));
		localeCombo.addItem("DE");
		localeCombo.addItem("EN");
		// Editable like the former combo box: presets, but any other value may be typed in
		localeCombo.setCaseSensitive(false);
		localeCombo.setMatchMode(DropDown.MatchMode.STARTS_WITH);
		localeCombo.setAllowCustomValues(true);
		localeCombo.setText("DE");
		localeCombo.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent e) {
				try {
					final Locale locale = Locale.forLanguageTag(localeCombo.getText());
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
		databaseTimezoneCombo = new DropDown();
		databaseTimezoneCombo.setToolTipText(LangResources.get("databaseTimezone_help"));
		databaseTimezoneCombo.setPreferredSize(new Dimension(200, databaseTimezoneCombo.getPreferredSize().height));
		// Fixed list: no custom values (like the former non-editable combo box)
		databaseTimezoneCombo.setCaseSensitive(false);
		databaseTimezoneCombo.setMatchMode(DropDown.MatchMode.CONTAINS);
		databaseTimezoneCombo.setAllowCustomValues(false);
		for (final String databaseTimezone : TimeZone.getAvailableIDs()) {
			databaseTimezoneCombo.addItem(databaseTimezone);
		}
		databaseTimezoneCombo.setText(TimeZone.getDefault().getID());
		databaseTimezoneCombo.addActionListener(event -> checkButtonStatus());
		databaseTimezonePanel.add(databaseTimezoneCombo);
		mandatoryParameterPanel.add(databaseTimezonePanel);

		// Export data timezone Panel
		final JPanel exportDataTimezonePanel = new JPanel();
		exportDataTimezonePanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		final JLabel exportDataTimezoneLabel = new JLabel(LangResources.get("exportDataTimezone"));
		exportDataTimezonePanel.add(exportDataTimezoneLabel);
		exportDataTimezoneCombo = new DropDown();
		exportDataTimezoneCombo.setToolTipText(LangResources.get("exportDataTimezone_help"));
		exportDataTimezoneCombo.setPreferredSize(new Dimension(200, exportDataTimezoneCombo.getPreferredSize().height));
		// Fixed list: no custom values (like the former non-editable combo box)
		exportDataTimezoneCombo.setCaseSensitive(false);
		exportDataTimezoneCombo.setMatchMode(DropDown.MatchMode.CONTAINS);
		exportDataTimezoneCombo.setAllowCustomValues(false);
		for (final String exportDataTimezone : TimeZone.getAvailableIDs()) {
			exportDataTimezoneCombo.addItem(exportDataTimezone);
		}
		exportDataTimezoneCombo.setText(TimeZone.getDefault().getID());
		exportDataTimezoneCombo.addActionListener(event -> checkButtonStatus());
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

		interpretEscapeSequencesBox = new JCheckBox(LangResources.get("interpretEscapeSequences"));
		interpretEscapeSequencesBox.setToolTipText(LangResources.get("interpretEscapeSequences_help"));
		interpretEscapeSequencesBox.setSelected(true);
		optionalParametersPanel.add(interpretEscapeSequencesBox);

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

					final List<String> appParameters = new ArrayList<>();
					appParameters.add("gui");
					final ApplicationConfigurationDialog applicationConfigurationDialog = new ApplicationConfigurationDialog(dbExportGui, DbExport.APPLICATION_NAME, DbExport.APPLICATION_STARTUPCLASS_NAME, DbExport.VERSION, DbExport.VERSION_BUILDTIME, applicationConfiguration, iconData, iconImage, DbExport.VERSIONINFO_DOWNLOAD_URL, DbExport.TRUSTED_UPDATE_CA_CERTIFICATES, appParameters);
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

		dbExportDefinition.setDbVendor(getRequiredSelectedItem(dbTypeCombo, "dbtype"));
		dbExportDefinition.setHostnameAndPort(hostField.isEnabled() ? hostField.getText() : null);
		dbExportDefinition.setDbName(dbNameField.getText());
		dbExportDefinition.setUsername(userField.isEnabled() ? userField.getText() : null);
		dbExportDefinition.setPassword(passwordField.isEnabled() ? passwordField.getPassword() : null);
		// The secure connection settings were missing, so the GUI always used an unencrypted connection
		dbExportDefinition.setSecureConnection(secureConnectionBox.isEnabled() && secureConnectionBox.isSelected());
		dbExportDefinition.setTrustStoreFile(trustStoreFilePathField.isEnabled() && Utilities.isNotBlank(trustStoreFilePathField.getText()) ? new File(trustStoreFilePathField.getText()) : null);
		dbExportDefinition.setTrustStorePassword(trustStorePasswordField.isEnabled() && trustStorePasswordField.getPassword().length > 0 ? trustStorePasswordField.getPassword() : null);
		dbExportDefinition.setOutputpath(outputpathField.getText());
		dbExportDefinition.setSqlStatementOrTablelist(statementField.getText());

		dbExportDefinition.setDataType(getRequiredSelectedItem(dataTypeCombo, "datatype"));

		dbExportDefinition.setLog(fileLogBox.isSelected());
		FileCompressionType fileCompressionType;
		try {
			fileCompressionType = FileCompressionType.getFromString(getSelectedItem(compressionTypeCombo));
		} catch (@SuppressWarnings("unused") final Exception e) {
			fileCompressionType = null;
		}
		dbExportDefinition.setCompression(fileCompressionType);
		// null stands for no usage of zip password, but GUI field text is always not null, so use empty field as deactivation of zip password
		dbExportDefinition.setZipPassword(Utilities.isEmpty(zipPasswordField.getPassword()) ? null : zipPasswordField.getPassword());
		dbExportDefinition.setKdbxPassword(Utilities.isEmpty(kdbxPasswordField.getPassword()) ? null : kdbxPasswordField.getPassword());
		dbExportDefinition.setAlwaysQuote(alwaysQuoteBox.isEnabled() ? alwaysQuoteBox.isSelected() : false);
		dbExportDefinition.setInterpretEscapeSequences(interpretEscapeSequencesBox.isEnabled() ? interpretEscapeSequencesBox.isSelected() : true);
		dbExportDefinition.setCreateBlobFiles(blobfilesBox.isSelected());
		dbExportDefinition.setCreateClobFiles(clobfilesBox.isSelected());
		dbExportDefinition.setBeautify(beautifyBox.isEnabled() ? beautifyBox.isSelected() : false);
		// Only the file name, its directory is derived from the output path (see DbExportDefinition.getExportStructureFilePath()).
		// Before, it was always created within the output path, which failed for an output file.
		final String exportStructureFilePath = exportStructureBox.isSelected() ? ("dbstructure_" + DateUtilities.formatDate("yyyy-MM-dd_HH-mm-ss", LocalDateTime.now()) + ".json") : null;
		dbExportDefinition.setExportStructureFilePath(exportStructureFilePath);
		dbExportDefinition.setNoHeaders(noHeadersBox.isEnabled() ? noHeadersBox.isSelected() : false);
		dbExportDefinition.setEncoding(Charset.forName(encodingCombo.getText()));
		dbExportDefinition.setSeparator(separatorCombo.getText().charAt(0));
		dbExportDefinition.setStringQuote(stringQuoteCombo.getText().charAt(0));
		dbExportDefinition.setStringQuoteEscapeCharacter(stringQuoteEscapeCombo.getText().charAt(0));
		String indentationString;
		if ("TAB".equalsIgnoreCase(indentationCombo.getText())) {
			indentationString = "\t";
		} else if ("BLANK".equalsIgnoreCase(indentationCombo.getText())) {
			indentationString = " ";
		} else if ("DOUBLEBLANK".equalsIgnoreCase(indentationCombo.getText())) {
			indentationString = "  ";
		} else {
			indentationString = indentationCombo.getText();
		}
		dbExportDefinition.setIndentation(indentationString);
		final Locale locale = Locale.forLanguageTag(localeCombo.getText());
		dbExportDefinition.setDateFormatLocale(localeCombo.isEnabled() ? locale : null);

		if (Utilities.isNotBlank(exportDateFormatField.getText()) && exportDateFormatField.isEnabled()) {
			dbExportDefinition.setDateFormat(exportDateFormatField.getText());
		}

		if (Utilities.isNotBlank(exportDateTimeFormatField.getText()) && exportDateTimeFormatField.isEnabled()) {
			dbExportDefinition.setDateTimeFormat(exportDateTimeFormatField.getText());
		}

		dbExportDefinition.setNullValueString(nullValueStringCombo.getText());

		dbExportDefinition.setDatabaseTimeZone(getRequiredSelectedItem(databaseTimezoneCombo, "databaseTimezone"));
		dbExportDefinition.setExportDataTimeZone(getRequiredSelectedItem(exportDataTimezoneCombo, "exportDataTimezone"));

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
		for (final String dbTypeItem : dbTypeCombo.getItems()) {
			if (DbVendor.getDbVendorByName(dbTypeItem) == dbExportDefinition.getDbVendor()) {
				dbTypeCombo.setText(dbTypeItem);
				break;
			}
		}

		hostField.setText(dbExportDefinition.getHostnameAndPort());
		dbNameField.setText(dbExportDefinition.getDbName());
		userField.setText(dbExportDefinition.getUsername());
		passwordField.setText(dbExportDefinition.getPassword() == null ? "" : new String(dbExportDefinition.getPassword()));
		secureConnectionBox.setSelected(dbExportDefinition.isSecureConnection());
		trustStoreFilePathField.setText(dbExportDefinition.getTrustStoreFile() == null ? "" : dbExportDefinition.getTrustStoreFile().getAbsolutePath());
		trustStorePasswordField.setText(dbExportDefinition.getTrustStorePassword() == null ? "" : new String(dbExportDefinition.getTrustStorePassword()));
		outputpathField.setText(dbExportDefinition.getOutputpath());
		statementField.setText(dbExportDefinition.getSqlStatementOrTablelist());

		selectItem(dataTypeCombo, dbExportDefinition.getDataType().toString());

		fileLogBox.setSelected(dbExportDefinition.isLog());

		if (dbExportDefinition.getCompression() == null || !selectItem(compressionTypeCombo, dbExportDefinition.getCompression().name())) {
			compressionTypeCombo.setText(compressionTypeCombo.getItems()[0]);
		}

		zipPasswordField.setText(dbExportDefinition.getZipPassword() == null ? "" : new String(dbExportDefinition.getZipPassword()));
		kdbxPasswordField.setText(dbExportDefinition.getKdbxPassword() == null ? "" : new String(dbExportDefinition.getKdbxPassword()));
		alwaysQuoteBox.setSelected(dbExportDefinition.isAlwaysQuote());
		interpretEscapeSequencesBox.setSelected(dbExportDefinition.isInterpretEscapeSequences());
		blobfilesBox.setSelected(dbExportDefinition.isCreateBlobFiles());
		clobfilesBox.setSelected(dbExportDefinition.isCreateClobFiles());
		beautifyBox.setSelected(dbExportDefinition.isBeautify());
		exportStructureBox.setSelected(dbExportDefinition.getExportStructureFilePath() != null);
		noHeadersBox.setSelected(dbExportDefinition.isNoHeaders());
		createOutputDirectoyIfNotExistsBox.setSelected(dbExportDefinition.isCreateOutputDirectoyIfNotExists());
		replaceAlreadyExistingFilesBox.setSelected(dbExportDefinition.isReplaceAlreadyExistingFiles());

		selectItem(encodingCombo, dbExportDefinition.getEncoding().name());

		selectItem(separatorCombo, Character.toString(dbExportDefinition.getSeparator()));

		selectItem(stringQuoteCombo, Character.toString(dbExportDefinition.getStringQuote()));

		selectItem(stringQuoteEscapeCombo, Character.toString(dbExportDefinition.getStringQuoteEscapeCharacter()));

		if ("\t".equals(dbExportDefinition.getIndentation())) {
			indentationCombo.setText("TAB");
		} else if (" ".equals(dbExportDefinition.getIndentation())) {
			indentationCombo.setText("BLANK");
		} else if ("  ".equals(dbExportDefinition.getIndentation())) {
			indentationCombo.setText("DOUBLEBLANK");
		} else {
			selectItem(indentationCombo, dbExportDefinition.getIndentation());
		}

		selectItem(localeCombo, dbExportDefinition.getDateFormatLocale().getLanguage());

		exportDateFormatField.setText(dbExportDefinition.getDateFormat());
		exportDateTimeFormatField.setText(dbExportDefinition.getDateTimeFormat());

		// selectItem() prefers the exact match, so "NULL", "Null" and "null" stay distinct
		selectItem(nullValueStringCombo, dbExportDefinition.getNullValueString());

		selectItem(databaseTimezoneCombo, dbExportDefinition.getDatabaseTimeZone());

		selectItem(exportDataTimezoneCombo, dbExportDefinition.getExportDataTimeZone());

		checkButtonStatus();
	}

	/**
	 * Returns the item matching the current text of the DropDown, preferring an exact match over a
	 * case-insensitive one. Returns null if the text is invalid or only a partial input that was not
	 * accepted yet (DropDown.getText() may return such a partial text while it is still valid).
	 */
	private static String getSelectedItem(final DropDown dropDown) {
		final String text = dropDown.getText();
		if (text == null) {
			return null;
		}
		for (final String item : dropDown.getItems()) {
			if (item.equals(text)) {
				return item;
			}
		}
		for (final String item : dropDown.getItems()) {
			if (item.equalsIgnoreCase(text)) {
				return item;
			}
		}
		return null;
	}

	/**
	 * Like {@link #getSelectedItem(DropDown)}, but throws a descriptive exception instead of returning null.
	 * Used for the fixed-list DropDowns, whose value must be one of the items.
	 */
	private static String getRequiredSelectedItem(final DropDown dropDown, final String fieldLabelKey) throws Exception {
		final String selectedItem = getSelectedItem(dropDown);
		if (selectedItem == null) {
			throw new Exception("Invalid value for '" + LangResources.get(fieldLabelKey) + "': '" + (dropDown.getText() == null ? "" : dropDown.getText()) + "'");
		}
		return selectedItem;
	}

	/**
	 * Shows the item matching the given value (exact match preferred, then case-insensitive).
	 * If there is no such item, the value itself is shown, but only if the DropDown allows custom values.
	 *
	 * @return true if a matching item was found
	 */
	private static boolean selectItem(final DropDown dropDown, final String value) {
		final String valueToSelect = value == null ? "" : value;
		for (final String item : dropDown.getItems()) {
			if (item.equals(valueToSelect)) {
				dropDown.setText(item);
				return true;
			}
		}
		for (final String item : dropDown.getItems()) {
			if (item.equalsIgnoreCase(valueToSelect)) {
				dropDown.setText(item);
				return true;
			}
		}
		if (dropDown.isAllowCustomValues()) {
			dropDown.setText(valueToSelect);
		}
		return false;
	}

	/**
	 * Check button status.
	 */
	private void checkButtonStatus() {
		// null while the typed text is no (complete) valid entry
		final String selectedDbType = getSelectedItem(dbTypeCombo);
		final String selectedDataType = getSelectedItem(dataTypeCombo);

		if (DbVendor.SQLite.toString().equalsIgnoreCase(selectedDbType)
				|| DbVendor.Derby.toString().equalsIgnoreCase(selectedDbType)) {
			hostField.setEnabled(false);
			userField.setEnabled(false);
			passwordField.setEnabled(false);
			// File based databases have no secure connection (the checkbox kept the state of the previous vendor)
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
					DbVendor.Oracle.toString().equalsIgnoreCase(selectedDbType)
					|| DbVendor.MySQL.toString().equalsIgnoreCase(selectedDbType)
					|| DbVendor.MariaDB.toString().equalsIgnoreCase(selectedDbType)
					|| DbVendor.MsSQL.toString().equalsIgnoreCase(selectedDbType));
			trustStoreFilePathField.setEnabled(secureConnectionBox.isEnabled() && secureConnectionBox.isSelected());
			trustStoreFileButton.setEnabled(secureConnectionBox.isEnabled() && secureConnectionBox.isSelected());
			createTrustStoreFileButton.setEnabled(DbVendor.Oracle.toString().equalsIgnoreCase(selectedDbType)
					&& secureConnectionBox.isEnabled() && secureConnectionBox.isSelected() && Utilities.isNotBlank(hostField.getText()));
			trustStorePasswordField.setEnabled(secureConnectionBox.isEnabled() && secureConnectionBox.isSelected() && Utilities.isNotBlank(trustStoreFilePathField.getText()));

			final boolean isCassandra = DbVendor.Cassandra.toString().equalsIgnoreCase(selectedDbType);
			final boolean credentialsOk = isCassandra
					? (Utilities.isBlank(userField.getText()) || Utilities.isNotBlank(passwordField.getPassword()))
					: (Utilities.isNotBlank(userField.getText()) && Utilities.isNotBlank(passwordField.getPassword()));

			connectionCheckButton.setEnabled(
					Utilities.isNotBlank(dbNameField.getText())
					&& Utilities.isNotBlank(hostField.getText())
					&& credentialsOk);
		}

		// Incomplete data type input falls back to the CSV (default) field states
		switch (selectedDataType == null ? DataType.CSV : DataType.getFromString(selectedDataType)) {
			case JSON:
				separatorCombo.setEnabled(false);
				stringQuoteCombo.setEnabled(false);
				alwaysQuoteBox.setEnabled(false);
				interpretEscapeSequencesBox.setEnabled(false);
				noHeadersBox.setEnabled(false);
				beautifyBox.setEnabled(true);
				indentationCombo.setEnabled(true);
				nullValueStringCombo.setEnabled(false);
				kdbxPasswordField.setEnabled(false);
				localeCombo.setEnabled(false);
				break;
			case YAML:
				separatorCombo.setEnabled(false);
				stringQuoteCombo.setEnabled(false);
				alwaysQuoteBox.setEnabled(false);
				interpretEscapeSequencesBox.setEnabled(false);
				noHeadersBox.setEnabled(false);
				beautifyBox.setEnabled(false);
				indentationCombo.setEnabled(false);
				nullValueStringCombo.setEnabled(false);
				kdbxPasswordField.setEnabled(false);
				localeCombo.setEnabled(false);
				break;
			case XML:
				separatorCombo.setEnabled(false);
				stringQuoteCombo.setEnabled(false);
				alwaysQuoteBox.setEnabled(false);
				interpretEscapeSequencesBox.setEnabled(false);
				noHeadersBox.setEnabled(false);
				beautifyBox.setEnabled(true);
				indentationCombo.setEnabled(true);
				nullValueStringCombo.setEnabled(true);
				kdbxPasswordField.setEnabled(false);
				localeCombo.setEnabled(true);
				break;
			case KDBX:
				// KDBX uses none of the formatting settings, beautify was even rejected by DbExportDefinition.checkParameters()
				separatorCombo.setEnabled(false);
				stringQuoteCombo.setEnabled(false);
				alwaysQuoteBox.setEnabled(false);
				interpretEscapeSequencesBox.setEnabled(false);
				noHeadersBox.setEnabled(false);
				beautifyBox.setEnabled(false);
				indentationCombo.setEnabled(false);
				nullValueStringCombo.setEnabled(false);
				kdbxPasswordField.setEnabled(true);
				localeCombo.setEnabled(false);
				break;
			case SQL:
				separatorCombo.setEnabled(false);
				stringQuoteCombo.setEnabled(false);
				alwaysQuoteBox.setEnabled(false);
				interpretEscapeSequencesBox.setEnabled(false);
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
				interpretEscapeSequencesBox.setEnabled(false);
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
				interpretEscapeSequencesBox.setEnabled(true);
				noHeadersBox.setEnabled(true);
				beautifyBox.setEnabled(true);
				indentationCombo.setEnabled(false);
				nullValueStringCombo.setEnabled(true);
				kdbxPasswordField.setEnabled(false);
				localeCombo.setEnabled(true);
				break;
		}

		if (DbVendor.SQLite.toString().equalsIgnoreCase(selectedDbType)) {
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
					new QuestionDialog(dbExportGui, DbExport.APPLICATION_NAME + " ERROR", "ERROR:\n" + e.getMessage()).setBackgroundColor(SwingColor.LightRed).open();
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

	/**
	 * Sets up the default values of the application configuration.
	 *
	 * @param applicationConfiguration the application configuration
	 */
	public static void setupDefaultConfig(final ConfigurationProperties applicationConfiguration) {
		applicationConfiguration.setupDefaultConfig();
	}

	/**
	 * Activates or deactivates the daily update check and schedules the next check for tomorrow.
	 *
	 * @param checkboxStatus true to activate the daily update check
	 */
	@Override
	protected void setDailyUpdateCheckStatus(final boolean checkboxStatus) {
		applicationConfiguration.set(ConfigurationProperties.CONFIG_KEY_DAILY_UPDATE_CHECK, checkboxStatus);
		applicationConfiguration.set(ConfigurationProperties.CONFIG_KEY_NEXT_DAILY_UPDATE_CHECK, LocalDateTime.now().plusDays(1));
		applicationConfiguration.save();
	}

	/**
	 * Returns whether the daily update check is activated.
	 *
	 * @return true if the daily update check is activated
	 */
	@Override
	protected Boolean isDailyUpdateCheckActivated() {
		return applicationConfiguration.getBoolean(ConfigurationProperties.CONFIG_KEY_DAILY_UPDATE_CHECK);
	}

	/**
	 * Returns whether the daily update check is activated, due and a network connection is available.
	 *
	 * @return true if the daily update check should be executed now
	 */
	protected boolean dailyUpdateCheckIsPending() {
		return applicationConfiguration.getBoolean(ConfigurationProperties.CONFIG_KEY_DAILY_UPDATE_CHECK)
				&& (applicationConfiguration.getDate(ConfigurationProperties.CONFIG_KEY_NEXT_DAILY_UPDATE_CHECK) == null || applicationConfiguration.getDate(ConfigurationProperties.CONFIG_KEY_NEXT_DAILY_UPDATE_CHECK).isBefore(LocalDateTime.now()))
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
