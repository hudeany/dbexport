package de.soderer.dbcsvexport;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.util.Date;
import java.util.Locale;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;

import de.soderer.dbcsvexport.DbCsvExportDefinition.ExportType;
import de.soderer.dbcsvexport.worker.AbstractDbExportWorker;
import de.soderer.utilities.ApplicationUpdateHelper;
import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.DbUtilities;
import de.soderer.utilities.DbUtilities.DbVendor;
import de.soderer.utilities.ExceptionUtilities;
import de.soderer.utilities.LangResources;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.Version;
import de.soderer.utilities.swing.BasicUpdateableGuiApplication;
import de.soderer.utilities.swing.DualProgressDialog;
import de.soderer.utilities.swing.SecurePreferencesDialog;
import de.soderer.utilities.swing.SimpleProgressDialog.Result;
import de.soderer.utilities.swing.TextDialog;

/**
 * The GUI for DbCsvExport.
 */
public class DbCsvExportGui extends BasicUpdateableGuiApplication {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = 5969613637206441880L;

	/** The db type combo. */
	private JComboBox<String> dbTypeCombo;

	/** The host field. */
	private JTextField hostField;

	/** The db name field. */
	private JTextField dbNameField;

	/** The user field. */
	private JTextField userField;

	/** The password field. */
	private JTextField passwordField;

	/** The export type combo. */
	private JComboBox<String> exportTypeCombo;

	/** The file log box. */
	private JCheckBox fileLogBox;

	/** The outputpath field. */
	private JTextField outputpathField;

	/** The separator combo. */
	private JComboBox<String> separatorCombo;

	/** The string quote combo. */
	private JComboBox<String> stringQuoteCombo;

	/** The indentation combo. */
	private JComboBox<String> indentationCombo;

	/** The null value string combo. */
	private JComboBox<String> nullValueStringCombo;

	/** The encoding combo. */
	private JComboBox<String> encodingCombo;

	/** The locale combo. */
	private JComboBox<String> localeCombo;

	/** The statement field. */
	private JTextArea statementField;

	/** The zip box. */
	private JCheckBox zipBox;

	/** The blobfiles box. */
	private JCheckBox blobfilesBox;

	/** The clobfiles box. */
	private JCheckBox clobfilesBox;

	/** The always quote box. */
	private JCheckBox alwaysQuoteBox;

	/** The beautify box. */
	private JCheckBox beautifyBox;

	/** The export structure box. */
	private JCheckBox exportStructureBox;

	/** The no headers box. */
	private JCheckBox noHeadersBox;

	/** The temporary preferences password. */
	private char[] temporaryPreferencesPassword = null;

	/**
	 * Instantiates a new db csv export gui.
	 *
	 * @param dbCsvExportDefinition
	 *            the db csv export definition
	 * @throws Exception
	 *             the exception
	 */
	public DbCsvExportGui(DbCsvExportDefinition dbCsvExportDefinition) throws Exception {
		super(DbCsvExport.APPLICATION_NAME, new Version(DbCsvExport.VERSION));

		final DbCsvExportGui dbCsvExportGui = this;

		setLocationRelativeTo(null);

		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		setLayout(new BoxLayout(getContentPane(), BoxLayout.PAGE_AXIS));

		// Parameter Panel
		JPanel parameterPanel = new JPanel();
		parameterPanel.setLayout(new BoxLayout(parameterPanel, BoxLayout.LINE_AXIS));

		// Mandatory parameter Panel
		JPanel mandatoryParameterPanel = new JPanel();
		mandatoryParameterPanel.setLayout(new BoxLayout(mandatoryParameterPanel, BoxLayout.PAGE_AXIS));

		// DBType Pane
		JPanel dbTypePanel = new JPanel();
		dbTypePanel.setLayout(new FlowLayout());
		JLabel dbTypeLabel = new JLabel(LangResources.get("dbtype"));
		dbTypePanel.add(dbTypeLabel);
		dbTypeCombo = new JComboBox<String>();
		dbTypeCombo.setToolTipText(LangResources.get("dbtype_help"));
		for (DbVendor dbVendor : DbVendor.values()) {
			dbTypeCombo.addItem(dbVendor.toString());
		}
		dbTypeCombo.addItemListener(new ItemListener() {
			@Override
			public void itemStateChanged(ItemEvent e) {
				checkButtonStatus();
			}
		});
		dbTypePanel.add(dbTypeCombo, BorderLayout.EAST);
		mandatoryParameterPanel.add(dbTypePanel);

		// Host Panel
		JPanel hostPanel = new JPanel();
		hostPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		JLabel hostLabel = new JLabel(LangResources.get("host"));
		hostPanel.add(hostLabel);
		hostField = new JTextField();
		hostField.setToolTipText(LangResources.get("host_help"));
		hostField.setPreferredSize(new Dimension(200, hostField.getPreferredSize().height));
		hostPanel.add(hostField);
		mandatoryParameterPanel.add(hostPanel);

		// DB name Panel
		JPanel dbNamePanel = new JPanel();
		dbNamePanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		JLabel dbNameLabel = new JLabel(LangResources.get("dbname"));
		dbNamePanel.add(dbNameLabel);
		dbNameField = new JTextField();
		dbNameField.setToolTipText(LangResources.get("dbname_help"));
		dbNameField.setPreferredSize(new Dimension(200, dbNameField.getPreferredSize().height));
		dbNamePanel.add(dbNameField);
		mandatoryParameterPanel.add(dbNamePanel);

		// User Panel
		JPanel userPanel = new JPanel();
		userPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		JLabel userLabel = new JLabel(LangResources.get("user"));
		userPanel.add(userLabel);
		userField = new JTextField();
		userField.setToolTipText(LangResources.get("user_help"));
		userField.setPreferredSize(new Dimension(200, userField.getPreferredSize().height));
		userPanel.add(userField);
		mandatoryParameterPanel.add(userPanel);

		// Password Panel
		JPanel passwordPanel = new JPanel();
		passwordPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		JLabel passwordLabel = new JLabel(LangResources.get("password"));
		passwordPanel.add(passwordLabel);
		passwordField = new JPasswordField();
		passwordField.setToolTipText(LangResources.get("password_help"));
		passwordField.setPreferredSize(new Dimension(200, passwordField.getPreferredSize().height));
		passwordPanel.add(passwordField);
		mandatoryParameterPanel.add(passwordPanel);

		// Export type Pane
		JPanel exportTypePanel = new JPanel();
		exportTypePanel.setLayout(new FlowLayout());
		JLabel exportTypeLabel = new JLabel(LangResources.get("exporttype"));
		exportTypePanel.add(exportTypeLabel);
		exportTypeCombo = new JComboBox<String>();
		exportTypeCombo.setToolTipText(LangResources.get("exporttype_help"));
		for (ExportType exportType : ExportType.values()) {
			exportTypeCombo.addItem(exportType.toString());
		}
		exportTypeCombo.addItemListener(new ItemListener() {
			@Override
			public void itemStateChanged(ItemEvent e) {
				if (((String) exportTypeCombo.getSelectedItem()).equalsIgnoreCase("JSON") || ((String) exportTypeCombo.getSelectedItem()).equalsIgnoreCase("XML")) {
					beautifyBox.setSelected(true);
				}
				checkButtonStatus();
			}
		});
		exportTypePanel.add(exportTypeCombo, BorderLayout.EAST);
		mandatoryParameterPanel.add(exportTypePanel);

		// Outputpath Panel
		JPanel outputpathPanel = new JPanel();
		outputpathPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		JLabel outputpathLabel = new JLabel(LangResources.get("outputpath"));
		outputpathPanel.add(outputpathLabel);
		outputpathField = new JTextField();
		outputpathField.setToolTipText(LangResources.get("outputpath_help"));
		outputpathField.setPreferredSize(new Dimension(200, outputpathField.getPreferredSize().height));
		outputpathField.setBorder(BorderFactory.createLineBorder(Color.GRAY));
		outputpathPanel.add(outputpathField);
		mandatoryParameterPanel.add(outputpathPanel);

		// Encoding Pane
		JPanel encodingPanel = new JPanel();
		encodingPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		JLabel encodingLabel = new JLabel(LangResources.get("encoding"));
		encodingPanel.add(encodingLabel);
		encodingCombo = new JComboBox<String>();
		encodingCombo.setToolTipText(LangResources.get("encoding_help"));
		encodingCombo.setPreferredSize(new Dimension(200, encodingCombo.getPreferredSize().height));
		encodingCombo.addItem("UTF-8");
		encodingCombo.addItem("ISO-8859-1");
		encodingCombo.addItem("ISO-8859-15");
		encodingCombo.setEditable(true);
		encodingPanel.add(encodingCombo);
		mandatoryParameterPanel.add(encodingPanel);

		// Separator Pane
		JPanel separatorPanel = new JPanel();
		separatorPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		JLabel separatorLabel = new JLabel(LangResources.get("separator"));
		separatorPanel.add(separatorLabel);
		separatorCombo = new JComboBox<String>();
		separatorCombo.setToolTipText(LangResources.get("separator_help"));
		separatorCombo.setPreferredSize(new Dimension(200, separatorCombo.getPreferredSize().height));
		separatorCombo.addItem(";");
		separatorCombo.addItem(",");
		separatorCombo.setEditable(true);
		separatorPanel.add(separatorCombo);
		mandatoryParameterPanel.add(separatorPanel);

		// StringQuote Pane
		JPanel stringQuotePanel = new JPanel();
		stringQuotePanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		JLabel stringQuoteLabel = new JLabel(LangResources.get("stringquote"));
		stringQuotePanel.add(stringQuoteLabel);
		stringQuoteCombo = new JComboBox<String>();
		stringQuoteCombo.setToolTipText(LangResources.get("stringquote_help"));
		stringQuoteCombo.setPreferredSize(new Dimension(200, stringQuoteCombo.getPreferredSize().height));
		stringQuoteCombo.addItem("\"");
		stringQuoteCombo.addItem("'");
		stringQuoteCombo.setEditable(true);
		stringQuotePanel.add(stringQuoteCombo);
		mandatoryParameterPanel.add(stringQuotePanel);

		// Indentation Pane
		JPanel indentationPanel = new JPanel();
		indentationPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		JLabel indentationLabel = new JLabel(LangResources.get("indentation"));
		indentationPanel.add(indentationLabel);
		indentationCombo = new JComboBox<String>();
		indentationCombo.setToolTipText(LangResources.get("indentation_help"));
		indentationCombo.setPreferredSize(new Dimension(200, indentationCombo.getPreferredSize().height));
		indentationCombo.addItem("TAB");
		indentationCombo.addItem("BLANK");
		indentationCombo.addItem("DOUBLEBLANK");
		indentationCombo.setEditable(true);
		indentationPanel.add(indentationCombo);
		mandatoryParameterPanel.add(indentationPanel);

		// NullValueString Pane
		JPanel nullValueStringPanel = new JPanel();
		nullValueStringPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		JLabel nullValueStringLabel = new JLabel(LangResources.get("nullvaluetext"));
		nullValueStringPanel.add(nullValueStringLabel);
		nullValueStringCombo = new JComboBox<String>();
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
		JPanel localePanel = new JPanel();
		localePanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		JLabel localeLabel = new JLabel(LangResources.get("locale"));
		localePanel.add(localeLabel);
		localeCombo = new JComboBox<String>();
		localeCombo.setToolTipText(LangResources.get("locale_help"));
		localeCombo.setPreferredSize(new Dimension(200, localeCombo.getPreferredSize().height));
		localeCombo.addItem("DE");
		localeCombo.addItem("EN");
		localeCombo.setEditable(true);
		localePanel.add(localeCombo);
		mandatoryParameterPanel.add(localePanel);

		// Statement Panel
		JPanel statementPanel = new JPanel();
		statementPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		JLabel statementLabel = new JLabel(LangResources.get("statement"));
		statementPanel.add(statementLabel);
		statementField = new JTextArea();
		statementField.setToolTipText(LangResources.get("statement_help"));
		JScrollPane statementScrollpane = new JScrollPane(statementField);
		statementScrollpane.setPreferredSize(new Dimension(200, 100));
		statementPanel.add(statementScrollpane);
		mandatoryParameterPanel.add(statementPanel);

		// Optional parameters Panel
		JPanel optionalParametersPanel = new JPanel();
		optionalParametersPanel.setLayout(new BoxLayout(optionalParametersPanel, BoxLayout.PAGE_AXIS));

		fileLogBox = new JCheckBox(LangResources.get("filelog"));
		fileLogBox.setToolTipText(LangResources.get("filelog_help"));
		optionalParametersPanel.add(fileLogBox);

		zipBox = new JCheckBox(LangResources.get("zip"));
		zipBox.setToolTipText(LangResources.get("zip_help"));
		optionalParametersPanel.add(zipBox);

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

		// Button Panel
		JPanel buttonPanel = new JPanel();
		buttonPanel.setLayout(new BoxLayout(buttonPanel, BoxLayout.LINE_AXIS));

		// Start Button
		JButton startButton = new JButton(LangResources.get("export"));
		startButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent event) {
				try {
					export(getConfigurationAsDefinition(), dbCsvExportGui);
				} catch (Exception e) {
					new TextDialog(dbCsvExportGui, DbCsvExport.APPLICATION_NAME + " ERROR", "ERROR:\n" + e.getMessage(), LangResources.get("close"), false, Color.RED).setVisible(true);
				}
			}
		});
		buttonPanel.add(startButton);

		buttonPanel.add(Box.createRigidArea(new Dimension(5, 0)));

		// Preferences Button
		JButton preferencesButton = new JButton(LangResources.get("preferences"));
		preferencesButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent event) {
				try {
					SecurePreferencesDialog credentialsDialog = new SecurePreferencesDialog(dbCsvExportGui, DbCsvExport.APPLICATION_NAME + " " + LangResources.get("preferences"),
							LangResources.get("preferences_text"), DbCsvExport.SECURE_PREFERENCES_FILE, LangResources.get("load"), LangResources.get("create"), LangResources.get("update"),
							LangResources.get("delete"), LangResources.get("preferences_save"), LangResources.get("cancel"), LangResources.get("preferences_password_text"),
							LangResources.get("username"), LangResources.get("password"), LangResources.get("ok"), LangResources.get("cancel"));

					credentialsDialog.setCurrentSecureDataEntry(getConfigurationAsDefinition());
					credentialsDialog.setPassword(temporaryPreferencesPassword);
					credentialsDialog.showDialog();
					if (credentialsDialog.getCurrentSecureDataEntry() != null) {
						setConfigurationByDefinition((DbCsvExportDefinition) credentialsDialog.getCurrentSecureDataEntry());
					}

					temporaryPreferencesPassword = credentialsDialog.getPassword();
				} catch (Exception e) {
					temporaryPreferencesPassword = null;
					TextDialog textDialog = new TextDialog(dbCsvExportGui, DbCsvExport.APPLICATION_NAME + " ERROR", "ERROR:\n" + e.getMessage(), LangResources.get("close"), false, Color.PINK);
					textDialog.setVisible(true);
				}
			}
		});
		buttonPanel.add(preferencesButton);

		buttonPanel.add(Box.createRigidArea(new Dimension(5, 0)));

		// Update Button
		JButton updateButton = new JButton(LangResources.get("checkupdate"));
		updateButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent event) {
				try {
					new ApplicationUpdateHelper(DbCsvExport.APPLICATION_NAME, DbCsvExport.VERSION, DbCsvExport.VERSIONINFO_DOWNLOAD_URL, dbCsvExportGui, "-gui").executeUpdate();
				} catch (Exception e) {
					TextDialog textDialog = new TextDialog(dbCsvExportGui, DbCsvExport.APPLICATION_NAME + " ERROR", "ERROR:\n" + e.getMessage(), LangResources.get("close"), false, Color.PINK);
					textDialog.setVisible(true);
				}
			}
		});
		buttonPanel.add(updateButton);

		buttonPanel.add(Box.createRigidArea(new Dimension(5, 0)));

		// Close Button
		JButton closeButton = new JButton(LangResources.get("close"));
		closeButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent event) {
				dispose();
			}
		});
		buttonPanel.add(closeButton);

		parameterPanel.add(mandatoryParameterPanel);
		parameterPanel.add(optionalParametersPanel);
		add(parameterPanel);
		add(buttonPanel);

		setConfigurationByDefinition(dbCsvExportDefinition);

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
	private DbCsvExportDefinition getConfigurationAsDefinition() throws Exception {
		DbCsvExportDefinition dbCsvExportDefinition = new DbCsvExportDefinition();

		dbCsvExportDefinition.setDbVendor((String) dbTypeCombo.getSelectedItem());
		dbCsvExportDefinition.setHostname(hostField.isEnabled() ? hostField.getText() : null);
		dbCsvExportDefinition.setDbName(dbNameField.getText());
		dbCsvExportDefinition.setUsername(userField.isEnabled() ? userField.getText() : null);
		dbCsvExportDefinition.setPassword(passwordField.isEnabled() ? passwordField.getText() : null);
		dbCsvExportDefinition.setOutputpath(outputpathField.getText());
		dbCsvExportDefinition.setSqlStatementOrTablelist(statementField.getText());

		dbCsvExportDefinition.setExportType((String) exportTypeCombo.getSelectedItem());

		dbCsvExportDefinition.setLog(fileLogBox.isSelected());
		dbCsvExportDefinition.setZip(zipBox.isSelected());
		dbCsvExportDefinition.setAlwaysQuote(alwaysQuoteBox.isEnabled() ? alwaysQuoteBox.isSelected() : false);
		dbCsvExportDefinition.setCreateBlobFiles(blobfilesBox.isSelected());
		dbCsvExportDefinition.setCreateClobFiles(clobfilesBox.isSelected());
		dbCsvExportDefinition.setBeautify(beautifyBox.isEnabled() ? beautifyBox.isSelected() : false);
		dbCsvExportDefinition.setExportStructure(exportStructureBox.isSelected());
		dbCsvExportDefinition.setNoHeaders(noHeadersBox.isEnabled() ? noHeadersBox.isSelected() : false);
		dbCsvExportDefinition.setEncoding((String) encodingCombo.getSelectedItem());
		dbCsvExportDefinition.setSeparator(((String) separatorCombo.getSelectedItem()).charAt(0));
		dbCsvExportDefinition.setStringQuote(((String) stringQuoteCombo.getSelectedItem()).charAt(0));
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
		dbCsvExportDefinition.setIndentation(indentationString);
		dbCsvExportDefinition.setDateAndDecimalLocale(localeCombo.isEnabled() ? new Locale((String) localeCombo.getSelectedItem()) : null);

		dbCsvExportDefinition.setNullValueString((String) nullValueStringCombo.getSelectedItem());

		return dbCsvExportDefinition;
	}

	/**
	 * Sets the configuration by definition.
	 *
	 * @param dbCsvExportDefinition
	 *            the new configuration by definition
	 * @throws Exception
	 *             the exception
	 */
	private void setConfigurationByDefinition(DbCsvExportDefinition dbCsvExportDefinition) throws Exception {
		for (int i = 0; i < dbTypeCombo.getItemCount(); i++) {
			if (DbUtilities.DbVendor.getDbVendorByName(dbTypeCombo.getItemAt(i)) == dbCsvExportDefinition.getDbVendor()) {
				dbTypeCombo.setSelectedIndex(i);
				break;
			}
		}

		hostField.setText(dbCsvExportDefinition.getHostname());
		dbNameField.setText(dbCsvExportDefinition.getDbName());
		userField.setText(dbCsvExportDefinition.getUsername());
		passwordField.setText(dbCsvExportDefinition.getPassword());
		outputpathField.setText(dbCsvExportDefinition.getOutputpath());
		statementField.setText(dbCsvExportDefinition.getSqlStatementOrTablelist());

		for (int i = 0; i < exportTypeCombo.getItemCount(); i++) {
			if (exportTypeCombo.getItemAt(i).equalsIgnoreCase(dbCsvExportDefinition.getExportType().toString())) {
				exportTypeCombo.setSelectedIndex(i);
				break;
			}
		}

		fileLogBox.setSelected(dbCsvExportDefinition.isLog());
		zipBox.setSelected(dbCsvExportDefinition.isZip());
		alwaysQuoteBox.setSelected(dbCsvExportDefinition.isAlwaysQuote());
		blobfilesBox.setSelected(dbCsvExportDefinition.isCreateBlobFiles());
		clobfilesBox.setSelected(dbCsvExportDefinition.isCreateClobFiles());
		beautifyBox.setSelected(dbCsvExportDefinition.isBeautify());
		exportStructureBox.setSelected(dbCsvExportDefinition.isExportStructure());
		noHeadersBox.setSelected(dbCsvExportDefinition.isNoHeaders());

		boolean encodingFound = false;
		for (int i = 0; i < encodingCombo.getItemCount(); i++) {
			if (encodingCombo.getItemAt(i).equalsIgnoreCase(dbCsvExportDefinition.getEncoding())) {
				encodingCombo.setSelectedIndex(i);
				encodingFound = true;
				break;
			}
		}
		if (!encodingFound) {
			encodingCombo.setSelectedItem(dbCsvExportDefinition.getEncoding());
		}

		boolean separatorFound = false;
		for (int i = 0; i < separatorCombo.getItemCount(); i++) {
			if (separatorCombo.getItemAt(i).equalsIgnoreCase(Character.toString(dbCsvExportDefinition.getSeparator()))) {
				separatorCombo.setSelectedIndex(i);
				separatorFound = true;
				break;
			}
		}
		if (!separatorFound) {
			separatorCombo.setSelectedItem(Character.toString(dbCsvExportDefinition.getSeparator()));
		}

		boolean stringQuoteFound = false;
		for (int i = 0; i < stringQuoteCombo.getItemCount(); i++) {
			if (stringQuoteCombo.getItemAt(i).equalsIgnoreCase(Character.toString(dbCsvExportDefinition.getStringQuote()))) {
				stringQuoteCombo.setSelectedIndex(i);
				stringQuoteFound = true;
				break;
			}
		}
		if (!stringQuoteFound) {
			stringQuoteCombo.setSelectedItem(Character.toString(dbCsvExportDefinition.getStringQuote()));
		}

		if (dbCsvExportDefinition.getIndentation().equals("\t")) {
			indentationCombo.setSelectedIndex(0);
		} else if (dbCsvExportDefinition.getIndentation().equals(" ")) {
			indentationCombo.setSelectedIndex(1);
		} else if (dbCsvExportDefinition.getIndentation().equals("  ")) {
			indentationCombo.setSelectedIndex(2);
		} else {
			boolean indentationFound = false;
			for (int i = 0; i < indentationCombo.getItemCount(); i++) {
				if (indentationCombo.getItemAt(i).equalsIgnoreCase(dbCsvExportDefinition.getIndentation())) {
					indentationCombo.setSelectedIndex(i);
					indentationFound = true;
					break;
				}
			}
			if (!indentationFound) {
				indentationCombo.setSelectedItem(dbCsvExportDefinition.getIndentation());
			}
		}

		boolean foundLocale = false;
		for (int i = 0; i < localeCombo.getItemCount(); i++) {
			if (localeCombo.getItemAt(i).equalsIgnoreCase(dbCsvExportDefinition.getDateAndDecimalLocale().getLanguage())) {
				localeCombo.setSelectedIndex(i);
				foundLocale = true;
				break;
			}
		}
		if (!foundLocale) {
			localeCombo.setSelectedItem(dbCsvExportDefinition.getDateAndDecimalLocale().getLanguage());
		}

		if (dbCsvExportDefinition.getNullValueString().equals("")) {
			nullValueStringCombo.setSelectedIndex(0);
		} else if (dbCsvExportDefinition.getNullValueString().equals("NULL")) {
			nullValueStringCombo.setSelectedIndex(1);
		} else if (dbCsvExportDefinition.getNullValueString().equals("Null")) {
			nullValueStringCombo.setSelectedIndex(2);
		} else if (dbCsvExportDefinition.getNullValueString().equals("null")) {
			nullValueStringCombo.setSelectedIndex(3);
		} else {
			nullValueStringCombo.setSelectedItem(dbCsvExportDefinition.getNullValueString());
		}
		
		checkButtonStatus();
	}

	/**
	 * Check button status.
	 */
	private void checkButtonStatus() {
		if (DbVendor.SQLite.toString().equalsIgnoreCase((String) dbTypeCombo.getSelectedItem()) || DbVendor.Derby.toString().equalsIgnoreCase((String) dbTypeCombo.getSelectedItem())) {
			hostField.setEnabled(false);
			userField.setEnabled(false);
			passwordField.setEnabled(false);
		} else {
			hostField.setEnabled(true);
			userField.setEnabled(true);
			passwordField.setEnabled(true);
		}

		if (ExportType.CSV.toString().equalsIgnoreCase((String) exportTypeCombo.getSelectedItem())) {
			separatorCombo.setEnabled(true);
			stringQuoteCombo.setEnabled(true);
			alwaysQuoteBox.setEnabled(true);
			noHeadersBox.setEnabled(true);
			beautifyBox.setEnabled(true);
			indentationCombo.setEnabled(false);
			nullValueStringCombo.setEnabled(true);
		} else if (ExportType.JSON.toString().equalsIgnoreCase((String) exportTypeCombo.getSelectedItem())) {
			separatorCombo.setEnabled(false);
			stringQuoteCombo.setEnabled(false);
			alwaysQuoteBox.setEnabled(false);
			noHeadersBox.setEnabled(false);
			beautifyBox.setEnabled(true);
			indentationCombo.setEnabled(true);
			nullValueStringCombo.setEnabled(false);
		} else if (ExportType.XML.toString().equalsIgnoreCase((String) exportTypeCombo.getSelectedItem())) {
			separatorCombo.setEnabled(false);
			stringQuoteCombo.setEnabled(false);
			alwaysQuoteBox.setEnabled(false);
			noHeadersBox.setEnabled(false);
			beautifyBox.setEnabled(true);
			indentationCombo.setEnabled(true);
			nullValueStringCombo.setEnabled(true);
		} else if (ExportType.SQL.toString().equalsIgnoreCase((String) exportTypeCombo.getSelectedItem())) {
			separatorCombo.setEnabled(false);
			stringQuoteCombo.setEnabled(false);
			alwaysQuoteBox.setEnabled(false);
			noHeadersBox.setEnabled(false);
			beautifyBox.setEnabled(false);
			indentationCombo.setEnabled(false);
			nullValueStringCombo.setEnabled(false);
		}

		if (DbVendor.SQLite.toString().equalsIgnoreCase((String) dbTypeCombo.getSelectedItem()) || ExportType.JSON.toString().equalsIgnoreCase((String) exportTypeCombo.getSelectedItem())) {
			localeCombo.setEnabled(false);
		} else {
			localeCombo.setEnabled(true);
		}
	}

	/**
	 * Export.
	 *
	 * @param dbCsvExportDefinition
	 *            the db csv export definition
	 * @param dbCsvExportGui
	 *            the db csv export gui
	 */
	private void export(DbCsvExportDefinition dbCsvExportDefinition, final DbCsvExportGui dbCsvExportGui) {
		try {
			dbCsvExportDefinition.checkParameters();
			if (!new DbCsvExportDriverSupplier(this, dbCsvExportDefinition.getDbVendor()).supplyDriver()) {
				throw new Exception("Cannot aquire db driver for db vendor: " + dbCsvExportDefinition.getDbVendor());
			}
			
			// The worker parent is set later by the opened DualProgressDialog
			AbstractDbExportWorker worker = dbCsvExportDefinition.getConfiguredWorker(null);

			DualProgressDialog<AbstractDbExportWorker> progressDialog = new DualProgressDialog<AbstractDbExportWorker>(dbCsvExportGui, DbCsvExport.APPLICATION_NAME, worker);
			Result result = progressDialog.showDialog();

			if (result == Result.CANCELED) {
				new TextDialog(dbCsvExportGui, DbCsvExport.APPLICATION_NAME, LangResources.get("error.canceledbyuser"), LangResources.get("close"), Color.YELLOW).setVisible(true);
			} else if (result == Result.ERROR) {
				Exception e = progressDialog.getWorker().getError();
				if (e instanceof DbCsvExportException) {
					TextDialog textDialog = new TextDialog(dbCsvExportGui, DbCsvExport.APPLICATION_NAME + " ERROR", "ERROR:\n" + ((DbCsvExportException) e).getMessage(), LangResources.get("close"), Color.PINK);
					textDialog.setVisible(true);
				} else {
					String stacktrace = ExceptionUtilities.getStackTrace(e);
					new TextDialog(dbCsvExportGui, DbCsvExport.APPLICATION_NAME + " ERROR", "ERROR:\n" + e.getClass().getSimpleName() + ":\n" + e.getMessage() + "\n\n" + stacktrace, LangResources.get("close"), false, Color.PINK).setVisible(true);
				}
			} else {
				Date start = worker.getStartTime();
				Date end = worker.getEndTime();
				long itemsDone = worker.getItemsDone();

				String resultText = LangResources.get("start") + ": " + start + "\n" + LangResources.get("end") + ": " + end + "\n" + LangResources.get("timeelapsed") + ": "
						+ DateUtilities.getHumanReadableTimespan(end.getTime() - start.getTime(), true);

				if (dbCsvExportDefinition.getSqlStatementOrTablelist().toLowerCase().startsWith("select ")) {
					resultText += "\n" + LangResources.get("exported") + " 1 select";
				} else {
					resultText += "\n" + LangResources.get("exportedtables") + ": " + itemsDone;
				}

				long exportedLines = worker.getOverallExportedLines();

				resultText += "\n" + LangResources.get("exportedlines") + ": " + exportedLines;

				if (!dbCsvExportDefinition.getOutputpath().toLowerCase().equalsIgnoreCase("console")) {
					long exportedDataAmount = worker.getOverallExportedDataAmount();
					resultText += "\n" + LangResources.get("exporteddataamount") + ": " + Utilities.getHumanReadableNumber(exportedDataAmount, "B");
				}

				new TextDialog(dbCsvExportGui, DbCsvExport.APPLICATION_NAME, LangResources.get("result") + ":\n" + resultText, LangResources.get("close"), false).setVisible(true);
			}
		} catch (Exception e) {
			new TextDialog(dbCsvExportGui, DbCsvExport.APPLICATION_NAME + " ERROR", "ERROR:\n" + e.getMessage(), LangResources.get("close"), Color.PINK).setVisible(true);
		}
	}
}
