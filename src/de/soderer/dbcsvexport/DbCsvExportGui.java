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
import de.soderer.utilities.ApplicationUpdateHelper;
import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.DbUtilities;
import de.soderer.utilities.ExceptionUtilities;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.Version;
import de.soderer.utilities.WorkerDual;
import de.soderer.utilities.DbUtilities.DbVendor;
import de.soderer.utilities.swing.BasicUpdateableGuiApplication;
import de.soderer.utilities.swing.DualProgressDialog;
import de.soderer.utilities.swing.SimpleProgressDialog.Result;
import de.soderer.utilities.swing.TextDialog;

public class DbCsvExportGui extends BasicUpdateableGuiApplication {
	private static final long serialVersionUID = 5969613637206441880L;
	
	private JComboBox<String> dbTypeCombo;
	private JTextField hostField;
	private JTextField dbNameField;
	private JTextField userField;
	private JTextField passwordField;
	private JComboBox<String> exportTypeCombo;
	private JCheckBox fileLogBox;
	private JTextField outputpathField;
	private JComboBox<String> separatorCombo;
	private JComboBox<String> stringQuoteCombo;
	private JComboBox<String> indentationCombo;
	private JComboBox<String> nullValueStringCombo;
	private JComboBox<String> encodingCombo;
	private JComboBox<String> localeCombo;
	private JTextArea statementField;
	private JCheckBox zipBox;
	private JCheckBox blobfilesBox;
	private JCheckBox clobfilesBox;
	private JCheckBox alwaysQuoteBox;
	private JCheckBox beautifyBox;
	private JCheckBox exportStructureBox;
	private JCheckBox noHeadersyBox;

	public DbCsvExportGui(DbCsvExportDefinition dbCsvExportDefinition) throws Exception {
		super(DbCsvExport.APPLICATION_NAME, new Version(DbCsvExport.VERSION));

		final DbCsvExportGui dbCsvExportGui = this;

		setLocationRelativeTo(null);

		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		setLayout(new BoxLayout(this.getContentPane(), BoxLayout.PAGE_AXIS));
		
		// Parameter Panel
		JPanel parameterPanel = new JPanel();
		parameterPanel.setLayout(new BoxLayout(parameterPanel, BoxLayout.LINE_AXIS));

		// Mandatory parameter Panel
		JPanel mandatoryParameterPanel = new JPanel();
		mandatoryParameterPanel.setLayout(new BoxLayout(mandatoryParameterPanel, BoxLayout.PAGE_AXIS));

		// DBType Pane
		JPanel dbTypePanel = new JPanel();
		dbTypePanel.setLayout(new FlowLayout());
		JLabel dbTypeLabel = new JLabel("DB-Type");
		dbTypePanel.add(dbTypeLabel);
		dbTypeCombo = new JComboBox<String>();
		dbTypeCombo.setToolTipText("DB-Type: Oracle (default port 1521), MySQL (default port 3306), PostgreSQL (default port 5432), SQLite or Derby");
		for (DbVendor dbVendor : DbVendor.values()) {
			dbTypeCombo.addItem(dbVendor.toString());
		}
		for (int i = 0; i < dbTypeCombo.getItemCount(); i++) {
			if (DbUtilities.DbVendor.getDbVendorByName(dbTypeCombo.getItemAt(i)) == dbCsvExportDefinition.getDbVendor()) {
				dbTypeCombo.setSelectedIndex(i);
				break;
			}
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
		JLabel hostLabel = new JLabel("Host");
		hostPanel.add(hostLabel);
		hostField = new JTextField();
		hostField.setToolTipText("Hostname and optional port like \"hostname:port\"");
		hostField.setPreferredSize(new Dimension(200, hostField.getPreferredSize().height));
		if (Utilities.isNotBlank(dbCsvExportDefinition.getHostname())) {
			hostField.setText(dbCsvExportDefinition.getHostname());
		}
		hostPanel.add(hostField);
		mandatoryParameterPanel.add(hostPanel);

		// DB name Panel
		JPanel dbNamePanel = new JPanel();
		dbNamePanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		JLabel dbNameLabel = new JLabel("DB-Name");
		dbNamePanel.add(dbNameLabel);
		dbNameField = new JTextField();
		dbNameField.setToolTipText("Database name or Oracle SID");
		dbNameField.setPreferredSize(new Dimension(200, dbNameField.getPreferredSize().height));
		if (Utilities.isNotBlank(dbCsvExportDefinition.getDbName())) {
			dbNameField.setText(dbCsvExportDefinition.getDbName());
		}
		dbNamePanel.add(dbNameField);
		mandatoryParameterPanel.add(dbNamePanel);

		// User Panel
		JPanel userPanel = new JPanel();
		userPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		JLabel userLabel = new JLabel("User");
		userPanel.add(userLabel);
		userField = new JTextField();
		userField.setToolTipText("Username for db authentification");
		userField.setPreferredSize(new Dimension(200, userField.getPreferredSize().height));
		if (Utilities.isNotBlank(dbCsvExportDefinition.getUsername())) {
			userField.setText(dbCsvExportDefinition.getUsername());
		}
		userPanel.add(userField);
		mandatoryParameterPanel.add(userPanel);

		// Password Panel
		JPanel passwordPanel = new JPanel();
		passwordPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		JLabel passwordLabel = new JLabel("Password");
		passwordPanel.add(passwordLabel);
		passwordField = new JPasswordField();
		passwordField.setToolTipText("Password for db authentification");
		passwordField.setPreferredSize(new Dimension(200, passwordField.getPreferredSize().height));
		if (Utilities.isNotBlank(dbCsvExportDefinition.getPassword())) {
			passwordField.setText(dbCsvExportDefinition.getPassword());
		}
		passwordPanel.add(passwordField);
		mandatoryParameterPanel.add(passwordPanel);
		
		// Export type Pane
		JPanel exportTypePanel = new JPanel();
		exportTypePanel.setLayout(new FlowLayout());
		JLabel exportTypeLabel = new JLabel("Export-Format");
		exportTypePanel.add(exportTypeLabel);
		exportTypeCombo = new JComboBox<String>();
		exportTypeCombo.setToolTipText("Export-Format: CSV, JSON, XML or SQL. Don't forget to beautify JSON for human readable data");
		exportTypeCombo.addItem("CSV");
		exportTypeCombo.addItem("JSON");
		exportTypeCombo.addItem("XML");
		exportTypeCombo.addItem("SQL");
		for (int i = 0; i < exportTypeCombo.getItemCount(); i++) {
			if (exportTypeCombo.getItemAt(i).equalsIgnoreCase(dbCsvExportDefinition.getExportType().toString())) {
				exportTypeCombo.setSelectedIndex(i);
				break;
			}
		}
		exportTypeCombo.addItemListener(new ItemListener() {
			@Override
			public void itemStateChanged(ItemEvent e) {
				if (((String) exportTypeCombo.getSelectedItem()).equalsIgnoreCase("JSON")
						|| ((String) exportTypeCombo.getSelectedItem()).equalsIgnoreCase("XML")) {
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
		JLabel outputpathLabel = new JLabel("Outputpath");
		outputpathPanel.add(outputpathLabel);
		outputpathField = new JTextField();
		outputpathField.setToolTipText("File for single statement or directory for tablepatterns or 'console' for output to terminal");
		outputpathField.setPreferredSize(new Dimension(200, outputpathField.getPreferredSize().height));
		outputpathField.setBorder(BorderFactory.createLineBorder(Color.GRAY));
		if (Utilities.isNotBlank(dbCsvExportDefinition.getOutputpath())) {
			outputpathField.setText(dbCsvExportDefinition.getOutputpath());
		}
		outputpathPanel.add(outputpathField);
		mandatoryParameterPanel.add(outputpathPanel);

		// Encoding Pane
		JPanel encodingPanel = new JPanel();
		encodingPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		JLabel encodingLabel = new JLabel("Encoding");
		encodingPanel.add(encodingLabel);
		encodingCombo = new JComboBox<String>();
		encodingCombo.setToolTipText("Output encoding");
		encodingCombo.setPreferredSize(new Dimension(200, encodingCombo.getPreferredSize().height));
		encodingCombo.addItem("UTF-8");
		encodingCombo.addItem("ISO-8859-15");
		encodingCombo.setEditable(true);
		boolean foundEncoding = false;
		for (int i = 0; i < encodingCombo.getItemCount(); i++) {
			if (encodingCombo.getItemAt(i).equalsIgnoreCase(dbCsvExportDefinition.getEncoding())) {
				encodingCombo.setSelectedIndex(i);
				foundEncoding = true;
				break;
			}
		}
		if (!foundEncoding) {
			encodingCombo.setSelectedItem(dbCsvExportDefinition.getEncoding());
		}
		encodingPanel.add(encodingCombo);
		mandatoryParameterPanel.add(encodingPanel);

		// Separator Pane
		JPanel separatorPanel = new JPanel();
		separatorPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		JLabel separatorLabel = new JLabel("Separator");
		separatorPanel.add(separatorLabel);
		separatorCombo = new JComboBox<String>();
		separatorCombo.setToolTipText("CSV value separator");
		separatorCombo.setPreferredSize(new Dimension(200, separatorCombo.getPreferredSize().height));
		separatorCombo.addItem(";");
		separatorCombo.addItem(",");
		separatorCombo.setEditable(true);
		boolean separatorEncoding = false;
		for (int i = 0; i < separatorCombo.getItemCount(); i++) {
			if (separatorCombo.getItemAt(i).equalsIgnoreCase("" + dbCsvExportDefinition.getSeparator())) {
				separatorCombo.setSelectedIndex(i);
				separatorEncoding = true;
				break;
			}
		}
		if (!separatorEncoding) {
			separatorCombo.setSelectedItem("" + dbCsvExportDefinition.getSeparator());
		}
		separatorPanel.add(separatorCombo);
		mandatoryParameterPanel.add(separatorPanel);

		// StringQuote Pane
		JPanel stringQuotePanel = new JPanel();
		stringQuotePanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		JLabel stringQuoteLabel = new JLabel("Stringquote");
		stringQuotePanel.add(stringQuoteLabel);
		stringQuoteCombo = new JComboBox<String>();
		stringQuoteCombo.setToolTipText("CSV string quote for values");
		stringQuoteCombo.setPreferredSize(new Dimension(200, stringQuoteCombo.getPreferredSize().height));
		stringQuoteCombo.addItem("\"");
		stringQuoteCombo.addItem("'");
		stringQuoteCombo.setEditable(true);
		boolean stringQuoteFound = false;
		for (int i = 0; i < stringQuoteCombo.getItemCount(); i++) {
			if (stringQuoteCombo.getItemAt(i).equalsIgnoreCase("" + dbCsvExportDefinition.getStringQuote())) {
				stringQuoteCombo.setSelectedIndex(i);
				stringQuoteFound = true;
				break;
			}
		}
		if (!stringQuoteFound) {
			stringQuoteCombo.setSelectedItem("" + dbCsvExportDefinition.getStringQuote());
		}
		stringQuotePanel.add(stringQuoteCombo);
		mandatoryParameterPanel.add(stringQuotePanel);

		// Indentation Pane
		JPanel indentationPanel = new JPanel();
		indentationPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		JLabel indentationLabel = new JLabel("Indentation");
		indentationPanel.add(indentationLabel);
		indentationCombo = new JComboBox<String>();
		indentationCombo.setToolTipText("CSV string quote for values");
		indentationCombo.setPreferredSize(new Dimension(200, indentationCombo.getPreferredSize().height));
		indentationCombo.addItem("TAB");
		indentationCombo.addItem("BLANK");
		indentationCombo.addItem("DOUBLEBLANK");
		indentationCombo.setEditable(true);
		if (dbCsvExportDefinition.getIndentation() == "\t") {
			indentationCombo.setSelectedIndex(0);
		} else if (dbCsvExportDefinition.getIndentation() == " ") {
			indentationCombo.setSelectedIndex(1);
		} else if (dbCsvExportDefinition.getIndentation() == "  ") {
			indentationCombo.setSelectedIndex(2);
		} else {
			boolean indentationFound = false;
			for (int i = 0; i < indentationCombo.getItemCount(); i++) {
				if (indentationCombo.getItemAt(i).equalsIgnoreCase("" + dbCsvExportDefinition.getIndentation())) {
					indentationCombo.setSelectedIndex(i);
					indentationFound = true;
					break;
				}
			}
			if (!indentationFound) {
				indentationCombo.setSelectedItem("" + dbCsvExportDefinition.getIndentation());
			}
		}
		indentationPanel.add(indentationCombo);
		mandatoryParameterPanel.add(indentationPanel);

		// NullValueString Pane
		JPanel nullValueStringPanel = new JPanel();
		nullValueStringPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		JLabel nullValueStringLabel = new JLabel("Null-Value Text");
		nullValueStringPanel.add(nullValueStringLabel);
		nullValueStringCombo = new JComboBox<String>();
		nullValueStringCombo.setToolTipText("String text for null values in csv and xml format only");
		nullValueStringCombo.setPreferredSize(new Dimension(200, nullValueStringCombo.getPreferredSize().height));
		nullValueStringCombo.addItem("");
		nullValueStringCombo.addItem("NULL");
		nullValueStringCombo.addItem("Null");
		nullValueStringCombo.addItem("null");
		nullValueStringCombo.setEditable(true);
		if (dbCsvExportDefinition.getNullValueString().equals("")) {
			nullValueStringCombo.setSelectedIndex(0);
		} else if (dbCsvExportDefinition.getNullValueString().equals("NULL")) {
			nullValueStringCombo.setSelectedIndex(1);
		} else if (dbCsvExportDefinition.getNullValueString().equals("Null")) {
			nullValueStringCombo.setSelectedIndex(2);
		} else if (dbCsvExportDefinition.getNullValueString().equals("null")) {
			nullValueStringCombo.setSelectedIndex(3);
		} else {
			nullValueStringCombo.setSelectedItem("" + dbCsvExportDefinition.getNullValueString());
		}
		nullValueStringPanel.add(nullValueStringCombo);
		mandatoryParameterPanel.add(nullValueStringPanel);

		// Locale Panel
		JPanel localePanel = new JPanel();
		localePanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		JLabel localeLabel = new JLabel("Locale");
		localePanel.add(localeLabel);
		localeCombo = new JComboBox<String>();
		localeCombo.setToolTipText("Locale to format numbers and date/time values");
		localeCombo.setPreferredSize(new Dimension(200, localeCombo.getPreferredSize().height));
		localeCombo.addItem("DE");
		localeCombo.addItem("EN");
		localeCombo.setEditable(true);
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
		localePanel.add(localeCombo);
		mandatoryParameterPanel.add(localePanel);

		// Statement Panel
		JPanel statementPanel = new JPanel();
		statementPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		JLabel statementLabel = new JLabel("Statement");
		statementPanel.add(statementLabel);
		statementField = new JTextArea();
		statementField.setToolTipText("Single SQL select statment or a comma-separated list of tablenames with wildcards *? and !(not, before tablename)");
		if (Utilities.isNotBlank(dbCsvExportDefinition.getSqlStatementOrTablelist())) {
			statementField.setText(dbCsvExportDefinition.getSqlStatementOrTablelist());
		}
		JScrollPane statementScrollpane = new JScrollPane(statementField);
		statementScrollpane.setPreferredSize(new Dimension(200, 100));
		statementPanel.add(statementScrollpane);
		mandatoryParameterPanel.add(statementPanel);

		// Optional parameters Panel
		JPanel optionalParametersPanel = new JPanel();
		optionalParametersPanel.setLayout(new BoxLayout(optionalParametersPanel, BoxLayout.PAGE_AXIS));

		fileLogBox = new JCheckBox("Filelog");
		fileLogBox.setToolTipText("Create additional log files for each csv file with statistics and other information");
		fileLogBox.setSelected(dbCsvExportDefinition.isLog());
		optionalParametersPanel.add(fileLogBox);

		zipBox = new JCheckBox("Zip");
		zipBox.setToolTipText("Zip exported csv files");
		zipBox.setSelected(dbCsvExportDefinition.isZip());
		optionalParametersPanel.add(zipBox);

		alwaysQuoteBox = new JCheckBox("Always quote");
		alwaysQuoteBox.setToolTipText("Quote every csv value by the string quote character");
		alwaysQuoteBox.setSelected(dbCsvExportDefinition.isAlwaysQuote());
		optionalParametersPanel.add(alwaysQuoteBox);

		blobfilesBox = new JCheckBox("Blobfiles");
		blobfilesBox.setToolTipText("Create a file (.blob or .blob.zip) for each blob instead of base64 encoding");
		blobfilesBox.setSelected(dbCsvExportDefinition.isCreateBlobFiles());
		optionalParametersPanel.add(blobfilesBox);

		clobfilesBox = new JCheckBox("Clobfiles");
		clobfilesBox.setToolTipText("Create a file (.clob or .clob.zip) for each clob instead of base64 encoding");
		clobfilesBox.setSelected(dbCsvExportDefinition.isCreateClobFiles());
		optionalParametersPanel.add(clobfilesBox);

		beautifyBox = new JCheckBox("Beautify");
		beautifyBox.setToolTipText("<html>Beautify csv output to make column values equal length (takes extra time)<br />or beautify json output to make it human readable with linebreak and indention<html>");
		beautifyBox.setSelected(dbCsvExportDefinition.isBeautify());
		optionalParametersPanel.add(beautifyBox);

		noHeadersyBox = new JCheckBox("No headers");
		noHeadersyBox.setToolTipText("Do not export headers in CSV data");
		noHeadersyBox.setSelected(dbCsvExportDefinition.isNoHeaders());
		optionalParametersPanel.add(noHeadersyBox);

		exportStructureBox = new JCheckBox("Export structure");
		exportStructureBox.setToolTipText("Export the tables structure and column types");
		exportStructureBox.setSelected(dbCsvExportDefinition.isExportStructure());
		optionalParametersPanel.add(exportStructureBox);

		// Button Panel
		JPanel buttonPanel = new JPanel();
		buttonPanel.setLayout(new BoxLayout(buttonPanel, BoxLayout.LINE_AXIS));

		// Start Button
		JButton startButton = new JButton("Export");
		startButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent event) {
				try {
					putConfigurationInDefinition(dbCsvExportDefinition);
					
					export(dbCsvExportDefinition, dbCsvExportGui);
				} catch (Exception e) {
					new TextDialog(dbCsvExportGui, "DbCsvExport ERROR", "ERROR:\n" + e.getMessage(), Color.RED).setVisible(true);
				}
			}
		});
		buttonPanel.add(startButton);

		buttonPanel.add(Box.createRigidArea(new Dimension(5, 0)));

		// Update Button
		JButton updateButton = new JButton("Check update");
		updateButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent event) {
				try {
					new ApplicationUpdateHelper(DbCsvExport.APPLICATION_NAME, DbCsvExport.VERSION, DbCsvExport.VERSIONINFO_DOWNLOAD_URL, dbCsvExportGui, "-gui").executeUpdate();
				} catch (Exception e) {
					TextDialog textDialog = new TextDialog(dbCsvExportGui, "DbCsvExport ERROR", "ERROR:\n" + e.getMessage(), Color.RED);
					textDialog.setVisible(true);
				}
			}
		});
		buttonPanel.add(updateButton);
		
		buttonPanel.add(Box.createRigidArea(new Dimension(5, 0)));

		// Close Button
		JButton closeButton = new JButton("Close");
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
		
		checkButtonStatus();
		
		add(Box.createRigidArea(new Dimension(0, 5)));

		pack();

		setLocationRelativeTo(null);
		setResizable(false);
		setVisible(true);
	}

	private void putConfigurationInDefinition(DbCsvExportDefinition dbCsvExportDefinition) throws Exception {
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
		dbCsvExportDefinition.setNoHeaders(noHeadersyBox.isEnabled() ? noHeadersyBox.isSelected() : false);
		dbCsvExportDefinition.setEncoding((String) encodingCombo.getSelectedItem());
		dbCsvExportDefinition.setSeparator(((String) separatorCombo.getSelectedItem()).charAt(0));
		dbCsvExportDefinition.setStringQuote(((String) stringQuoteCombo.getSelectedItem()).charAt(0));
		String indentationString;
		if ("TAB".equalsIgnoreCase((String) indentationCombo.getSelectedItem())) {
			indentationString = "\t";
		} else if ("BLANK".equalsIgnoreCase((String) indentationCombo.getSelectedItem())) {
			indentationString = " ";
		}  else if ("DOUBLEBLANK".equalsIgnoreCase((String) indentationCombo.getSelectedItem())) {
			indentationString = "  ";
		} else {
			indentationString = (String) indentationCombo.getSelectedItem();
		}
		dbCsvExportDefinition.setIndentation(indentationString);
		dbCsvExportDefinition.setDateAndDecimalLocale(localeCombo.isEnabled() ? new Locale((String) localeCombo.getSelectedItem()) : null);
		
		dbCsvExportDefinition.setNullValueString((String) nullValueStringCombo.getSelectedItem());
		
		dbCsvExportDefinition.checkParameters();
		dbCsvExportDefinition.checkAndLoadDbDrivers();
	}
	
	private void checkButtonStatus() {
		if (DbVendor.SQLite.toString().equalsIgnoreCase((String) dbTypeCombo.getSelectedItem())
				|| DbVendor.Derby.toString().equalsIgnoreCase((String) dbTypeCombo.getSelectedItem())) {
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
			noHeadersyBox.setEnabled(true);
			beautifyBox.setEnabled(true);
			indentationCombo.setEnabled(false);
			nullValueStringCombo.setEnabled(true);
		} else if (ExportType.JSON.toString().equalsIgnoreCase((String) exportTypeCombo.getSelectedItem())) {
			separatorCombo.setEnabled(false);
			stringQuoteCombo.setEnabled(false);
			alwaysQuoteBox.setEnabled(false);
			noHeadersyBox.setEnabled(false);
			beautifyBox.setEnabled(true);
			indentationCombo.setEnabled(true);
			nullValueStringCombo.setEnabled(false);
		} else if (ExportType.XML.toString().equalsIgnoreCase((String) exportTypeCombo.getSelectedItem())) {
			separatorCombo.setEnabled(false);
			stringQuoteCombo.setEnabled(false);
			alwaysQuoteBox.setEnabled(false);
			noHeadersyBox.setEnabled(false);
			beautifyBox.setEnabled(true);
			indentationCombo.setEnabled(true);
			nullValueStringCombo.setEnabled(true);
		} else if (ExportType.SQL.toString().equalsIgnoreCase((String) exportTypeCombo.getSelectedItem())) {
			separatorCombo.setEnabled(false);
			stringQuoteCombo.setEnabled(false);
			alwaysQuoteBox.setEnabled(false);
			noHeadersyBox.setEnabled(false);
			beautifyBox.setEnabled(false);
			indentationCombo.setEnabled(false);
			nullValueStringCombo.setEnabled(false);
		}
		
		if (DbVendor.SQLite.toString().equalsIgnoreCase((String) dbTypeCombo.getSelectedItem())
				|| ExportType.JSON.toString().equalsIgnoreCase((String) exportTypeCombo.getSelectedItem())) {
			localeCombo.setEnabled(false);
		} else {
			localeCombo.setEnabled(true);
		}
	}

	private void export(DbCsvExportDefinition dbCsvExportDefinition, final DbCsvExportGui dbCsvExportGui) {
		try {
			WorkerDual<Boolean> worker;
			if (dbCsvExportDefinition.getExportType() == ExportType.JSON) {
				worker = new DbJsonExportWorker(null, dbCsvExportDefinition.getDbVendor(), dbCsvExportDefinition.getHostname(), dbCsvExportDefinition.getDbName(), dbCsvExportDefinition.getUsername(), dbCsvExportDefinition.getPassword(), dbCsvExportDefinition.getSqlStatementOrTablelist(), dbCsvExportDefinition.getOutputpath());
				((DbJsonExportWorker) worker).setBeautify(dbCsvExportDefinition.isBeautify());
				((DbJsonExportWorker) worker).setIndentation(dbCsvExportDefinition.getIndentation());
			} else if (dbCsvExportDefinition.getExportType() == ExportType.XML) {
				worker = new DbXmlExportWorker(null, dbCsvExportDefinition.getDbVendor(), dbCsvExportDefinition.getHostname(), dbCsvExportDefinition.getDbName(), dbCsvExportDefinition.getUsername(), dbCsvExportDefinition.getPassword(), dbCsvExportDefinition.getSqlStatementOrTablelist(), dbCsvExportDefinition.getOutputpath());
				((DbXmlExportWorker) worker).setDateAndDecimalLocale(dbCsvExportDefinition.getDateAndDecimalLocale());
				((DbXmlExportWorker) worker).setBeautify(dbCsvExportDefinition.isBeautify());
				((DbXmlExportWorker) worker).setIndentation(dbCsvExportDefinition.getIndentation());
				((DbXmlExportWorker) worker).setNullValueText(dbCsvExportDefinition.getNullValueString());
			} else if (dbCsvExportDefinition.getExportType() == ExportType.SQL) {
				worker = new DbSqlExportWorker(null, dbCsvExportDefinition.getDbVendor(), dbCsvExportDefinition.getHostname(), dbCsvExportDefinition.getDbName(), dbCsvExportDefinition.getUsername(), dbCsvExportDefinition.getPassword(), dbCsvExportDefinition.getSqlStatementOrTablelist(), dbCsvExportDefinition.getOutputpath());
				((DbSqlExportWorker) worker).setDateAndDecimalLocale(dbCsvExportDefinition.getDateAndDecimalLocale());
				((DbSqlExportWorker) worker).setBeautify(dbCsvExportDefinition.isBeautify());
			} else {
				worker = new DbCsvExportWorker(null, dbCsvExportDefinition.getDbVendor(), dbCsvExportDefinition.getHostname(), dbCsvExportDefinition.getDbName(), dbCsvExportDefinition.getUsername(), dbCsvExportDefinition.getPassword(), dbCsvExportDefinition.getSqlStatementOrTablelist(), dbCsvExportDefinition.getOutputpath());
				((DbCsvExportWorker) worker).setDateAndDecimalLocale(dbCsvExportDefinition.getDateAndDecimalLocale());
				((DbCsvExportWorker) worker).setSeparator(dbCsvExportDefinition.getSeparator());
				((DbCsvExportWorker) worker).setStringQuote(dbCsvExportDefinition.getStringQuote());
				((DbCsvExportWorker) worker).setAlwaysQuote(dbCsvExportDefinition.isAlwaysQuote());
				((DbCsvExportWorker) worker).setBeautify(dbCsvExportDefinition.isBeautify());
				((DbCsvExportWorker) worker).setNoHeaders(dbCsvExportDefinition.isNoHeaders());
				((DbCsvExportWorker) worker).setNullValueText(dbCsvExportDefinition.getNullValueString());
			}
			((AbstractDbExportWorker) worker).setLog(dbCsvExportDefinition.isLog());
			((AbstractDbExportWorker) worker).setZip(dbCsvExportDefinition.isZip());
			((AbstractDbExportWorker) worker).setEncoding(dbCsvExportDefinition.getEncoding());
			((AbstractDbExportWorker) worker).setCreateBlobFiles(dbCsvExportDefinition.isCreateBlobFiles());
			((AbstractDbExportWorker) worker).setCreateClobFiles(dbCsvExportDefinition.isCreateClobFiles());
			((AbstractDbExportWorker) worker).setExportStructure(dbCsvExportDefinition.isExportStructure());
			
			DualProgressDialog<WorkerDual<Boolean>> progressDialog = new DualProgressDialog<WorkerDual<Boolean>>(dbCsvExportGui, "DbCsvExport", worker);
			Result result = progressDialog.showDialog();
			
			if (result == Result.CANCELED) {
				new TextDialog(dbCsvExportGui, "DbCsvExport", "Canceled by user", Color.YELLOW).setVisible(true);
			} else if (result == Result.ERROR) {
				Exception e = progressDialog.getWorker().getError();
				if (e instanceof DbCsvExportException) {
					TextDialog textDialog = new TextDialog(dbCsvExportGui, "DbCsvExport ERROR", "ERROR:\n" + ((DbCsvExportException) e).getMessage(), Color.PINK);
					textDialog.setVisible(true);
				} else {
					String stacktrace = ExceptionUtilities.getStackTrace(e);
					new TextDialog(dbCsvExportGui, "DbCsvExport ERROR", "ERROR:\n" + e.getClass().getSimpleName() + ":\n" + ((Exception) e).getMessage() + "\n\n" + stacktrace, Color.PINK).setVisible(true);
				}
			} else {
				Date start = worker.getStartTime();
				Date end = worker.getEndTime();
				long itemsDone = worker.getItemsDone();
				
				String resultText = "Start: " + start + "\nEnd: " + end + "\nTime elapsed: " + DateUtilities.getHumanReadableTimespan(end.getTime() - start.getTime(), true);
				
				if (dbCsvExportDefinition.getSqlStatementOrTablelist().toLowerCase().startsWith("select ")) {
					resultText += "\nExported 1 select";
				} else {
					resultText += "\nExported tables: " + itemsDone;
				}
				
				long exportedLines;
				if (dbCsvExportDefinition.getExportType() == ExportType.JSON) {
					exportedLines = ((DbJsonExportWorker) worker).getOverallExportedLines();
				} else if (dbCsvExportDefinition.getExportType() == ExportType.XML) {
					exportedLines = ((DbXmlExportWorker) worker).getOverallExportedLines();
				} else if (dbCsvExportDefinition.getExportType() == ExportType.SQL) {
					exportedLines = ((DbSqlExportWorker) worker).getOverallExportedLines();
				} else {
					exportedLines = ((DbCsvExportWorker) worker).getOverallExportedLines();
				}
				
				long exportedDataAmount;
				if (dbCsvExportDefinition.getExportType() == ExportType.JSON) {
					exportedDataAmount = ((DbJsonExportWorker) worker).getOverallExportedDataAmount();
				} else if (dbCsvExportDefinition.getExportType() == ExportType.XML) {
					exportedDataAmount = ((DbXmlExportWorker) worker).getOverallExportedDataAmount();
				} else if (dbCsvExportDefinition.getExportType() == ExportType.SQL) {
					exportedDataAmount = ((DbSqlExportWorker) worker).getOverallExportedDataAmount();
				} else {
					exportedDataAmount = ((DbCsvExportWorker) worker).getOverallExportedDataAmount();
				}
				
				resultText += "\nExported lines: " + exportedLines;
				
				if (!dbCsvExportDefinition.getOutputpath().toLowerCase().equalsIgnoreCase("console")) {
					resultText += "\nExported data amount: " + Utilities.getHumanReadableNumber(exportedDataAmount, "B");
				}
				
				new TextDialog(dbCsvExportGui, "DbCsvExport", "Result:\n" + resultText, Color.WHITE).setVisible(true);
			}
		} catch (Exception e) {
			new TextDialog(dbCsvExportGui, "DbCsvExport ERROR", "ERROR:\n" + e.getMessage(), Color.RED).setVisible(true);
		}
	}
}
