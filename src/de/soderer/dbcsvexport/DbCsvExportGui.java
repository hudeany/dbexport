package de.soderer.dbcsvexport;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Date;
import java.util.Locale;
import java.util.concurrent.ExecutionException;

import javax.swing.BorderFactory;
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

import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.ExceptionUtilities;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.WorkerParentDual;
import de.soderer.utilities.swing.DualProgressDialog;
import de.soderer.utilities.swing.TextDialog;

public class DbCsvExportGui extends JFrame implements WorkerParentDual {
	private static final long serialVersionUID = 5969613637206441880L;

	DualProgressDialog progressDialog;
	DbCsvExportWorker dbCsvExportWorker;

	public DbCsvExportGui(DbCsvExportDefinition dbCsvExportDefinition) {
		super("DbCsvExport (Version " + DbCsvExport.VERSION + ")");

		final DbCsvExportGui dbCsvExportGui = this;

		setLocationRelativeTo(null);

		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		setLayout(new FlowLayout(FlowLayout.LEADING));

		// Parameter Panel
		JPanel parameterPanel = new JPanel();
		parameterPanel.setLayout(new BoxLayout(parameterPanel, BoxLayout.PAGE_AXIS));

		// DBType Pane
		JPanel dbTypePanel = new JPanel();
		dbTypePanel.setLayout(new FlowLayout());
		JLabel dbTypeLabel = new JLabel("DB-Type");
		dbTypePanel.add(dbTypeLabel);
		JComboBox<String> dbTypeCombo = new JComboBox<String>();
		dbTypeCombo.setToolTipText("DB-Type: Oracle (default port 1521) or MySQL (default port 3306)");
		dbTypeCombo.addItem("Oracle");
		dbTypeCombo.addItem("MySQL");
		for (int i = 0; i < dbTypeCombo.getItemCount(); i++) {
			if (dbTypeCombo.getItemAt(i).equalsIgnoreCase(dbCsvExportDefinition.getDbType())) {
				dbTypeCombo.setSelectedIndex(i);
				break;
			}
		}
		dbTypePanel.add(dbTypeCombo, BorderLayout.EAST);
		parameterPanel.add(dbTypePanel);

		// Host Panel
		JPanel hostPanel = new JPanel();
		hostPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		JLabel hostLabel = new JLabel("Host");
		hostPanel.add(hostLabel);
		JTextField hostField = new JTextField();
		hostField.setToolTipText("Hostname and optional port like \"hostname:port\"");
		hostField.setPreferredSize(new Dimension(200, hostField.getPreferredSize().height));
		if (Utilities.isNotBlank(dbCsvExportDefinition.getHostname())) {
			hostField.setText(dbCsvExportDefinition.getHostname());
		}
		hostPanel.add(hostField);
		parameterPanel.add(hostPanel);

		// DB name Panel
		JPanel dbNamePanel = new JPanel();
		dbNamePanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		JLabel dbNameLabel = new JLabel("DB-Name");
		dbNamePanel.add(dbNameLabel);
		JTextField dbNameField = new JTextField();
		dbNameField.setToolTipText("Database name or Oracle SID");
		dbNameField.setPreferredSize(new Dimension(200, dbNameField.getPreferredSize().height));
		if (Utilities.isNotBlank(dbCsvExportDefinition.getDbName())) {
			dbNameField.setText(dbCsvExportDefinition.getDbName());
		}
		dbNamePanel.add(dbNameField);
		parameterPanel.add(dbNamePanel);

		// User Panel
		JPanel userPanel = new JPanel();
		userPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		JLabel userLabel = new JLabel("User");
		userPanel.add(userLabel);
		JTextField userField = new JTextField();
		userField.setToolTipText("Username for db authentification");
		userField.setPreferredSize(new Dimension(200, userField.getPreferredSize().height));
		if (Utilities.isNotBlank(dbCsvExportDefinition.getUsername())) {
			userField.setText(dbCsvExportDefinition.getUsername());
		}
		userPanel.add(userField);
		parameterPanel.add(userPanel);

		// Password Panel
		JPanel passwordPanel = new JPanel();
		passwordPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		JLabel passwordLabel = new JLabel("Password");
		passwordPanel.add(passwordLabel);
		JTextField passwordField = new JPasswordField();
		passwordField.setToolTipText("Password for db authentification");
		passwordField.setPreferredSize(new Dimension(200, passwordField.getPreferredSize().height));
		if (Utilities.isNotBlank(dbCsvExportDefinition.getPassword())) {
			passwordField.setText(dbCsvExportDefinition.getPassword());
		}
		passwordPanel.add(passwordField);
		parameterPanel.add(passwordPanel);

		// Outputpath Panel
		JPanel outputpathPanel = new JPanel();
		outputpathPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		JLabel outputpathLabel = new JLabel("Outputpath");
		outputpathPanel.add(outputpathLabel);
		JTextField outputpathField = new JTextField();
		outputpathField.setToolTipText("File for single statement or directory for tablepatterns or 'console' for output to terminal");
		outputpathField.setPreferredSize(new Dimension(200, outputpathField.getPreferredSize().height));
		outputpathField.setBorder(BorderFactory.createLineBorder(Color.GRAY));
		if (Utilities.isNotBlank(dbCsvExportDefinition.getOutputpath())) {
			outputpathField.setText(dbCsvExportDefinition.getOutputpath());
		}
		outputpathPanel.add(outputpathField);
		parameterPanel.add(outputpathPanel);

		// Encoding Pane
		JPanel encodingPanel = new JPanel();
		encodingPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		JLabel encodingLabel = new JLabel("Encoding");
		encodingPanel.add(encodingLabel);
		JComboBox<String> encodingCombo = new JComboBox<String>();
		encodingCombo.setToolTipText("Output encoding");
		encodingCombo.setPreferredSize(new Dimension(200, encodingCombo.getPreferredSize().height));
		encodingCombo.addItem("UTF-8");
		encodingCombo.addItem("ISO-8859-15");
		encodingCombo.setEditable(true);
		boolean foundEncoding = false;
		for (int i = 0; i < dbTypeCombo.getItemCount(); i++) {
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
		parameterPanel.add(encodingPanel);

		// Separator Pane
		JPanel separatorPanel = new JPanel();
		separatorPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		JLabel separatorLabel = new JLabel("Separator");
		separatorPanel.add(separatorLabel);
		JComboBox<String> separatorCombo = new JComboBox<String>();
		separatorCombo.setToolTipText("CSV value separator");
		separatorCombo.setPreferredSize(new Dimension(200, separatorCombo.getPreferredSize().height));
		separatorCombo.addItem(";");
		separatorCombo.addItem(",");
		separatorCombo.setEditable(true);
		boolean separatorEncoding = false;
		for (int i = 0; i < dbTypeCombo.getItemCount(); i++) {
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
		parameterPanel.add(separatorPanel);

		// StringQuote Pane
		JPanel stringQuotePanel = new JPanel();
		stringQuotePanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		JLabel stringQuoteLabel = new JLabel("Stringquote");
		stringQuotePanel.add(stringQuoteLabel);
		JComboBox<String> stringQuoteCombo = new JComboBox<String>();
		stringQuoteCombo.setToolTipText("CSV string quote for values");
		stringQuoteCombo.setPreferredSize(new Dimension(200, stringQuoteCombo.getPreferredSize().height));
		stringQuoteCombo.addItem("\"");
		stringQuoteCombo.addItem("'");
		stringQuoteCombo.setEditable(true);
		boolean stringQuoteEncoding = false;
		for (int i = 0; i < dbTypeCombo.getItemCount(); i++) {
			if (stringQuoteCombo.getItemAt(i).equalsIgnoreCase("" + dbCsvExportDefinition.getStringQuote())) {
				stringQuoteCombo.setSelectedIndex(i);
				stringQuoteEncoding = true;
				break;
			}
		}
		if (!stringQuoteEncoding) {
			stringQuoteCombo.setSelectedItem("" + dbCsvExportDefinition.getStringQuote());
		}
		stringQuotePanel.add(stringQuoteCombo);
		parameterPanel.add(stringQuotePanel);

		// Locale Panel
		JPanel localePanel = new JPanel();
		localePanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		JLabel localeLabel = new JLabel("Locale");
		localePanel.add(localeLabel);
		JComboBox<String> localeCombo = new JComboBox<String>();
		localeCombo.setToolTipText("Locale to format numbers and date/time values");
		localeCombo.setPreferredSize(new Dimension(200, localeCombo.getPreferredSize().height));
		localeCombo.addItem("DE");
		localeCombo.addItem("EN");
		localeCombo.setEditable(true);
		boolean foundLocale = false;
		for (int i = 0; i < dbTypeCombo.getItemCount(); i++) {
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
		parameterPanel.add(localePanel);

		// Statement Panel
		JPanel statementPanel = new JPanel();
		statementPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		JLabel statementLabel = new JLabel("Statement");
		statementPanel.add(statementLabel);
		JTextArea statementField = new JTextArea();
		statementField.setToolTipText("Single SQL select statment or a comma-separated list of tablenames with wildcards *? and !(not, before tablename)");
		if (Utilities.isNotBlank(dbCsvExportDefinition.getSqlStatementOrTablelist())) {
			statementField.setText(dbCsvExportDefinition.getSqlStatementOrTablelist());
		}
		JScrollPane statementScrollpane = new JScrollPane(statementField);
		statementScrollpane.setPreferredSize(new Dimension(200, 100));
		statementPanel.add(statementScrollpane);
		parameterPanel.add(statementPanel);

		// Optional parameters Panel
		JPanel optionalParametersPanel = new JPanel();
		optionalParametersPanel.setLayout(new BoxLayout(optionalParametersPanel, BoxLayout.PAGE_AXIS));

		JCheckBox fileLogBox = new JCheckBox("Filelog");
		fileLogBox.setToolTipText("Create additional log files for each csv file with statistics and other information");
		fileLogBox.setSelected(dbCsvExportDefinition.isLog());
		optionalParametersPanel.add(fileLogBox);

		JCheckBox zipBox = new JCheckBox("Zip");
		zipBox.setToolTipText("Zip exported csv files");
		zipBox.setSelected(dbCsvExportDefinition.isZip());
		optionalParametersPanel.add(zipBox);

		JCheckBox alwaysQuoteBox = new JCheckBox("Always quote");
		alwaysQuoteBox.setToolTipText("Quote every csv value by the string quote character");
		alwaysQuoteBox.setSelected(dbCsvExportDefinition.isAlwaysQuote());
		optionalParametersPanel.add(alwaysQuoteBox);

		JCheckBox blobfilesBox = new JCheckBox("Blobfiles");
		blobfilesBox.setToolTipText("Create a file (.blob or .blob.zip) for each blob instead of base64 encoding");
		blobfilesBox.setSelected(dbCsvExportDefinition.isCreateBlobFiles());
		optionalParametersPanel.add(blobfilesBox);

		JCheckBox clobfilesBox = new JCheckBox("Clobfiles");
		clobfilesBox.setToolTipText("Create a file (.clob or .clob.zip) for each clob instead of base64 encoding");
		clobfilesBox.setSelected(dbCsvExportDefinition.isCreateClobFiles());
		optionalParametersPanel.add(clobfilesBox);

		JCheckBox beautifyBox = new JCheckBox("Beautify");
		beautifyBox.setToolTipText("Beautify csv output to make column values equal length (takes extra time)");
		beautifyBox.setSelected(dbCsvExportDefinition.isBeautify());
		optionalParametersPanel.add(beautifyBox);

		// Button Panel
		JPanel buttonPanel = new JPanel();
		buttonPanel.setLayout(new BoxLayout(buttonPanel, BoxLayout.LINE_AXIS));

		// Start Button
		JButton startButton = new JButton("Export");
		startButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent event) {
				dbCsvExportDefinition.setDbType((String) dbTypeCombo.getSelectedItem());
				dbCsvExportDefinition.setHostname(hostField.getText());
				dbCsvExportDefinition.setDbName(dbNameField.getText());
				dbCsvExportDefinition.setUsername(userField.getText());
				dbCsvExportDefinition.setPassword(passwordField.getText());
				dbCsvExportDefinition.setOutputpath(outputpathField.getText());
				dbCsvExportDefinition.setSqlStatementOrTablelist(statementField.getText());

				dbCsvExportDefinition.setLog(fileLogBox.isSelected());
				dbCsvExportDefinition.setZip(zipBox.isSelected());
				dbCsvExportDefinition.setAlwaysQuote(alwaysQuoteBox.isSelected());
				dbCsvExportDefinition.setCreateBlobFiles(blobfilesBox.isSelected());
				dbCsvExportDefinition.setCreateClobFiles(clobfilesBox.isSelected());
				dbCsvExportDefinition.setBeautify(beautifyBox.isSelected());

				dbCsvExportDefinition.setEncoding((String) encodingCombo.getSelectedItem());
				dbCsvExportDefinition.setSeparator(((String) separatorCombo.getSelectedItem()).charAt(0));
				dbCsvExportDefinition.setStringQuote(((String) stringQuoteCombo.getSelectedItem()).charAt(0));
				dbCsvExportDefinition.setDateAndDecimalLocale(new Locale((String) localeCombo.getSelectedItem()));

				dbCsvExportWorker = new DbCsvExportWorker(dbCsvExportGui, dbCsvExportDefinition);
				Thread dbCsvExportThread = new Thread(dbCsvExportWorker);
				dbCsvExportThread.start();

				progressDialog = new DualProgressDialog(dbCsvExportGui, "DbCsvExport");
				progressDialog.setVisible(true);
			}
		});
		buttonPanel.add(startButton);

		// Close Button
		JButton closeButton = new JButton("Close");
		closeButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent event) {
				dispose();
			}
		});
		buttonPanel.add(closeButton);

		add(parameterPanel);
		add(optionalParametersPanel);
		parameterPanel.add(buttonPanel);

		pack();

		setLocationRelativeTo(null);
		setResizable(false);
		setVisible(true);
	}

	private String getResult(DbCsvExportWorker dbCsvExportWorker, Date start, Date end, long itemsDone) {
		String result = "Start: " + start + "\nEnd: " + end + "\nTime elapsed: " + DateUtilities.getHumanReadableTimespan(end.getTime() - start.getTime(), true);
		if (dbCsvExportWorker.getDbCsvExportDefinition().getSqlStatementOrTablelist().toLowerCase().startsWith("select ")) {
			result += "\nExported 1 select";
		} else {
			result += "\nExported tables: " + itemsDone;
		}
		result += "\nExported lines: " + dbCsvExportWorker.getOverallExportedLines();
		return result;
	}

	@Override
	public void showUnlimitedProgress() {
		if (progressDialog != null) {
			progressDialog.setIndeterminate();
			progressDialog.setProgress(0, 0);
			progressDialog.setETA(null);
		}
	}

	@Override
	public void showUnlimitedSubProgress() {
		if (progressDialog != null) {
			progressDialog.setSubItemIndeterminate();
			progressDialog.setSubProgress(0, 0);
			progressDialog.setSubETA(null);
		}
	}

	@Override
	public void showProgress(Date start, long itemsToDo, long itemsDone) {
		if (progressDialog != null) {
			progressDialog.setProgress(itemsToDo, itemsDone);
			progressDialog.setETA(DateUtilities.calculateETA(start, itemsToDo, itemsDone));
		}
	}

	@Override
	public void showItemStart(String itemName) {
		if (progressDialog != null) {
			progressDialog.setSubItemTitle(itemName);
			progressDialog.setSubProgress(0, 0);
			progressDialog.setSubETA(null);
		}
	}

	@Override
	public void showItemProgress(Date itemStart, long subItemToDo, long subItemDone) {
		if (progressDialog != null) {
			progressDialog.setSubProgress(subItemToDo, subItemDone);
			progressDialog.setSubETA(DateUtilities.calculateETA(itemStart, subItemToDo, subItemDone));
		}
	}

	@Override
	public void showItemDone(Date itemStart, Date itemEnd, long subItemsDone) {
		if (progressDialog != null) {
			progressDialog.setSubItemTitle(null);
			progressDialog.setSubProgress(0, 0);
			progressDialog.setSubETA(null);
		}
	}

	@Override
	public void showDone(Date start, Date end, long itemsDone) {
		try {
			if (dbCsvExportWorker.isCancelled()) {
				TextDialog textDialog = new TextDialog(this, "DbCsvExport", "Canceled by user", Color.YELLOW);
				textDialog.setVisible(true);
			} else {
				// Get result to trigger possible Exception
				dbCsvExportWorker.get();

				TextDialog textDialog = new TextDialog(this, "DbCsvExport", "Result:\n" + getResult(dbCsvExportWorker, start, end, itemsDone), Color.WHITE);
				textDialog.setVisible(true);
			}
		} catch (ExecutionException e) {
			if (e.getCause() instanceof DbCsvExportException) {
				TextDialog textDialog = new TextDialog(this, "DbCsvExport ERROR", "ERROR:\n" + ((DbCsvExportException) e.getCause()).getMessage(), Color.PINK);
				textDialog.setVisible(true);
			} else if (e.getCause() instanceof Exception) {
				String stacktrace = ExceptionUtilities.getStackTrace(e.getCause());
				TextDialog textDialog = new TextDialog(this, "DbCsvExport ERROR",
						"ERROR:\n" + e.getCause().getClass().getSimpleName() + ":\n" + ((Exception) e.getCause()).getMessage() + "\n\n" + stacktrace, Color.PINK);
				textDialog.setVisible(true);
			} else {
				String stacktrace = ExceptionUtilities.getStackTrace(e.getCause());
				TextDialog textDialog = new TextDialog(this, "DbCsvExport ERROR", "ERROR:\n" + e.getCause().getClass().getSimpleName() + ":\n" + e.getMessage() + "\n\n" + stacktrace, Color.PINK);
				textDialog.setVisible(true);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			progressDialog.dispose();
		}
	}

	@Override
	public void cancel() {
		if (dbCsvExportWorker != null) {
			dbCsvExportWorker.cancel();
		}
	}
}
