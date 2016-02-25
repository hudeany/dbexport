package de.soderer.dbcsvimport;

import java.awt.Color;
import java.awt.Dialog;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Label;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JDialog;
import javax.swing.JPanel;
import javax.swing.JScrollPane;

import de.soderer.utilities.DbColumnType;
import de.soderer.utilities.DbColumnType.SimpleDataType;
import de.soderer.utilities.LangResources;
import de.soderer.utilities.Triple;
import de.soderer.utilities.Tuple;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.collection.CaseInsensitiveMap;
import de.soderer.utilities.swing.TextDialog;

public class DbCsvImportMappingDialog extends JDialog {
	private static final long serialVersionUID = 396542497082344683L;
	
	private CaseInsensitiveMap<DbColumnType> columnTypes;
	private List<String> dataColumns;
	private String mappingString;
	
	private List<Triple<Label, JComboBox<String>, JComboBox<String>>> mappingEntries = new ArrayList<Triple<Label, JComboBox<String>, JComboBox<String>>>();
	
	public DbCsvImportMappingDialog(Window parent, String title, CaseInsensitiveMap<DbColumnType> columnTypes, List<String> dataColumns) throws Exception {
		super(parent, title, Dialog.ModalityType.DOCUMENT_MODAL);
		
		this.columnTypes = columnTypes;
		this.dataColumns = dataColumns;

		setResizable(false);

		JPanel panel = new JPanel();
		panel.setLayout(new BoxLayout(panel, BoxLayout.PAGE_AXIS));
		
		this.add(panel);
		
		panel.add(Box.createRigidArea(new Dimension(0, 5)));

		JPanel mappingPanel = new JPanel();
		mappingPanel.setLayout(new BoxLayout(mappingPanel, BoxLayout.PAGE_AXIS));
		
		JPanel mappingEntryPanelHeader = new JPanel(new FlowLayout(FlowLayout.LEFT));
		
		mappingEntryPanelHeader.add(Box.createRigidArea(new Dimension(5, 0)));
		
		Label dbColumnLabelHeader1 = new Label("DB-Column");
		dbColumnLabelHeader1.setPreferredSize(new Dimension(200, 18));
		mappingEntryPanelHeader.add(dbColumnLabelHeader1);
		
		mappingEntryPanelHeader.add(Box.createRigidArea(new Dimension(5, 0)));

		Label dbColumnLabelHeader2 = new Label("Data-Column");
		dbColumnLabelHeader2.setPreferredSize(new Dimension(130, 18));
		mappingEntryPanelHeader.add(dbColumnLabelHeader2);
		
		mappingEntryPanelHeader.add(Box.createRigidArea(new Dimension(5, 0)));

		Label dbColumnLabelHeader3 = new Label("Formatinfo");
		mappingEntryPanelHeader.add(dbColumnLabelHeader3);
		
		mappingEntryPanelHeader.add(Box.createRigidArea(new Dimension(5, 0)));
		mappingPanel.add(mappingEntryPanelHeader);
		
		List<String> dbColumnNames = new ArrayList<String>(columnTypes.keySet());
		Collections.sort(dbColumnNames);
		for (String dbColumnName : dbColumnNames) {
			DbColumnType dbColumnType = columnTypes.get(dbColumnName);
			
			JPanel mappingEntryPanel = new JPanel(new FlowLayout(FlowLayout.LEFT));
			
			mappingEntryPanel.add(Box.createRigidArea(new Dimension(5, 0)));
			
			Label dbColumnLabel = new Label(dbColumnName);
			dbColumnLabel.setPreferredSize(new Dimension(200, 18));
			mappingEntryPanel.add(dbColumnLabel);
			
			mappingEntryPanel.add(Box.createRigidArea(new Dimension(5, 0)));
			
			JComboBox<String> dataFieldCombo = new JComboBox<String>();
			dataFieldCombo.addItem("");
			for (String dataColumn : dataColumns) {
				dataFieldCombo.addItem(dataColumn);
			}
			mappingEntryPanel.add(dataFieldCombo);

			mappingEntryPanel.add(Box.createRigidArea(new Dimension(5, 0)));
			
			JComboBox<String> optionalComboBox = null;
			if (dbColumnType.getSimpleDataType() == SimpleDataType.Double || dbColumnType.getSimpleDataType() == SimpleDataType.Integer) {
				optionalComboBox = new JComboBox<String>();
				optionalComboBox.addItem(".");
				optionalComboBox.addItem(",");
				mappingEntryPanel.add(optionalComboBox);
			} else if (dbColumnType.getSimpleDataType() == SimpleDataType.Date) {
				optionalComboBox = new JComboBox<String>();
				optionalComboBox.setEditable(true);
				optionalComboBox.addItem("dd.MM.yyyy HH:mm:ss");
				optionalComboBox.addItem("dd.MM.yyyy");
				optionalComboBox.addItem("yyyy/MM/dd HH:mm:ss");
				optionalComboBox.addItem("yyyy/MM/dd");
				mappingEntryPanel.add(optionalComboBox);
			} else if (dbColumnType.getSimpleDataType() == SimpleDataType.Blob || dbColumnType.getSimpleDataType() == SimpleDataType.Clob) {
				optionalComboBox = new JComboBox<String>();
				optionalComboBox.addItem("");
				optionalComboBox.addItem("file");
				mappingEntryPanel.add(optionalComboBox);
			}
			
			mappingPanel.add(mappingEntryPanel);
			
			mappingEntryPanel.add(Box.createRigidArea(new Dimension(5, 0)));
			
			mappingEntries.add(new Triple<Label, JComboBox<String>, JComboBox<String>>(dbColumnLabel, dataFieldCombo, optionalComboBox));
		}
		
		JScrollPane mappingScrollpane = new JScrollPane(mappingPanel);
		mappingScrollpane.setPreferredSize(new Dimension(533, Utilities.limitValue(100, mappingPanel.getPreferredSize().height, 500)));
		
		panel.add(mappingScrollpane);
		
		panel.add(Box.createRigidArea(new Dimension(0, 5)));

		JPanel buttonPanel = new JPanel();
		panel.add(buttonPanel);

		buttonPanel.add(Box.createRigidArea(new Dimension(5, 0)));
		
		JButton okButton = new JButton(LangResources.get("ok"));
		okButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent event) {
				createMappingString();
				dispose();
			}
		});
		buttonPanel.add(okButton);
		
		buttonPanel.add(Box.createRigidArea(new Dimension(5, 0)));

		JButton cancelButton = new JButton(LangResources.get("cancel"));
		cancelButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent event) {
				dispose();
			}
		});
		buttonPanel.add(cancelButton);

		buttonPanel.add(Box.createRigidArea(new Dimension(5, 0)));
		
		panel.add(Box.createRigidArea(new Dimension(0, 5)));
		
		pack();

		setLocationRelativeTo(parent);
	}

	public boolean showDialog() {
		try {
			setVisible(true);
			return true;
		} catch (Exception e) {
			TextDialog textDialog = new TextDialog((Window) getParent(), "ERROR", "ERROR:\n" + e.getMessage(), LangResources.get("close"), false, Color.RED);
			textDialog.setVisible(true);
			return false;
		}
	}

	private void fillMappingEntries(CaseInsensitiveMap<DbColumnType> columnTypes, List<String> dataColumns) throws IOException, Exception {
		Map<String, Tuple<String, String>> mapping;
		if (Utilities.isNotBlank(mappingString)) {
			mapping = parseMappingString(mappingString);
		} else {
			// Create default mapping
			mapping = new HashMap<String, Tuple<String, String>>();
			for (String dbColumn : columnTypes.keySet()) {
				for (String dataColumn : dataColumns) {
					if (dbColumn.equalsIgnoreCase(dataColumn)) {
						mapping.put(dbColumn, new Tuple<String, String>(dataColumn, ""));
						break;
					}
				}
			}
		}
		
		for (Triple<Label, JComboBox<String>, JComboBox<String>> mappingEntry : mappingEntries) {
			for (Entry<String, Tuple<String, String>> entry : mapping.entrySet()) {
				if (entry.getKey().equals(mappingEntry.getFirst().getText())) {
					mappingEntry.getSecond().setSelectedItem(entry.getValue().getFirst());
					if (mappingEntry.getThird() != null && Utilities.isNotBlank(entry.getValue().getSecond())) {
						mappingEntry.getThird().setSelectedItem(entry.getValue().getSecond());
					}
					break;
				}
			}
		}
	}

	private void createMappingString() {
		mappingString = "";
		
		for (Triple<Label, JComboBox<String>, JComboBox<String>> mappingEntry : mappingEntries) {
			if (Utilities.isNotBlank(((String) mappingEntry.getSecond().getSelectedItem()))) {
				mappingString += mappingEntry.getFirst().getText() + "=\"" + ((String) mappingEntry.getSecond().getSelectedItem()) + "\"";
				if (mappingEntry.getThird() != null && Utilities.isNotBlank(((String) mappingEntry.getThird().getSelectedItem()))) {
					mappingString += " " + ((String) mappingEntry.getThird().getSelectedItem());
				}
				mappingString += "\n";
			}
		}
	}

	public void setMappingString(String mappingString) throws Exception {
		this.mappingString = mappingString;
		fillMappingEntries(columnTypes, dataColumns);
	}

	public String getMappingString() {
		return mappingString;
	}

	public static Map<String, Tuple<String, String>> parseMappingString(String mappingString) throws IOException, Exception {
		Map<String, Tuple<String, String>> mapping = new HashMap<String, Tuple<String, String>>();
		List<String> mappingLines = Utilities.splitAndTrimList(mappingString, ';', '\n', '\r');
		for (String mappingLine : mappingLines) {
			int dbColumnEnd = mappingLine.indexOf("=");
			if (dbColumnEnd <= 0) {
				throw new Exception("Invalid mapping line: " + mappingLine);
			}
			String dbColumn = mappingLine.substring(0, dbColumnEnd).toLowerCase().trim();
			if (mapping.containsKey(dbColumn)) {
				throw new Exception("Invalid mapping line with duplicate db column: " + mappingLine);
			}
			String rest = mappingLine.substring(dbColumnEnd + 1).trim();
			if (Utilities.isNotBlank(rest)) {
				if (rest.length() < 2 || (!rest.startsWith("\"") && !rest.startsWith("'"))) {
					throw new Exception("Invalid mapping line: " + mappingLine);
				}
				int dataColumnEnd = rest.indexOf(rest.charAt(0), 1);
				if (dataColumnEnd <= 0) {
					throw new Exception("Invalid mapping line: " + mappingLine);
				}
				String dataColumn = rest.substring(1, dataColumnEnd);
				rest = rest.substring(dataColumnEnd + 1).trim();
				if ("".equals(rest)) {
					mapping.put(dbColumn, new Tuple<String, String>(dataColumn, ""));
				} else if (".".equals(rest)) {
					mapping.put(dbColumn, new Tuple<String, String>(dataColumn, "."));
				} else if (",".equals(rest)) {
					mapping.put(dbColumn, new Tuple<String, String>(dataColumn, ","));
				} else if ("file".equalsIgnoreCase(rest)) {
					mapping.put(dbColumn, new Tuple<String, String>(dataColumn, "file"));
				} else if (Pattern.matches("[ yYmMdDhHsS:.-/]+", rest)) {
					mapping.put(dbColumn, new Tuple<String, String>(dataColumn, rest));
				} else {
					throw new Exception("Invalid mapping line: " + mappingLine);
				}
			}
		}
		return mapping;
	}
}
