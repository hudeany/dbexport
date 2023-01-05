package de.soderer.dbimport;

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
import javax.swing.JPanel;
import javax.swing.JScrollPane;

import de.soderer.utilities.DbColumnType;
import de.soderer.utilities.DbColumnType.SimpleDataType;
import de.soderer.utilities.LangResources;
import de.soderer.utilities.Triple;
import de.soderer.utilities.Tuple;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.collection.CaseInsensitiveMap;
import de.soderer.utilities.swing.ModalDialog;

public class DbImportMappingDialog extends ModalDialog<Boolean> {
	private static final long serialVersionUID = 396542497082344683L;

	private final CaseInsensitiveMap<DbColumnType> columnTypes;
	private final List<String> dataColumns;
	private String mappingString;

	private final List<Triple<Label, JComboBox<String>, JComboBox<String>>> mappingEntries = new ArrayList<>();

	public DbImportMappingDialog(final Window parent, final String title, final CaseInsensitiveMap<DbColumnType> columnTypes, final List<String> dataColumns, final List<String> keyColumns) throws Exception {
		super(parent, title);

		this.columnTypes = columnTypes;
		this.dataColumns = dataColumns;

		setResizable(false);

		final JPanel panel = new JPanel();
		panel.setLayout(new BoxLayout(panel, BoxLayout.PAGE_AXIS));

		add(panel);

		panel.add(Box.createRigidArea(new Dimension(0, 5)));

		final JPanel mappingPanel = new JPanel();
		mappingPanel.setLayout(new BoxLayout(mappingPanel, BoxLayout.PAGE_AXIS));

		final JPanel mappingEntryPanelHeader = new JPanel(new FlowLayout(FlowLayout.LEFT));

		mappingEntryPanelHeader.add(Box.createRigidArea(new Dimension(5, 0)));

		final Label dbColumnLabelHeader1 = new Label("DB-Column");
		dbColumnLabelHeader1.setPreferredSize(new Dimension(200, 18));
		mappingEntryPanelHeader.add(dbColumnLabelHeader1);

		mappingEntryPanelHeader.add(Box.createRigidArea(new Dimension(5, 0)));

		final Label dbColumnLabelHeader2 = new Label("Data-Column");
		dbColumnLabelHeader2.setPreferredSize(new Dimension(130, 18));
		mappingEntryPanelHeader.add(dbColumnLabelHeader2);

		mappingEntryPanelHeader.add(Box.createRigidArea(new Dimension(5, 0)));

		final Label dbColumnLabelHeader3 = new Label("Formatinfo");
		mappingEntryPanelHeader.add(dbColumnLabelHeader3);

		mappingEntryPanelHeader.add(Box.createRigidArea(new Dimension(5, 0)));
		mappingPanel.add(mappingEntryPanelHeader);

		final List<String> dbColumnNames = new ArrayList<>(columnTypes.keySet());
		Collections.sort(dbColumnNames);
		if (keyColumns != null) {
			for (final String keyColumn : keyColumns) {
				final boolean wasIncluded = dbColumnNames.remove(keyColumn);
				if (wasIncluded) {
					dbColumnNames.add(0, keyColumn);
				}
			}
		}

		for (final String dbColumnName : dbColumnNames) {
			final DbColumnType dbColumnType = columnTypes.get(dbColumnName);

			final JPanel mappingEntryPanel = new JPanel(new FlowLayout(FlowLayout.LEFT));

			mappingEntryPanel.add(Box.createRigidArea(new Dimension(5, 0)));

			final Label dbColumnLabel = new Label(dbColumnName);
			dbColumnLabel.setPreferredSize(new Dimension(200, 18));
			mappingEntryPanel.add(dbColumnLabel);

			mappingEntryPanel.add(Box.createRigidArea(new Dimension(5, 0)));

			final JComboBox<String> dataFieldCombo = new JComboBox<>();
			dataFieldCombo.addItem("");
			for (final String dataColumn : dataColumns) {
				dataFieldCombo.addItem(dataColumn);
			}
			mappingEntryPanel.add(dataFieldCombo);

			mappingEntryPanel.add(Box.createRigidArea(new Dimension(5, 0)));

			JComboBox<String> optionalComboBox = null;
			if (dbColumnType.getSimpleDataType() == SimpleDataType.Double || dbColumnType.getSimpleDataType() == SimpleDataType.Integer) {
				optionalComboBox = new JComboBox<>();
				optionalComboBox.addItem(".");
				optionalComboBox.addItem(",");
				mappingEntryPanel.add(optionalComboBox);
			} else if (dbColumnType.getSimpleDataType() == SimpleDataType.DateTime) {
				optionalComboBox = new JComboBox<>();
				optionalComboBox.setEditable(true);
				optionalComboBox.addItem("dd.MM.yyyy HH:mm:ss");
				optionalComboBox.addItem("dd.MM.yyyy");
				optionalComboBox.addItem("yyyy/MM/dd HH:mm:ss");
				optionalComboBox.addItem("yyyy/MM/dd");
				mappingEntryPanel.add(optionalComboBox);
			} else if (dbColumnType.getSimpleDataType() == SimpleDataType.Date) {
				optionalComboBox = new JComboBox<>();
				optionalComboBox.setEditable(true);
				optionalComboBox.addItem("dd.MM.yyyy HH:mm:ss");
				optionalComboBox.addItem("dd.MM.yyyy");
				optionalComboBox.addItem("yyyy/MM/dd HH:mm:ss");
				optionalComboBox.addItem("yyyy/MM/dd");
				mappingEntryPanel.add(optionalComboBox);
			} else if (dbColumnType.getSimpleDataType() == SimpleDataType.Blob || dbColumnType.getSimpleDataType() == SimpleDataType.Clob) {
				optionalComboBox = new JComboBox<>();
				optionalComboBox.addItem("");
				optionalComboBox.addItem("file");
				mappingEntryPanel.add(optionalComboBox);
			} else if (dbColumnType.getSimpleDataType() == SimpleDataType.String) {
				optionalComboBox = new JComboBox<>();
				optionalComboBox.addItem("");
				optionalComboBox.addItem("LowerCase");
				optionalComboBox.addItem("UpperCase");
				mappingEntryPanel.add(optionalComboBox);

				if ("email".equalsIgnoreCase(dbColumnName)) {
					optionalComboBox.setSelectedItem("LowerCase");
				}
			}

			mappingPanel.add(mappingEntryPanel);

			mappingEntryPanel.add(Box.createRigidArea(new Dimension(5, 0)));

			mappingEntries.add(new Triple<>(dbColumnLabel, dataFieldCombo, optionalComboBox));
		}

		final JScrollPane mappingScrollpane = new JScrollPane(mappingPanel);
		mappingScrollpane.setPreferredSize(new Dimension(533, Utilities.limitValue(100, mappingPanel.getPreferredSize().height, 500)));

		panel.add(mappingScrollpane);

		panel.add(Box.createRigidArea(new Dimension(0, 5)));

		final JPanel buttonPanel = new JPanel();
		panel.add(buttonPanel);

		buttonPanel.add(Box.createRigidArea(new Dimension(5, 0)));

		final JButton okButton = new JButton(LangResources.get("ok"));
		okButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent event) {
				createMappingString();
				dispose();
			}
		});
		buttonPanel.add(okButton);

		buttonPanel.add(Box.createRigidArea(new Dimension(5, 0)));

		final JButton cancelButton = new JButton(LangResources.get("cancel"));
		cancelButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent event) {
				dispose();
			}
		});
		buttonPanel.add(cancelButton);

		buttonPanel.add(Box.createRigidArea(new Dimension(5, 0)));

		panel.add(Box.createRigidArea(new Dimension(0, 5)));

		pack();

		setLocationRelativeTo(parent);

		returnValue = true;
	}

	private void fillMappingEntries(final CaseInsensitiveMap<DbColumnType> columnTypesToUse, final List<String> dataColumnsToUse) throws IOException, Exception {
		Map<String, Tuple<String, String>> mapping;
		if (Utilities.isNotBlank(mappingString)) {
			mapping = parseMappingString(mappingString);
		} else {
			// Create default mapping
			mapping = new HashMap<>();
			for (final String dbColumn : columnTypesToUse.keySet()) {
				for (final String dataColumn : dataColumnsToUse) {
					if (Utilities.trimSimultaneously(Utilities.trimSimultaneously(dbColumn, "\""), "`").equalsIgnoreCase(dataColumn)) {
						mapping.put(dbColumn, new Tuple<>(dataColumn, ""));
						break;
					}
				}
			}
		}

		for (final Triple<Label, JComboBox<String>, JComboBox<String>> mappingEntry : mappingEntries) {
			for (final Entry<String, Tuple<String, String>> entry : mapping.entrySet()) {
				if (entry.getKey().equals(mappingEntry.getFirst().getText())) {
					mappingEntry.getSecond().setSelectedItem(entry.getValue().getFirst());
					if (mappingEntry.getThird() != null && Utilities.isNotBlank(entry.getValue().getSecond())) {
						if ("lc".equalsIgnoreCase(entry.getValue().getSecond())) {
							mappingEntry.getThird().setSelectedItem("LowerCase");
						} else if ("uc".equalsIgnoreCase(entry.getValue().getSecond())) {
							mappingEntry.getThird().setSelectedItem("UpperCase");
						} else {
							mappingEntry.getThird().setSelectedItem(entry.getValue().getSecond());
						}
					}
					break;
				}
			}
		}
	}

	private void createMappingString() {
		mappingString = "";

		for (final Triple<Label, JComboBox<String>, JComboBox<String>> mappingEntry : mappingEntries) {
			if (Utilities.isNotBlank(((String) mappingEntry.getSecond().getSelectedItem()))) {
				mappingString += mappingEntry.getFirst().getText() + "=\"" + ((String) mappingEntry.getSecond().getSelectedItem()) + "\"";
				if (mappingEntry.getThird() != null && Utilities.isNotBlank(((String) mappingEntry.getThird().getSelectedItem()))) {
					final String formatValue = ((String) mappingEntry.getThird().getSelectedItem());
					if ("lowercase".equalsIgnoreCase(formatValue)) {
						mappingString += " lc";
					} else if ("uppercase".equalsIgnoreCase(formatValue)) {
						mappingString += " uc";
					} else {
						mappingString += " " + formatValue;
					}
				}
				mappingString += "\n";
			}
		}
	}

	public void setMappingString(final String mappingString) throws Exception {
		this.mappingString = mappingString;
		fillMappingEntries(columnTypes, dataColumns);
	}

	public String getMappingString() {
		return mappingString;
	}

	/**
	 * Mapping Map contains dbColumn as key, csvFileColumn as valueTuples first and formatString as valeTuples second
	 *
	 * @param mappingString
	 * @return
	 * @throws IOException
	 * @throws Exception
	 */
	public static Map<String, Tuple<String, String>> parseMappingString(final String mappingString) throws IOException, Exception {
		final Map<String, Tuple<String, String>> mapping = new HashMap<>();
		final List<String> mappingLines = Utilities.splitAndTrimList(mappingString, ';', '\n', '\r');
		for (final String mappingLine : mappingLines) {
			final int dbColumnEnd = mappingLine.indexOf("=");
			if (dbColumnEnd <= 0) {
				throw new Exception("Invalid mapping line: " + mappingLine);
			}
			final String dbColumn = mappingLine.substring(0, dbColumnEnd).toLowerCase().trim();
			if (mapping.containsKey(dbColumn)) {
				throw new Exception("Invalid mapping line with duplicate db column: " + mappingLine);
			}
			String rest = mappingLine.substring(dbColumnEnd + 1).trim();
			if (Utilities.isNotBlank(rest)) {
				if (rest.length() < 2 || (!rest.startsWith("\"") && !rest.startsWith("'"))) {
					throw new Exception("Invalid mapping line: " + mappingLine);
				}
				final int dataColumnEnd = rest.indexOf(rest.charAt(0), 1);
				if (dataColumnEnd <= 0) {
					throw new Exception("Invalid mapping line: " + mappingLine);
				}
				final String dataColumn = rest.substring(1, dataColumnEnd);
				rest = rest.substring(dataColumnEnd + 1).trim();
				if ("".equals(rest)) {
					mapping.put(dbColumn, new Tuple<>(dataColumn, ""));
				} else if (".".equals(rest)) {
					mapping.put(dbColumn, new Tuple<>(dataColumn, "."));
				} else if (",".equals(rest)) {
					mapping.put(dbColumn, new Tuple<>(dataColumn, ","));
				} else if ("file".equalsIgnoreCase(rest)) {
					mapping.put(dbColumn, new Tuple<>(dataColumn, "file"));
				} else if (Pattern.matches("[ yYmMdDhHsS:.-/]+", rest)) {
					mapping.put(dbColumn, new Tuple<>(dataColumn, rest));
				} else if ("lc".equalsIgnoreCase(rest)) {
					mapping.put(dbColumn, new Tuple<>(dataColumn, "lc"));
				} else if ("uc".equalsIgnoreCase(rest)) {
					mapping.put(dbColumn, new Tuple<>(dataColumn, "uc"));
				} else {
					throw new Exception("Invalid mapping line: " + mappingLine);
				}
			}
		}
		return mapping;
	}
}
