package de.soderer.dbimport;

import java.io.File;
import java.sql.Connection;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.LangResources;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.db.DbNotExistsException;
import de.soderer.utilities.db.DbUtilities;
import de.soderer.utilities.db.DbUtilities.DbVendor;
import de.soderer.utilities.worker.WorkerDual;
import de.soderer.utilities.worker.WorkerParentSimple;

public class DbImportMultiWorker extends WorkerDual<Boolean> implements WorkerParentSimple {
	private final DbImportDefinition dbImportDefinition;
	private final List<File> filesToImport;
	private final String tableName;

	private DbImportWorker subWorker;
	private StringBuilder multiImportResult;
	private boolean multiImportHadError;

	public DbImportMultiWorker(final DbImportDefinition dbImportDefinition, final List<File> filesToImport, final String tableName) {
		super(null);

		this.dbImportDefinition = dbImportDefinition;
		this.filesToImport = filesToImport;
		this.tableName = tableName;
	}

	@Override
	public Boolean work() throws Exception {
		boolean constraintsWereDeactivated = false;
		boolean triggersWereDeactivated = false;

		try (Connection connection = getDatabaseConnection(dbImportDefinition)) {
			signalUnlimitedProgress();
			itemsToDo = filesToImport.size();

			multiImportResult = new StringBuilder();
			multiImportHadError = false;
			final LocalDateTime startDate = LocalDateTime.now();
			long importedDataSize = 0l;

			multiImportResult.append(LangResources.get("start") + ": " + DateUtilities.formatDate(DateUtilities.YYYY_MM_DD_HHMMSS, startDate) + "\n");
			itemsDone = 0;
			signalProgress();

			if ("*".equals(tableName) && !dbImportDefinition.isCreateTable()) {
				final List<String> notFoundTables = new ArrayList<>();
				final List<String> availableTables = DbUtilities.getAvailableTables(connection, "*").stream().map(x -> x.toLowerCase()).collect(Collectors.toList());
				for (int fileIndex = 0; fileIndex < filesToImport.size(); fileIndex++) {
					String tableNameToImport = filesToImport.get(fileIndex).getName().toLowerCase();
					if (tableNameToImport.endsWith(".zip")) {
						tableNameToImport = tableNameToImport.substring(0, tableNameToImport.length() - 4);
					}
					if (tableNameToImport.contains(".")) {
						tableNameToImport = tableNameToImport.substring(0, tableNameToImport.indexOf("."));
					}
					if (!availableTables.contains(tableNameToImport)) {
						notFoundTables.add(tableNameToImport);
					}
				}

				if (notFoundTables.size() > 0) {
					throw new Exception("Import tables not available in database: " + Utilities.join(notFoundTables, ", "));
				}
			}

			if (dbImportDefinition.isDeactivateForeignKeyConstraints()) {
				if (dbImportDefinition.isVerbose()) {
					parent.changeTitle(LangResources.get("deactivateForeignKeyConstraints"));
				}
				constraintsWereDeactivated = true;
				DbUtilities.setForeignKeyConstraintStatus(dbImportDefinition.getDbVendor(), connection, false);
				if (!connection.getAutoCommit()) {
					connection.commit();
				}
			}

			if (dbImportDefinition.isDeactivateTriggers()) {
				if (dbImportDefinition.isVerbose()) {
					parent.changeTitle(LangResources.get("deactivateTriggers"));
				}
				triggersWereDeactivated = true;
				DbUtilities.setTriggerStatus(dbImportDefinition.getDbVendor(), connection, false);
				if (!connection.getAutoCommit()) {
					connection.commit();
				}
			}

			for (int fileIndex = 0; fileIndex < filesToImport.size(); fileIndex++) {
				String tableToImport = tableName;

				if ("*".equals(tableToImport)) {
					tableToImport = filesToImport.get(fileIndex).getName();
					if (tableToImport.toLowerCase().endsWith(".zip")) {
						tableToImport = tableToImport.substring(0, tableToImport.length() - 4);
					}
					if (tableToImport.contains(".")) {
						tableToImport = tableToImport.substring(0, tableToImport.indexOf("."));
					}
				}

				final File fileToImport = filesToImport.get(fileIndex);
				if (cancel || (multiImportHadError && dbImportDefinition.isCompleteCommit())) {
					break;
				}

				if ("*".equals(tableToImport)) {
					tableToImport = fileToImport.getName();
					if (tableToImport.toLowerCase().endsWith(".zip")) {
						tableToImport = tableToImport.substring(0, tableToImport.length() - 4);
					}
					if (tableToImport.contains(".")) {
						tableToImport = tableToImport.substring(0, tableToImport.lastIndexOf("."));
					}
				}

				signalUnlimitedSubProgress();

				subWorker = dbImportDefinition.getConfiguredWorker(this, false, tableToImport, fileToImport.getAbsolutePath());

				// prevent multiple constraint deactivation
				subWorker.setDeactivateForeignKeyConstraints(false);
				subWorker.setDeactivateTriggers(false);

				signalItemStart(tableToImport + " (" + fileToImport.getName() + ") " + (itemsDone + 1) + "/" + itemsToDo, subWorker.getConfigurationLogString());

				subWorker.run();

				importedDataSize += subWorker.getImportedDataAmount();

				final String tableImportShortResult = tableToImport + " (" + fileToImport.getName() + ", " + Utilities.getHumanReadableNumber(subWorker.getImportedDataAmount(), "Byte", false, 5, false, Locale.getDefault()) + ")";
				if (subWorker.getError() != null) {
					multiImportHadError = true;
					multiImportResult.append(tableImportShortResult + ": ERROR (" + subWorker.getError().getMessage().replace("\n", "") + ")\n");
				} else {
					multiImportResult.append(tableImportShortResult + ": OK\n");
				}

				subWorker = null;

				itemsDone = fileIndex + 1;
				signalProgress(true);
			}

			itemsDone = itemsToDo;
			signalProgress(true);

			final LocalDateTime endDate = LocalDateTime.now();

			multiImportResult.append("End: " + DateUtilities.formatDate(DateUtilities.YYYY_MM_DD_HHMMSS, endDate) + "\n");
			multiImportResult.append("Time elapsed: " + DateUtilities.getHumanReadableTimespan(Duration.between(startDate, endDate), true) + "\n");
			multiImportResult.append("Imported dataamount: " + Utilities.getHumanReadableNumber(importedDataSize, "Byte", false, 5, false, Locale.getDefault()) + "\n");

			if (multiImportHadError) {
				return false;
			} else if (cancel) {
				return false;
			} else {
				return true;
			}
		} catch (final Exception e) {
			e.printStackTrace();
			return false;
		} finally {
			if (dbImportDefinition.isDeactivateForeignKeyConstraints() && constraintsWereDeactivated) {
				parent.changeTitle(LangResources.get("reactivateForeignKeyConstraints"));
				try (Connection connection = getDatabaseConnection(dbImportDefinition)) {
					DbUtilities.setForeignKeyConstraintStatus(dbImportDefinition.getDbVendor(), connection, true);
					if (!connection.getAutoCommit()) {
						connection.commit();
					}
				} catch (final Exception e) {
					System.err.println("Cannot reactivate foreign key constraints");
					e.printStackTrace();
				}
			}
			if (dbImportDefinition.isDeactivateTriggers() && triggersWereDeactivated) {
				parent.changeTitle(LangResources.get("reactivateTriggers"));
				try (Connection connection = getDatabaseConnection(dbImportDefinition)) {
					DbUtilities.setTriggerStatus(dbImportDefinition.getDbVendor(), connection, true);
					if (!connection.getAutoCommit()) {
						connection.commit();
					}
				} catch (final Exception e) {
					System.err.println("Cannot reactivate triggers");
					e.printStackTrace();
				}
			}
		}
	}

	private static Connection getDatabaseConnection(final DbImportDefinition dbImportDefinitionParam) throws Exception {
		if (dbImportDefinitionParam.getDbVendor() == DbVendor.Derby || (dbImportDefinitionParam.getDbVendor() == DbVendor.HSQL && Utilities.isBlank(dbImportDefinitionParam.getHostnameAndPort())) || dbImportDefinitionParam.getDbVendor() == DbVendor.SQLite) {
			try {
				return DbUtilities.createConnection(dbImportDefinitionParam, true);
			} catch (final DbNotExistsException e) {
				return DbUtilities.createNewDatabase(dbImportDefinitionParam.getDbVendor(), dbImportDefinitionParam.getDbName());
			}
		} else {
			return DbUtilities.createConnection(dbImportDefinitionParam, false);
		}
	}

	@Override
	public boolean cancel() {
		if (subWorker != null) {
			subWorker.cancel();
		}
		return super.cancel();
	}

	public String getResult() {
		return multiImportResult.toString();
	}

	public boolean wasErrorneous() {
		return multiImportHadError;
	}

	@Override
	public void receiveUnlimitedProgressSignal() {
		signalUnlimitedSubProgress();
	}

	@Override
	public void receiveProgressSignal(final LocalDateTime start, final long itemsToDoParameter, final long itemsDoneParameter, final String itemsUnitSignParameter) {
		// Progress of subWorker must be display of subItem progress
		subItemsToDo = itemsToDoParameter;
		subItemsUnitSign = itemsUnitSignParameter;
		subItemsDone = itemsDoneParameter;
		signalItemProgress();
	}

	@Override
	public void receiveDoneSignal(final LocalDateTime start, final LocalDateTime end, final long itemsDoneParameter, final String itemsUnitSignParameter, final String resultText) {
		// Progress of subWorker must be display of subItem progress
		subItemsDone = itemsDoneParameter;
		signalItemProgress();
		signalItemDone(resultText);
	}

	@Override
	public void changeTitle(final String text) {
		if (parent != null) {
			parent.changeTitle(text);
		}
	}
}
