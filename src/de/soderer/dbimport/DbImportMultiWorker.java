package de.soderer.dbimport;

import java.io.File;
import java.sql.Connection;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Locale;

import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.DbNotExistsException;
import de.soderer.utilities.DbUtilities;
import de.soderer.utilities.DbUtilities.DbVendor;
import de.soderer.utilities.LangResources;
import de.soderer.utilities.Utilities;
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
		boolean constraintWereDeactivated = false;
		try {
			showUnlimitedProgress();
			itemsToDo = filesToImport.size();

			multiImportResult = new StringBuilder();
			multiImportHadError = false;
			final LocalDateTime startDate = LocalDateTime.now();
			long importedDataSize = 0l;

			multiImportResult.append(LangResources.get("start") + ": " + DateUtilities.formatDate(DateUtilities.YYYY_MM_DD_HHMMSS, startDate) + "\n");
			itemsDone = 0;
			showProgress();

			if (dbImportDefinition.isDeactivateForeignKeyConstraints()) {
				showItemStart("Deactivating foreign key constrains");
				constraintWereDeactivated = true;
				try (Connection connection = getDatabaseConnection(dbImportDefinition)) {
					DbUtilities.setForeignKeyConstraintStatus(dbImportDefinition.getDbVendor(), connection, false);
					if (!connection.getAutoCommit()) {
						connection.commit();
					}
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

				showItemStart(tableToImport + " (" + fileToImport.getName() + ")");
				showItemProgress(true);

				showUnlimitedSubProgress();

				subWorker = dbImportDefinition.getConfiguredWorker(this, false, tableToImport, fileToImport.getAbsolutePath());

				// prevent multiple constraint deactivation
				subWorker.setDeactivateForeignKeyConstraints(false);

				subWorker.run();

				importedDataSize += subWorker.getImportedDataAmount();

				final String tableImportShortResult = tableToImport + " (" + fileToImport.getName() + ", " +  Utilities.getHumanReadableNumber(subWorker.getImportedDataAmount(), "Byte", false, 5, false, Locale.getDefault()) + ")";
				if (subWorker.getError() != null) {
					multiImportHadError = true;
					multiImportResult.append(tableImportShortResult + ": ERROR (" + subWorker.getError().getMessage() + ")\n");
				} else {
					multiImportResult.append(tableImportShortResult + ": OK\n");
				}

				subWorker = null;

				itemsDone = fileIndex + 1;
				showProgress(true);

				showItemDone();
			}

			itemsDone = itemsToDo;
			showProgress(true);

			final LocalDateTime endDate = LocalDateTime.now();

			multiImportResult.append(LangResources.get("end") + ": " + DateUtilities.formatDate(DateUtilities.YYYY_MM_DD_HHMMSS, endDate) + "\n");
			multiImportResult.append(LangResources.get("timeelapsed") + ": " + DateUtilities.getHumanReadableTimespan(Duration.between(startDate, endDate), true) + "\n");
			multiImportResult.append(LangResources.get("importeddataamount") + ": " + Utilities.getHumanReadableNumber(importedDataSize, "Byte", false, 5, false, Locale.getDefault()) + "\n");

			showDone(startDate, endDate, itemsDone);

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
			if (dbImportDefinition.isDeactivateForeignKeyConstraints() && constraintWereDeactivated) {
				showItemStart("Reactivating foreign key constrains");
				try (Connection connection = getDatabaseConnection(dbImportDefinition)) {
					DbUtilities.setForeignKeyConstraintStatus(dbImportDefinition.getDbVendor(), connection, true);
					if (!connection.getAutoCommit()) {
						connection.commit();
					}
				}
			}
		}
	}

	private static Connection getDatabaseConnection(final DbImportDefinition dbImportDefinition) throws Exception {
		final DbVendor dbVendor = dbImportDefinition.getDbVendor();
		final String hostname = dbImportDefinition.getHostname();
		final String dbName = dbImportDefinition.getDbName();
		final String username = dbImportDefinition.getUsername();
		final char[] password = dbImportDefinition.getPassword();
		final boolean secureConnection = dbImportDefinition.getSecureConnection();
		final String trustStoreFilePath = dbImportDefinition.getTrustStoreFilePath();
		final char[] trustStorePassword = dbImportDefinition.getTrustStorePassword();
		if (dbVendor == DbVendor.Derby || (dbVendor == DbVendor.HSQL && Utilities.isBlank(hostname)) || dbVendor == DbVendor.SQLite) {
			try {
				return DbUtilities.createConnection(dbVendor, hostname, dbName, username, (password == null ? null : password), false, null, null, true);
			} catch (@SuppressWarnings("unused") final DbNotExistsException e) {
				return DbUtilities.createNewDatabase(dbVendor, dbName);
			}
		} else {
			return DbUtilities.createConnection(dbVendor, hostname, dbName, username, (password == null ? null : password), secureConnection, Utilities.isNotBlank(trustStoreFilePath) ? new File(trustStoreFilePath) : null, trustStorePassword, false);
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
	public void showUnlimitedProgress() {
		// Progress of subWorker must be display of subItem progress
		showUnlimitedSubProgress();
	}

	@Override
	public void showProgress(final LocalDateTime start, final long itemsToDoParameter, final long itemsDoneParameter) {
		// Progress of subWorker must be display of subItem progress
		subItemsToDo = itemsToDoParameter;
		subItemsDone = itemsDoneParameter;
		showItemProgress();
	}

	@Override
	public void showDone(final LocalDateTime start, final LocalDateTime end, final long itemsDoneParameter) {
		// Progress of subWorker must be display of subItem progress
		subItemsDone = itemsDoneParameter;
		showItemProgress();
		showItemDone();
	}

	@Override
	public void changeTitle(final String text) {
		if (parent != null) {
			parent.changeTitle(text);
		}
	}
}
