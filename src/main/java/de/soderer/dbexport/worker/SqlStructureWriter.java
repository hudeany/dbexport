package de.soderer.dbexport.worker;

import java.io.Writer;
import java.util.List;

/**
 * Writes the structure of database tables as SQL DDL statements, without data.
 *
 * @see DbStructureSqlWriter generic implementation via JDBC metadata
 * @see PostgreSqlStructureWriter PostgreSQL implementation in the format of pg_dump
 */
public interface SqlStructureWriter {
	/**
	 * Writes the complete structure dump of the tables.
	 *
	 * @param tableNames names of the tables, optionally qualified by their schema
	 * @param writer output, not closed
	 * @param creatorText text about the creator for the header, e.g. "DbExport 1.2.3", may be null
	 * @param progressListener notified after each table and asked for cancellation
	 * @throws Exception if the structure cannot be read or written
	 */
	void writeStructure(List<String> tableNames, Writer writer, String creatorText, ProgressListener progressListener) throws Exception;

	/**
	 * Progress of a structure export.
	 */
	interface ProgressListener {
		/**
		 * Called after the structure of a table was written.
		 */
		void tableDone();

		/**
		 * Checks whether the export was canceled.
		 *
		 * @return true to stop the export
		 */
		boolean isCanceled();
	}
}
