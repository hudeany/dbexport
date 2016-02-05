package de.soderer.dbcsvexport;

/**
 * The Class DbCsvExportException.
 */
public class DbCsvExportException extends Exception {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = 6039775378389122712L;

	/**
	 * Instantiates a new db csv export exception.
	 *
	 * @param errorMessage
	 *            the error message
	 */
	public DbCsvExportException(String errorMessage) {
		super(errorMessage);
	}
}
