package de.soderer.dbcsvimport;

/**
 * The Class DbCsvImportException.
 */
public class DbCsvImportException extends Exception {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = 6039775378389122712L;

	/**
	 * Instantiates a new db csv import exception.
	 *
	 * @param errorMessage
	 *            the error message
	 */
	public DbCsvImportException(String errorMessage) {
		super(errorMessage);
	}
	
	public DbCsvImportException(String errorMessage, Exception e) {
		super(errorMessage, e);
	}
}
