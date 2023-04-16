package de.soderer.utilities.db;

public class DbNotExistsException extends Exception {
	private static final long serialVersionUID = -3769530565881724303L;

	public DbNotExistsException() {
		super();
	}

	public DbNotExistsException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public DbNotExistsException(String message, Throwable cause) {
		super(message, cause);
	}

	public DbNotExistsException(String message) {
		super(message);
	}

	public DbNotExistsException(Throwable cause) {
		super(cause);
	}
}
