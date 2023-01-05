package de.soderer.utilities.json.schema;

public class JsonSchemaDataValidationError extends Exception {
	private static final long serialVersionUID = -4849599671599546633L;

	private final String jsonDataPath;

	public JsonSchemaDataValidationError(final String message, final String jsonDataPath) {
		super(message);

		this.jsonDataPath = jsonDataPath;
	}

	public JsonSchemaDataValidationError(final String message, final String jsonDataPath, final Exception e) {
		super(message, e);

		this.jsonDataPath = jsonDataPath;
	}

	public String getJsonDataPath() {
		return jsonDataPath;
	}

	@Override
	public String getMessage() {
		return "Invalid JSON data: " + super.getMessage() + (jsonDataPath == null ? "" : " at JSON path: " + jsonDataPath);
	}
}
