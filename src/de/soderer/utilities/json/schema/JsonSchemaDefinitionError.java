package de.soderer.utilities.json.schema;

public class JsonSchemaDefinitionError extends Exception {
	private static final long serialVersionUID = 571902904309032324L;

	private final String jsonSchemaPath;

	public JsonSchemaDefinitionError(final String message, final String jsonSchemaPath) {
		super(message);

		this.jsonSchemaPath = jsonSchemaPath;
	}

	public JsonSchemaDefinitionError(final String message, final String jsonSchemaPath, final Exception e) {
		super(message, e);

		this.jsonSchemaPath = jsonSchemaPath;
	}

	public String getJsonSchemaPath() {
		return jsonSchemaPath;
	}

	@Override
	public String getMessage() {
		return "Invalid JSON schema definition: " + super.getMessage() + (jsonSchemaPath == null ? "" : " at JSON schema path: " + jsonSchemaPath);
	}
}
