package de.soderer.utilities.json.schema;

public class JsonSchemaDefinitionError extends Exception {
	private static final long serialVersionUID = 571902904309032324L;
	
	private String jsonSchemaPath;
	
	public JsonSchemaDefinitionError(String message, String jsonSchemaPath) {
		super(message);
		
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
