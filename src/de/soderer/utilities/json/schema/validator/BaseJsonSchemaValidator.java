package de.soderer.utilities.json.schema.validator;

import de.soderer.utilities.json.JsonNode;
import de.soderer.utilities.json.schema.JsonSchemaDataValidationError;
import de.soderer.utilities.json.schema.JsonSchemaDefinitionError;
import de.soderer.utilities.json.schema.JsonSchemaDependencyResolver;

public abstract class BaseJsonSchemaValidator {
	protected JsonSchemaDependencyResolver jsonSchemaDependencyResolver;
	protected String jsonSchemaPath;
	protected Object validatorData;
	protected JsonNode jsonNode;
	protected String jsonPath;
	
	protected BaseJsonSchemaValidator(JsonSchemaDependencyResolver jsonSchemaDependencyResolver, String jsonSchemaPath, Object validatorData, JsonNode jsonNode, String jsonPath) throws JsonSchemaDefinitionError {
		if (validatorData == null) {
			throw new JsonSchemaDefinitionError("ValidatorData is 'null'", jsonSchemaPath);
		}
		
		this.jsonSchemaDependencyResolver = jsonSchemaDependencyResolver;
		this.jsonSchemaPath = jsonSchemaPath;
		this.validatorData = validatorData;
		this.jsonNode = jsonNode;
		this.jsonPath = jsonPath;
	}

	public abstract void validate() throws JsonSchemaDefinitionError, JsonSchemaDataValidationError;
}
