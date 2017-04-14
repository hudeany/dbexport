package de.soderer.utilities.json.schema.validator;

import java.util.List;
import java.util.Map.Entry;

import de.soderer.utilities.json.JsonArray;
import de.soderer.utilities.json.JsonNode;
import de.soderer.utilities.json.JsonObject;
import de.soderer.utilities.json.schema.JsonSchema;
import de.soderer.utilities.json.schema.JsonSchemaDataValidationError;
import de.soderer.utilities.json.schema.JsonSchemaDefinitionError;
import de.soderer.utilities.json.schema.JsonSchemaDependencyResolver;

public class DependenciesValidator extends BaseJsonSchemaValidator {
    public DependenciesValidator(JsonSchemaDependencyResolver jsonSchemaDependencyResolver, String jsonSchemaPath, Object validatorData, JsonNode jsonNode, String jsonPath) throws JsonSchemaDefinitionError {
    	super(jsonSchemaDependencyResolver, jsonSchemaPath, validatorData, jsonNode, jsonPath);
    	
    	if (validatorData == null) {
    		throw new JsonSchemaDefinitionError("Dependencies value is 'null'", jsonSchemaPath);
    	} else if (!(validatorData instanceof JsonObject)) {
    		throw new JsonSchemaDefinitionError("Dependencies value is not an 'object'", jsonSchemaPath);
    	}
    }
	
	@Override
	public void validate() throws JsonSchemaDefinitionError, JsonSchemaDataValidationError {
		if (!jsonNode.isJsonObject()) {
			if (!jsonSchemaDependencyResolver.isUseDraftV4Mode()) {
				throw new JsonSchemaDataValidationError("Expected a 'object' value for dependency but was '" + jsonNode.getJsonDataType().getName() + "'", jsonPath);
			}
		} else {
			for (Entry<String, Object> entry : ((JsonObject) validatorData).entrySet()) {
				if (((JsonObject) jsonNode.getValue()).containsPropertyKey(entry.getKey())) {
					if (entry.getValue() == null) {
						throw new JsonSchemaDefinitionError("Dependencies value is 'null'", jsonSchemaPath);
					} else if (entry.getValue() instanceof JsonArray) {
						for (Object item : ((JsonArray) entry.getValue())) {
							if (item == null || !(item instanceof String)) {
								throw new JsonSchemaDefinitionError("Dependencies value for key '" + entry.getKey() + "' contains invalid data that is not 'string'", jsonSchemaPath);
							} else if (!((JsonObject) jsonNode.getValue()).containsPropertyKey((String) item)) {
								throw new JsonSchemaDataValidationError("Dependent property key '" + (String) item + "' for existing parent key '" + entry.getKey() + "' is missing", jsonPath);
							}
						}
					} else if (entry.getValue() instanceof JsonObject) {
						List<BaseJsonSchemaValidator> subValidators = JsonSchema.createValidators((JsonObject) entry.getValue(), jsonSchemaDependencyResolver, jsonSchemaPath, jsonNode, jsonPath);
						for (BaseJsonSchemaValidator validator : subValidators) {
							validator.validate();
						}
					} else if (entry.getValue() instanceof String) {
						if (!((JsonObject) jsonNode.getValue()).containsPropertyKey((String) entry.getValue())) {
							throw new JsonSchemaDataValidationError("Dependent property key '" + (String) entry.getValue() + "' for existing parent key '" + entry.getKey() + "' is missing", jsonPath);
						}
					} else {
						throw new JsonSchemaDefinitionError("Dependencies value for key '" + entry.getKey() + "' is not an 'object' or 'array' or 'string'", jsonSchemaPath);
					}
				}
			}
		}
    }
}
