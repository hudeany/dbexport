package de.soderer.utilities.json.schema.validator;

import de.soderer.utilities.json.JsonArray;
import de.soderer.utilities.json.JsonNode;
import de.soderer.utilities.json.JsonObject;
import de.soderer.utilities.json.schema.JsonSchemaDataValidationError;
import de.soderer.utilities.json.schema.JsonSchemaDefinitionError;
import de.soderer.utilities.json.schema.JsonSchemaDependencyResolver;

public class RequiredValidator extends BaseJsonSchemaValidator {
	public RequiredValidator(final JsonSchemaDependencyResolver jsonSchemaDependencyResolver, final String jsonSchemaPath, final Object validatorData, final JsonNode jsonNode, final String jsonPath) throws JsonSchemaDefinitionError {
		super(jsonSchemaDependencyResolver, jsonSchemaPath, validatorData, jsonNode, jsonPath);

		if (!(validatorData instanceof JsonArray)) {
			throw new JsonSchemaDefinitionError("Data for required property keys is not a JsonArray", jsonSchemaPath);
		}
	}

	@Override
	public void validate() throws JsonSchemaDefinitionError, JsonSchemaDataValidationError {
		if (!(jsonNode.isJsonObject())) {
			if (!jsonSchemaDependencyResolver.isUseDraftV4Mode()) {
				throw new JsonSchemaDataValidationError("Expected data type 'object' but was '" + jsonNode.getJsonDataType().getName() + "'", jsonPath);
			}
		} else {
			for (final Object propertyKey : (JsonArray) validatorData) {
				if (propertyKey == null) {
					throw new JsonSchemaDefinitionError("Data entry for required property key name must be 'string' but was 'null'", jsonSchemaPath);
				} else if (!(propertyKey instanceof String)) {
					throw new JsonSchemaDefinitionError("Data entry for required property key name must be 'string' but was '" + propertyKey.getClass().getSimpleName() + "'", jsonSchemaPath);
				} else if (!((JsonObject) jsonNode.getValue()).containsPropertyKey((String) propertyKey)) {
					throw new JsonSchemaDataValidationError("Invalid property key. Missing required property '" + propertyKey + "'", jsonPath);
				}
			}
		}
	}
}
