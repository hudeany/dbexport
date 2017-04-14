package de.soderer.utilities.json.schema.validator;

import java.util.List;
import java.util.Map.Entry;

import de.soderer.utilities.json.JsonNode;
import de.soderer.utilities.json.JsonObject;
import de.soderer.utilities.json.schema.JsonSchema;
import de.soderer.utilities.json.schema.JsonSchemaDataValidationError;
import de.soderer.utilities.json.schema.JsonSchemaDefinitionError;
import de.soderer.utilities.json.schema.JsonSchemaDependencyResolver;

public class PropertiesValidator extends BaseJsonSchemaValidator {
	public PropertiesValidator(JsonSchemaDependencyResolver jsonSchemaDependencyResolver, String jsonSchemaPath, Object validatorData, JsonNode jsonNode, String jsonPath) throws JsonSchemaDefinitionError {
		super(jsonSchemaDependencyResolver, jsonSchemaPath, validatorData, jsonNode, jsonPath);
		
		if (!(validatorData instanceof JsonObject)) {
			throw new JsonSchemaDefinitionError("Properties data is not a JsonObject", jsonSchemaPath);
    	}
	}
	
	@Override
	public void validate() throws JsonSchemaDefinitionError, JsonSchemaDataValidationError {
		if (!(jsonNode.isJsonObject())) {
			if (!jsonSchemaDependencyResolver.isUseDraftV4Mode()) {
				throw new JsonSchemaDataValidationError("Expected data type 'object' but was '" + jsonNode.getJsonDataType().getName() + "'", jsonPath);
			}
		} else {
			for (Entry<String, Object> entry : ((JsonObject) validatorData).entrySet()) {
				if (!(entry.getValue() instanceof JsonObject)) {
					throw new JsonSchemaDefinitionError("Properties data is not a JsonObject", jsonSchemaPath);
		    	} else {
		    		if (((JsonObject) jsonNode.getValue()).containsPropertyKey(entry.getKey())) {
						JsonNode newJsonNode;
						try {
							newJsonNode = new JsonNode(((JsonObject) jsonNode.getValue()).get(entry.getKey()));
						} catch (Exception e) {
							throw new JsonSchemaDataValidationError("Invalid data type '" + ((JsonObject) jsonNode.getValue()).get(entry.getKey()).getClass().getSimpleName() + "'", jsonPath + "." + entry.getKey());
						}
						List<BaseJsonSchemaValidator> subValidators = JsonSchema.createValidators(((JsonObject) entry.getValue()), jsonSchemaDependencyResolver, jsonSchemaPath + "." + entry.getKey(), newJsonNode, jsonPath + "." + entry.getKey());
						for (BaseJsonSchemaValidator subValidator : subValidators) {
							subValidator.validate();
						}
					}
		    	}
			}
		}
    }
}
