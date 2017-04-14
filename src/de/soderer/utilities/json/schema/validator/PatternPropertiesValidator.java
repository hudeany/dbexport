package de.soderer.utilities.json.schema.validator;

import java.util.List;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import de.soderer.utilities.json.JsonNode;
import de.soderer.utilities.json.JsonObject;
import de.soderer.utilities.json.schema.JsonSchema;
import de.soderer.utilities.json.schema.JsonSchemaDataValidationError;
import de.soderer.utilities.json.schema.JsonSchemaDefinitionError;
import de.soderer.utilities.json.schema.JsonSchemaDependencyResolver;

public class PatternPropertiesValidator extends BaseJsonSchemaValidator {
    public PatternPropertiesValidator(JsonSchemaDependencyResolver jsonSchemaDependencyResolver, String jsonSchemaPath, Object validatorData, JsonNode jsonNode, String jsonPath) throws JsonSchemaDefinitionError {
    	super(jsonSchemaDependencyResolver, jsonSchemaPath, validatorData, jsonNode, jsonPath);
    	
    	if (validatorData == null) {
    		throw new JsonSchemaDefinitionError("PatternProperties data is 'null'", jsonSchemaPath);
    	} else if (!(validatorData instanceof JsonObject)) {
    		throw new JsonSchemaDefinitionError("PatternProperties data is not a JsonObject", jsonSchemaPath);
    	}
    }
	
	@Override
	public void validate() throws JsonSchemaDefinitionError, JsonSchemaDataValidationError {
		if (!(jsonNode.isJsonObject())) {
			if (!jsonSchemaDependencyResolver.isUseDraftV4Mode()) {
				throw new JsonSchemaDataValidationError("Expected data type 'object' but was '" + jsonNode.getJsonDataType().getName() + "'", jsonPath);
			}
		} else {
			for (Entry<String, Object> propertyEntry : ((JsonObject) jsonNode.getValue()).entrySet()) {
				for (Entry<String, Object> entry : ((JsonObject) validatorData).entrySet()) {
					if (entry.getValue() == null || !(entry.getValue() instanceof JsonObject)) {
						throw new JsonSchemaDefinitionError("PatternProperties data contains a non-JsonObject", jsonSchemaPath);
			    	} else {
						Pattern propertyKeyPattern;
						try {
							propertyKeyPattern = Pattern.compile(entry.getKey());
						} catch (Exception e1) {
							throw new JsonSchemaDefinitionError("PatternProperties data contains invalid RegEx pattern: " + entry.getKey(), jsonSchemaPath);
						}
						
						if (propertyKeyPattern.matcher(propertyEntry.getKey()).find()) {
							JsonNode jsonNode;
							try {
								jsonNode = new JsonNode(propertyEntry.getValue());
							} catch (Exception e) {
								throw new JsonSchemaDataValidationError("Invalid property data type was '" + propertyEntry.getValue().getClass().getSimpleName() + "'", jsonPath + "." + propertyEntry.getKey());
							}
							List<BaseJsonSchemaValidator> subValidators = JsonSchema.createValidators(((JsonObject) entry.getValue()), jsonSchemaDependencyResolver, jsonSchemaPath + "." + propertyEntry.getKey(), jsonNode, jsonPath + "." + propertyEntry.getKey());
							for (BaseJsonSchemaValidator subValidator : subValidators) {
								subValidator.validate();
							}
						}
			    	}
				}
			}
		}
    }
}
