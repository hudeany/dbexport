package de.soderer.utilities.json.schema.validator;

import java.util.List;

import de.soderer.utilities.json.JsonNode;
import de.soderer.utilities.json.JsonObject;
import de.soderer.utilities.json.schema.JsonSchema;
import de.soderer.utilities.json.schema.JsonSchemaDataValidationError;
import de.soderer.utilities.json.schema.JsonSchemaDefinitionError;
import de.soderer.utilities.json.schema.JsonSchemaDependencyResolver;

public class ReferenceValidator extends BaseJsonSchemaValidator {
	private JsonObject dereferencedSchemaObject;
	
    public ReferenceValidator(JsonSchemaDependencyResolver jsonSchemaDependencyResolver, String jsonSchemaPath, Object validatorData, JsonNode jsonNode, String jsonPath) throws JsonSchemaDefinitionError {
    	super(jsonSchemaDependencyResolver, jsonSchemaPath, validatorData, jsonNode, jsonPath);
    	
    	try {
			if (validatorData == null) {
				throw new JsonSchemaDefinitionError("Reference key is 'null'", jsonSchemaPath);
			} else if (!(validatorData instanceof String)) {
				throw new JsonSchemaDefinitionError("Reference key is not a 'string'", jsonSchemaPath);
			} else if (jsonSchemaDependencyResolver == null) {
				throw new JsonSchemaDefinitionError("JSON schema reference definitions is empty. Cannot dereference key '" + validatorData + "'", jsonSchemaPath);
			} else {
				jsonSchemaDependencyResolver.checkCyclicDependency(jsonPath, (String) validatorData, jsonSchemaPath);
				
				Object dereferencedValue = jsonSchemaDependencyResolver.getDependencyByReference((String) validatorData, jsonSchemaPath);
				if (dereferencedValue == null) {
					throw new JsonSchemaDefinitionError("Invalid JSON schema reference data type for key '" + validatorData + "'. Expected 'object' but was 'null'", jsonSchemaPath);
				} else if (!(dereferencedValue instanceof JsonObject)) {
					throw new JsonSchemaDefinitionError("Invalid JSON schema reference data type for key '" + validatorData + "'. Expected 'object' but was '" + (dereferencedValue == null ? "null" : dereferencedValue.getClass().getSimpleName()) + "'", jsonSchemaPath);
				} else {
					dereferencedSchemaObject = (JsonObject) dereferencedValue;
					this.jsonSchemaPath = ((String) validatorData);
				}
			}
		} catch (JsonSchemaDefinitionError e) {
			throw e;
		} catch (Exception e) {
			throw new JsonSchemaDefinitionError("Error '" + e.getClass().getSimpleName() + "' while resolving JSON schema reference '" + validatorData + "': " + e.getMessage(), jsonSchemaPath);
		}
    }
	
	@Override
	public void validate() throws JsonSchemaDefinitionError, JsonSchemaDataValidationError {
		if (dereferencedSchemaObject == null) {
			throw new JsonSchemaDefinitionError("Invalid JSON schema reference data type for key '" + validatorData + "'. Expected 'object' but was 'null'", jsonSchemaPath);
		}
		
		List<BaseJsonSchemaValidator> subValidators = JsonSchema.createValidators(dereferencedSchemaObject, jsonSchemaDependencyResolver, jsonSchemaPath, jsonNode, jsonPath);
		for (BaseJsonSchemaValidator subValidator : subValidators) {
			subValidator.validate();
		}
    }
}
