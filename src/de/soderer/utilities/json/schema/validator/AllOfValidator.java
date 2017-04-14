package de.soderer.utilities.json.schema.validator;

import java.util.ArrayList;
import java.util.List;

import de.soderer.utilities.json.JsonArray;
import de.soderer.utilities.json.JsonNode;
import de.soderer.utilities.json.JsonObject;
import de.soderer.utilities.json.schema.JsonSchema;
import de.soderer.utilities.json.schema.JsonSchemaDataValidationError;
import de.soderer.utilities.json.schema.JsonSchemaDefinitionError;
import de.soderer.utilities.json.schema.JsonSchemaDependencyResolver;

public class AllOfValidator extends BaseJsonSchemaValidator {
	private List<List<BaseJsonSchemaValidator>> subValidatorPackages = null;
	
    public AllOfValidator(JsonSchemaDependencyResolver jsonSchemaDependencyResolver, String jsonSchemaPath, Object validatorData, JsonNode jsonNode, String jsonPath) throws JsonSchemaDefinitionError {
    	super(jsonSchemaDependencyResolver, jsonSchemaPath, validatorData, jsonNode, jsonPath);
    	
    	if (validatorData == null) {
    		throw new JsonSchemaDefinitionError("AllOf array is 'null'", jsonSchemaPath);
    	} else if (validatorData instanceof JsonArray) {
    		subValidatorPackages = new ArrayList<List<BaseJsonSchemaValidator>>();
    		for (int i = 0; i < ((JsonArray) validatorData).size(); i++) {
    			Object subValidationData = ((JsonArray) validatorData).get(i);
    			if (subValidationData instanceof JsonObject) {
    				subValidatorPackages.add(JsonSchema.createValidators((JsonObject) subValidationData, jsonSchemaDependencyResolver, jsonSchemaPath, jsonNode, jsonPath));
    			} else {
    				throw new JsonSchemaDefinitionError("AllOf array contains a non-JsonObject", jsonSchemaPath);
    			}
    		}
    		if (subValidatorPackages == null || subValidatorPackages.size() == 0) {
    			throw new JsonSchemaDefinitionError("AllOf array is empty", jsonSchemaPath);
    		}
    	} else {
    		throw new JsonSchemaDefinitionError("AllOf property does not have an array value", jsonSchemaPath);
    	}
    }

	@Override
	public void validate() throws JsonSchemaDefinitionError, JsonSchemaDataValidationError {
		for (List<BaseJsonSchemaValidator> subValidatorPackage : subValidatorPackages) {
			try {
				for (BaseJsonSchemaValidator subValidator : subValidatorPackage) {
					subValidator.validate();
				}
			} catch (JsonSchemaDataValidationError e) {
				throw new JsonSchemaDataValidationError("Some option of 'allOf' property did not apply to JsonNode", jsonPath);
			}
		}
	}
}
