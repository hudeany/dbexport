package de.soderer.utilities.json.schema;

import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import de.soderer.utilities.Utilities;
import de.soderer.utilities.json.Json5Reader;
import de.soderer.utilities.json.JsonArray;
import de.soderer.utilities.json.JsonNode;
import de.soderer.utilities.json.JsonObject;
import de.soderer.utilities.json.JsonPath;

public class JsonSchemaDependencyResolver {
	private JsonObject schemaDocumentNode = null;
	private Map<String, JsonObject> additionalSchemaDocumentNodes = new HashMap<String, JsonObject>();
	
	/**
	 * Draft V4 mode is NOT default mode<br />
	 * <br />
	 * The default mode uses a slightly more strict JSON schema definition.<br />
	 * This is useful in detection of schema definition errors.<br />
	 * Nontheless you can switch to the Draf V4 standard behaviour<br />
	 */
	private boolean useDraftV4Mode = false;
	
	private boolean downloadReferencedSchemas = false;

	private String latestJsonPath = null;
	private Set<String> latestDependencies;
	
	public JsonSchemaDependencyResolver(JsonObject schemaDocumentNode) throws JsonSchemaDefinitionError {
		if (schemaDocumentNode == null) {
			throw new JsonSchemaDefinitionError("Invalid data type 'null' for JsonSchemaDependencyResolver", "$");
		}
		this.schemaDocumentNode = schemaDocumentNode;
	}

	public Object getDependencyByReference(String reference, String jsonSchemaPath) throws Exception {
		if (reference != null) {
			if (!reference.contains("#")) {
				// Dereference simple reference without '#'
				if (schemaDocumentNode.get("definitions") != null && schemaDocumentNode.get("definitions") instanceof JsonObject && ((JsonObject) schemaDocumentNode.get("definitions")).containsPropertyKey(reference)) {
					return ((JsonObject) schemaDocumentNode.get("definitions")).get(reference);
				} else {
					for (JsonObject indirectJsonDefinitions : additionalSchemaDocumentNodes.values()) {
						if (indirectJsonDefinitions.get("definitions") != null && indirectJsonDefinitions.get("definitions") instanceof JsonObject && ((JsonObject) indirectJsonDefinitions.get("definitions")).containsPropertyKey(reference)) {
							return ((JsonObject) indirectJsonDefinitions.get("definitions")).get(reference);
						}
					}
					throw new Exception("Invalid JSON schema reference key '" + reference + "' or reference key not found. Use simple reference keys or this pattern for reference keys: '<referenced packagename or empty>#/definitions/<your reference key>'");
				}
			} else if (reference.startsWith("#")) {
				// Dereference local document reference
				JsonPath jsonPath = new JsonPath(reference);
				JsonObject referencedObject = schemaDocumentNode;
				for (Object referencePathPartObject : jsonPath.getPathParts()) {
					if (!(referencePathPartObject instanceof String)) {
						throw new JsonSchemaDefinitionError("Invalid JSON reference path contains array index'" + reference + "'", jsonSchemaPath);
					}
					String referencePathPart = (String) referencePathPartObject;
					if (!referencedObject.containsPropertyKey(referencePathPart)) {
						throw new JsonSchemaDefinitionError("JsonSchema does not contain the reference path '" + reference + "'", jsonSchemaPath);
					} else if (referencedObject.get(referencePathPart) == null) {
						throw new JsonSchemaDefinitionError("Invalid data type 'null' for reference path '" + reference + "'", jsonSchemaPath);
					} else if (!(referencedObject.get(referencePathPart) instanceof JsonObject)) {
						throw new JsonSchemaDefinitionError("Invalid data type '" + schemaDocumentNode.get("definitions").getClass().getSimpleName() + "' for reference path '" + reference + "'", jsonSchemaPath);
					} else {
						referencedObject = (JsonObject) referencedObject.get(referencePathPart);
					}
				}
				return referencedObject;
			} else {
				// Dereference other document reference
				String packageName = reference.substring(0, reference.lastIndexOf("#"));
				
				if (!additionalSchemaDocumentNodes.containsKey(packageName) && packageName != null && packageName.toLowerCase().startsWith("http") && downloadReferencedSchemas) {
					URLConnection urlConnection = new URL(packageName).openConnection();
					try (InputStream jsonSchemaInputStream = urlConnection.getInputStream()) {
						addJsonSchemaDefinition(packageName, jsonSchemaInputStream);
					}
				}
						
				if (!additionalSchemaDocumentNodes.containsKey(packageName)) {
					throw new Exception("Unknown JSON schema reference package name '" + packageName + "'");
				} else if (additionalSchemaDocumentNodes.get(packageName) == null) {
					throw new Exception("Invalid empty JSON schema reference for package name '" + packageName + "'");
				} else {
					JsonObject referencedObject = additionalSchemaDocumentNodes.get(packageName);
					JsonPath jsonPath = new JsonPath(reference.substring(reference.lastIndexOf("#")));
					for (Object referencePathPartObject : jsonPath.getPathParts()) {
						if (!(referencePathPartObject instanceof String)) {
							throw new JsonSchemaDefinitionError("Invalid JSON reference path contains array index'" + reference + "'", jsonSchemaPath);
						}
						String referencePathPart = (String) referencePathPartObject;
						if (!referencedObject.containsPropertyKey(referencePathPart)) {
							throw new JsonSchemaDefinitionError("Referenced JsonSchema does not contain the reference path '" + reference + "'", jsonSchemaPath);
						} else if (referencedObject.get(referencePathPart) == null) {
							throw new JsonSchemaDefinitionError("Invalid data type 'null' for reference path '" + reference + "'", jsonSchemaPath);
						} else if (!(referencedObject.get(referencePathPart) instanceof JsonObject)) {
							throw new JsonSchemaDefinitionError("Invalid data type '" + schemaDocumentNode.get("definitions").getClass().getSimpleName() + "' for reference path '" + reference + "'", jsonSchemaPath);
						} else {
							referencedObject = (JsonObject) referencedObject.get(referencePathPart);
						}
					}
					return referencedObject;
				}
			}
		} else {
			throw new Exception("Invalid JSON schema reference key 'null'");
		}
	}

	public void addJsonSchemaDefinition(String definitionPackageName, InputStream jsonSchemaInputStream) throws Exception {
		if (Utilities.isBlank(definitionPackageName)) {
			throw new Exception("Invalid empty JSON schema definition package name");
		} else if (additionalSchemaDocumentNodes.containsKey(definitionPackageName)) {
			throw new Exception("Additional JSON schema definition package '" + definitionPackageName + "' was already added before");
		} else {
			try (Json5Reader reader = new Json5Reader(jsonSchemaInputStream)) {
				JsonNode jsonNode = reader.read();
				if (!jsonNode.isJsonObject()) {
					throw new Exception("Additional JSON schema definition package '" + definitionPackageName + "' does not contain JSON schema data of type 'object'");
				} else {
					JsonObject jsonSchema = (JsonObject) jsonNode.getValue();
					redirectReferences(jsonSchema, "#", definitionPackageName + "#");
					additionalSchemaDocumentNodes.put(definitionPackageName, jsonSchema);
				}
			}
		}
	}

	private void redirectReferences(JsonObject jsonObject, String referenceDefinitionStart, String referenceDefinitionReplacement) {
		for (Entry<String, Object> entry : jsonObject.entrySet()) {
			if ("$ref".equals(entry.getKey()) && entry.getValue() != null && entry.getValue() instanceof String && ((String) entry.getValue()).startsWith(referenceDefinitionStart)) {
				jsonObject.add("$ref", referenceDefinitionReplacement + ((String) entry.getValue()).substring(referenceDefinitionStart.length()));
			} else if (entry.getValue() instanceof JsonObject) {
				redirectReferences((JsonObject) entry.getValue(), referenceDefinitionStart, referenceDefinitionReplacement);
			} else if (entry.getValue() instanceof JsonArray) {
				redirectReferences((JsonArray) entry.getValue(), referenceDefinitionStart, referenceDefinitionReplacement);
			}
		}
	}

	private void redirectReferences(JsonArray jsonArray, String referenceDefinitionStart, String referenceDefinitionReplacement) {
		for (Object item : jsonArray) {
			if (item instanceof JsonObject) {
				redirectReferences((JsonObject) item, referenceDefinitionStart, referenceDefinitionReplacement);
			} else if (item instanceof JsonArray) {
				redirectReferences((JsonArray) item, referenceDefinitionStart, referenceDefinitionReplacement);
			}
		}
	}

	public void checkCyclicDependency(String jsonPath, String validatorData, String jsonSchemaPath) throws JsonSchemaDefinitionError {
		if (latestJsonPath == null || !latestJsonPath.equals(jsonPath)) {
			latestJsonPath = jsonPath;
			latestDependencies = new HashSet<String>();
		}
		if (latestDependencies.contains((String) validatorData)) {
			throw new JsonSchemaDefinitionError("Cyclic dependency detected: '" + Utilities.join(latestDependencies, "', ") + "'", jsonSchemaPath);
		} else{
			latestDependencies.add((String) validatorData);
		}
	}
	
	public void setUseDraftV4Mode(boolean useDraftV4Mode) {
		this.useDraftV4Mode = useDraftV4Mode;
	}

	public boolean isUseDraftV4Mode() {
		return useDraftV4Mode;
	}

	public void setDownloadReferencedSchemas(boolean downloadReferencedSchemas) {
		this.downloadReferencedSchemas = downloadReferencedSchemas;
	}
}
