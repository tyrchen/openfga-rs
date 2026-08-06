import { CredentialsMethod, OpenFgaClient } from "@openfga/sdk";

const apiUrl = process.env.FGA_API_URL;
if (!apiUrl || !/^https?:\/\/(localhost|127\.0\.0\.1|\[::1\])(?::[0-9]{1,5})?\/?$/.test(apiUrl)) {
  throw new Error("FGA_API_URL must be a loopback HTTP(S) URL");
}

const token = process.env.FGA_API_TOKEN;
const credentials = token
  ? { method: CredentialsMethod.ApiToken, config: { token } }
  : undefined;
const authorizationHeaders = token ? { authorization: `Bearer ${token}` } : {};

async function wireError(name, method, path, body) {
  const response = await fetch(`${apiUrl.replace(/\/$/, "")}${path}`, {
    method,
    headers: {
      ...authorizationHeaders,
      ...(body === undefined ? {} : { "content-type": "application/json" }),
    },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  if (response.ok) {
    throw new Error(`${name} unexpectedly succeeded`);
  }
  return { name, status: response.status, body: await response.json() };
}
const bootstrapClient = new OpenFgaClient({ apiUrl, credentials });
const created = await bootstrapClient.createStore({ name: "phase2-sdk-primary" });
if (typeof created.id !== "string" || !/^[0-9A-HJKMNP-TV-Z]{26}$/.test(created.id)) {
  throw new Error("CreateStore returned an invalid store identifier");
}
const second = await bootstrapClient.createStore({ name: "phase2-sdk-secondary" });
const firstPage = await bootstrapClient.listStores({ pageSize: 1 });
if (firstPage.stores.length !== 1 || !firstPage.continuation_token) {
  throw new Error("ListStores did not return a bounded first page");
}
const secondPage = await bootstrapClient.listStores({
  pageSize: 1,
  continuationToken: firstPage.continuation_token,
});
if (secondPage.stores.length !== 1) {
  throw new Error("ListStores continuation did not return the second page");
}

const storeClient = new OpenFgaClient({ apiUrl, storeId: created.id, credentials });
const fetched = await storeClient.getStore();
if (fetched.id !== created.id || fetched.name !== "phase2-sdk-primary") {
  throw new Error("GetStore did not return the SDK-created store");
}

const model = await storeClient.writeAuthorizationModel({
  schema_version: "1.1",
  type_definitions: [
    { type: "user" },
    {
      type: "document",
      relations: { viewer: { this: {} } },
      metadata: {
        relations: {
          viewer: { directly_related_user_types: [{ type: "user" }] },
        },
      },
    },
  ],
});
const conditionModel = await storeClient.writeAuthorizationModel({
  schema_version: "1.1",
  type_definitions: [
    { type: "user" },
    {
      type: "document",
      relations: { viewer: { this: {} }, writer: { this: {} } },
      metadata: {
        relations: {
          viewer: {
            directly_related_user_types: [{ type: "user", condition: "cond1" }],
          },
          writer: { directly_related_user_types: [{ type: "user" }] },
        },
      },
    },
  ],
  conditions: {
    cond1: {
      name: "cond1",
      expression: "param1 == 'ok'",
      parameters: { param1: { type_name: "TYPE_NAME_STRING" } },
    },
  },
});
const client = new OpenFgaClient({
  apiUrl,
  storeId: created.id,
  authorizationModelId: model.authorization_model_id,
  credentials,
});
const readModel = await client.readAuthorizationModel();
if (readModel.authorization_model.id !== model.authorization_model_id) {
  throw new Error("ReadAuthorizationModel returned the wrong model");
}
const models = await client.readAuthorizationModels({ pageSize: 1 });
if (models.authorization_models.length !== 1) {
  throw new Error("ReadAuthorizationModels returned the wrong page");
}

const tuple = { user: "user:anne", relation: "viewer", object: "document:roadmap" };
await client.writeTuples([tuple]);
const tuples = await client.read({}, { pageSize: 1 });
if (tuples.tuples.length !== 1) {
  throw new Error("Read did not return the written tuple");
}
const check = await client.check(tuple);
if (check.allowed !== true) {
  throw new Error("Check did not allow the written relationship");
}
const batch = await client.batchCheck({
  checks: [
    { ...tuple, correlationId: "allowed" },
    {
      user: "user:bob",
      relation: "viewer",
      object: "document:roadmap",
      correlationId: "denied",
    },
  ],
});
if (batch.result.length !== 2 || !batch.result.some((item) => item.allowed === true)) {
  throw new Error("BatchCheck did not preserve correlated mixed results");
}

await client.writeAssertions([{ ...tuple, expectation: true }]);
const assertions = await client.readAssertions();
if (assertions.assertions.length !== 1) {
  throw new Error("ReadAssertions did not return the written assertion");
}
const changes = await client.readChanges(undefined, { pageSize: 1 });
if (changes.changes.length !== 1) {
  throw new Error("ReadChanges did not return the tuple mutation");
}
const excessiveAssertions = Array.from({ length: 101 }, () => ({
  tuple_key: tuple,
  expectation: true,
}));
const httpErrors = [];
httpErrors.push(await wireError("createStore", "POST", "/stores", {}));
httpErrors.push(await wireError(
  "listStores",
  "GET",
  `/stores?page_size=0&continuation_token=${"!".repeat(5121)}&name=x`,
));
httpErrors.push(await wireError("getStore", "GET", "/stores/short"));
httpErrors.push(await wireError("deleteStore", "DELETE", "/stores/short"));
httpErrors.push(await wireError(
  "writeAuthorizationModel",
  "POST",
  `/stores/${created.id}/authorization-models`,
  { schema_version: "invalid", type_definitions: [] },
));
httpErrors.push(await wireError(
  "readAuthorizationModel",
  "GET",
  `/stores/${created.id}/authorization-models/short`,
));
httpErrors.push(await wireError(
  "readAuthorizationModels",
  "GET",
  `/stores/${created.id}/authorization-models?page_size=0&continuation_token=!`,
));
httpErrors.push(await wireError(
  "writeAssertions",
  "PUT",
  `/stores/${created.id}/assertions/short`,
  { assertions: excessiveAssertions },
));
httpErrors.push(await wireError(
  "readAssertions",
  "GET",
  `/stores/${created.id}/assertions/short`,
));
httpErrors.push(await wireError("read", "POST", `/stores/${created.id}/read`, {
  tuple_key: { object: "x", relation: "#", user: "x" },
  page_size: 0,
  continuation_token: "!",
}));
httpErrors.push(await wireError("write", "POST", `/stores/${created.id}/write`, {
  authorization_model_id: "short",
  writes: { tuple_keys: [{ object: "x", relation: "#", user: "x" }] },
}));
httpErrors.push(await wireError("check", "POST", `/stores/${created.id}/check`, {
  tuple_key: { object: "x", relation: "#", user: "x" },
  authorization_model_id: "short",
}));
httpErrors.push(await wireError("batchCheck", "POST", `/stores/${created.id}/batch-check`, {
  checks: [{
    tuple_key: { object: "x", relation: "#", user: "x" },
    correlation_id: "!",
  }],
  authorization_model_id: model.authorization_model_id,
}));
httpErrors.push(await wireError(
  "readChanges",
  "GET",
  `/stores/${created.id}/changes?type=%23&page_size=0&continuation_token=!`,
));
httpErrors.push(await wireError("expand", "POST", `/stores/${created.id}/expand`, {
  authorization_model_id: "short",
}));
httpErrors.push(await wireError(
  "listObjects",
  "POST",
  `/stores/${created.id}/list-objects`,
  { type: "#", relation: "#", user: "x", authorization_model_id: "short" },
));
httpErrors.push(await wireError(
  "listUsers",
  "POST",
  `/stores/${created.id}/list-users`,
  {
    object: { type: "document", id: "roadmap" },
    relation: "viewer",
    user_filters: [{ type: "#", relation: "#" }],
    authorization_model_id: model.authorization_model_id,
  },
));
httpErrors.push(await wireError(
  "streamedListObjects",
  "POST",
  `/stores/${created.id}/streamed-list-objects`,
  { type: "#", relation: "#", user: "x", authorization_model_id: "short" },
));
const semanticErrors = [];
semanticErrors.push(await wireError(
  "duplicateModelTypes",
  "POST",
  `/stores/${created.id}/authorization-models`,
  {
    schema_version: "1.1",
    type_definitions: [{ type: "user" }, { type: "user" }],
  },
));
semanticErrors.push(await wireError(
  "undefinedModelRelation",
  "POST",
  `/stores/${created.id}/authorization-models`,
  {
    schema_version: "1.1",
    type_definitions: [
      { type: "user" },
      {
        type: "document",
        relations: { viewer: { computedUserset: { relation: "missing" } } },
      },
    ],
  },
));
semanticErrors.push(await wireError(
  "illegalSelfComputedRelation",
  "POST",
  `/stores/${created.id}/authorization-models`,
  {
    schema_version: "1.1",
    type_definitions: [
      { type: "user" },
      {
        type: "document",
        relations: { viewer: { computedUserset: { relation: "viewer" } } },
      },
    ],
  },
));
semanticErrors.push(await wireError(
  "undefinedDirectRestrictionType",
  "POST",
  `/stores/${created.id}/authorization-models`,
  {
    schema_version: "1.1",
    type_definitions: [
      { type: "user" },
      {
        type: "document",
        relations: { viewer: { this: {} } },
        metadata: {
          relations: {
            viewer: { directly_related_user_types: [{ type: "group" }] },
          },
        },
      },
    ],
  },
));
semanticErrors.push(await wireError(
  "computedRelationCycle",
  "POST",
  `/stores/${created.id}/authorization-models`,
  {
    schema_version: "1.1",
    type_definitions: [
      { type: "user" },
      {
        type: "other",
        relations: {
          x: { difference: { base: { this: {} }, subtract: { computedUserset: { relation: "y" } } } },
          y: { difference: { base: { this: {} }, subtract: { computedUserset: { relation: "z" } } } },
          z: { union: { child: [{ this: {} }, { computedUserset: { relation: "x" } }] } },
        },
        metadata: {
          relations: {
            x: { directly_related_user_types: [{ type: "user" }] },
            y: { directly_related_user_types: [{ type: "user" }] },
            z: { directly_related_user_types: [{ type: "user" }] },
          },
        },
      },
    ],
  },
));
semanticErrors.push(await wireError(
  "assignableRelationWithoutTypes",
  "POST",
  `/stores/${created.id}/authorization-models`,
  {
    schema_version: "1.1",
    type_definitions: [
      { type: "user" },
      { type: "document", relations: { viewer: { this: {} } } },
    ],
  },
));
semanticErrors.push(await wireError(
  "relationWithoutEntrypoints",
  "POST",
  `/stores/${created.id}/authorization-models`,
  {
    schema_version: "1.1",
    type_definitions: [{
      type: "document",
      relations: { viewer: { this: {} } },
      metadata: {
        relations: {
          viewer: { directly_related_user_types: [{ type: "document", relation: "viewer" }] },
        },
      },
    }],
  },
));
semanticErrors.push(await wireError(
  "undefinedRelationCondition",
  "POST",
  `/stores/${created.id}/authorization-models`,
  {
    schema_version: "1.1",
    type_definitions: [
      { type: "user" },
      {
        type: "document",
        relations: { viewer: { this: {} } },
        metadata: {
          relations: {
            viewer: {
              directly_related_user_types: [{ type: "user", condition: "missing" }],
            },
          },
        },
      },
    ],
  },
));
semanticErrors.push(await wireError(
  "conditionNameMismatch",
  "POST",
  `/stores/${created.id}/authorization-models`,
  {
    schema_version: "1.1",
    type_definitions: [{ type: "user" }],
    conditions: {
      condition2: { name: "condition3", expression: "true", parameters: {} },
    },
  },
));
semanticErrors.push(await wireError(
  "undefinedUsersetRestrictionRelation",
  "POST",
  `/stores/${created.id}/authorization-models`,
  {
    schema_version: "1.1",
    type_definitions: [
      { type: "user" },
      {
        type: "group",
        relations: { parent: { this: {} } },
        metadata: {
          relations: {
            parent: { directly_related_user_types: [{ type: "group", relation: "missing" }] },
          },
        },
      },
    ],
  },
));
semanticErrors.push(await wireError(
  "undefinedTupleToUsersetTarget",
  "POST",
  `/stores/${created.id}/authorization-models`,
  {
    schema_version: "1.1",
    type_definitions: [
      { type: "org" },
      {
        type: "document",
        relations: {
          parent: { this: {} },
          viewer: {
            tupleToUserset: {
              tupleset: { relation: "parent" },
              computedUserset: { relation: "viewer" },
            },
          },
        },
        metadata: {
          relations: {
            parent: { directly_related_user_types: [{ type: "org" }] },
          },
        },
      },
    ],
  },
));
semanticErrors.push(await wireError(
  "nonDirectTuplesetRelation",
  "POST",
  `/stores/${created.id}/authorization-models`,
  {
    schema_version: "1.1",
    type_definitions: [{
      type: "folder",
      relations: {
        root: { this: {} },
        parent: { union: { child: [{ this: {} }, { computedUserset: { relation: "root" } }] } },
        viewer: {
          tupleToUserset: {
            tupleset: { relation: "parent" },
            computedUserset: { relation: "viewer" },
          },
        },
      },
      metadata: {
        relations: {
          root: { directly_related_user_types: [{ type: "folder" }] },
          parent: { directly_related_user_types: [{ type: "folder" }] },
        },
      },
    }],
  },
));
semanticErrors.push(await wireError(
  "invalidTuplesetTypeRestriction",
  "POST",
  `/stores/${created.id}/authorization-models`,
  {
    schema_version: "1.1",
    type_definitions: [{
      type: "folder",
      relations: {
        parent: { this: {} },
        viewer: {
          tupleToUserset: {
            tupleset: { relation: "parent" },
            computedUserset: { relation: "viewer" },
          },
        },
      },
      metadata: {
        relations: {
          parent: {
            directly_related_user_types: [
              { type: "folder" },
              { type: "folder", relation: "parent" },
            ],
          },
        },
      },
    }],
  },
));
semanticErrors.push(await wireError("writeMissingRelation", "POST", `/stores/${created.id}/write`, {
  authorization_model_id: model.authorization_model_id,
  writes: {
    tuple_keys: [{
      object: "document:roadmap",
      relation: "editor",
      user: "user:anne",
    }],
  },
}));
semanticErrors.push(await wireError("checkMissingType", "POST", `/stores/${created.id}/check`, {
  tuple_key: { object: "unknown:roadmap", relation: "viewer", user: "user:anne" },
  authorization_model_id: model.authorization_model_id,
}));
semanticErrors.push(await wireError("checkMissingRelation", "POST", `/stores/${created.id}/check`, {
  tuple_key: { object: "document:roadmap", relation: "editor", user: "user:anne" },
  authorization_model_id: model.authorization_model_id,
}));
semanticErrors.push(await wireError("checkMissingSubjectType", "POST", `/stores/${created.id}/check`, {
  tuple_key: { object: "document:roadmap", relation: "viewer", user: "group:eng" },
  authorization_model_id: model.authorization_model_id,
}));
semanticErrors.push(await wireError("checkMissingUsersetRelation", "POST", `/stores/${created.id}/check`, {
  tuple_key: { object: "document:roadmap", relation: "viewer", user: "user:anne#member" },
  authorization_model_id: model.authorization_model_id,
}));
semanticErrors.push(await wireError("writeTupleNotPermitted", "POST", `/stores/${created.id}/write`, {
  authorization_model_id: model.authorization_model_id,
  writes: {
    tuple_keys: [{
      object: "document:roadmap",
      relation: "viewer",
      user: "group:eng",
    }],
  },
}));
semanticErrors.push(await wireError("writeMissingObjectType", "POST", `/stores/${created.id}/write`, {
  authorization_model_id: model.authorization_model_id,
  writes: {
    tuple_keys: [{
      object: "unknown:roadmap",
      relation: "viewer",
      user: "user:anne",
    }],
  },
}));
semanticErrors.push(await wireError("writeMissingUsersetRelation", "POST", `/stores/${created.id}/write`, {
  authorization_model_id: model.authorization_model_id,
  writes: {
    tuple_keys: [{
      object: "document:roadmap",
      relation: "viewer",
      user: "user:anne#missing",
    }],
  },
}));
semanticErrors.push(await wireError("writeMissingCondition", "POST", `/stores/${created.id}/write`, {
  authorization_model_id: conditionModel.authorization_model_id,
  writes: {
    tuple_keys: [{
      object: "document:roadmap",
      relation: "viewer",
      user: "user:anne",
    }],
  },
}));
semanticErrors.push(await wireError("writeUndefinedCondition", "POST", `/stores/${created.id}/write`, {
  authorization_model_id: conditionModel.authorization_model_id,
  writes: {
    tuple_keys: [{
      object: "document:roadmap",
      relation: "viewer",
      user: "user:anne",
      condition: { name: "cond2", context: {} },
    }],
  },
}));
semanticErrors.push(await wireError("writeInvalidRestrictionCondition", "POST", `/stores/${created.id}/write`, {
  authorization_model_id: conditionModel.authorization_model_id,
  writes: {
    tuple_keys: [{
      object: "document:roadmap",
      relation: "writer",
      user: "user:anne",
      condition: { name: "cond1", context: {} },
    }],
  },
}));
semanticErrors.push(await wireError("writeInvalidConditionContext", "POST", `/stores/${created.id}/write`, {
  authorization_model_id: conditionModel.authorization_model_id,
  writes: {
    tuple_keys: [{
      object: "document:roadmap",
      relation: "viewer",
      user: "user:anne",
      condition: { name: "cond1", context: { unknownparam: "bad" } },
    }],
  },
}));
semanticErrors.push(await wireError("writeInvalidConditionValue", "POST", `/stores/${created.id}/write`, {
  authorization_model_id: conditionModel.authorization_model_id,
  writes: {
    tuple_keys: [{
      object: "document:roadmap",
      relation: "viewer",
      user: "user:anne",
      condition: { name: "cond1", context: { param1: 12 } },
    }],
  },
}));
semanticErrors.push(await wireError("assertionMissingObjectType", "PUT", `/stores/${created.id}/assertions/${model.authorization_model_id}`, {
  assertions: [{
    tuple_key: { object: "unknown:roadmap", relation: "viewer", user: "user:anne" },
    expectation: true,
  }],
}));
semanticErrors.push(await wireError("assertionMissingRelation", "PUT", `/stores/${created.id}/assertions/${model.authorization_model_id}`, {
  assertions: [{
    tuple_key: { object: "document:roadmap", relation: "missing", user: "user:anne" },
    expectation: true,
  }],
}));
semanticErrors.push(await wireError("assertionMissingSubjectType", "PUT", `/stores/${created.id}/assertions/${model.authorization_model_id}`, {
  assertions: [{
    tuple_key: { object: "document:roadmap", relation: "viewer", user: "group:eng" },
    expectation: true,
  }],
}));
semanticErrors.push(await wireError("assertionMissingUsersetRelation", "PUT", `/stores/${created.id}/assertions/${model.authorization_model_id}`, {
  assertions: [{
    tuple_key: { object: "document:roadmap", relation: "viewer", user: "user:anne#missing" },
    expectation: true,
  }],
}));
semanticErrors.push(await wireError("invalidOnMissing", "POST", `/stores/${created.id}/write`, {
  authorization_model_id: model.authorization_model_id,
  deletes: { tuple_keys: [tuple], on_missing: "bad_option" },
}));
semanticErrors.push(await wireError("invalidOnDuplicate", "POST", `/stores/${created.id}/write`, {
  authorization_model_id: model.authorization_model_id,
  writes: { tuple_keys: [tuple], on_duplicate: "bad_option" },
}));
semanticErrors.push(await wireError("writeOperationLimit", "POST", `/stores/${created.id}/write`, {
  authorization_model_id: model.authorization_model_id,
  deletes: {
    tuple_keys: Array.from({ length: 101 }, (_, index) => ({
      object: `document:limit-${index}`,
      relation: "viewer",
      user: "user:anne",
    })),
  },
}));
semanticErrors.push(await wireError("duplicateWriteTuple", "POST", `/stores/${created.id}/write`, {
  authorization_model_id: model.authorization_model_id,
  writes: { tuple_keys: [tuple, tuple] },
}));
semanticErrors.push(await wireError(
  "storeNotFound",
  "GET",
  "/stores/01ARZ3NDEKTSV4RRFFQ69G5FAV",
));
semanticErrors.push(await wireError(
  "modelNotFound",
  "GET",
  `/stores/${created.id}/authorization-models/01ARZ3NDEKTSV4RRFFQ69G5FAV`,
));
await client.deleteTuples([tuple], {
  authorizationModelId: model.authorization_model_id,
});
await storeClient.deleteStore();
await new OpenFgaClient({ apiUrl, storeId: second.id, credentials }).deleteStore();

process.stdout.write(JSON.stringify({
  sdk: "@openfga/sdk",
  version: "0.9.6",
  status: "pass",
  endpoints: 14,
  httpErrors,
  semanticErrors,
}));
process.stdout.write("\n");
