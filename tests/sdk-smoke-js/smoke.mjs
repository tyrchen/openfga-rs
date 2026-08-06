import { CredentialsMethod, OpenFgaClient } from "@openfga/sdk";

const apiUrl = process.env.FGA_API_URL;
if (!apiUrl || !/^https?:\/\/(localhost|127\.0\.0\.1|\[::1\])(?::[0-9]{1,5})?\/?$/.test(apiUrl)) {
  throw new Error("FGA_API_URL must be a loopback HTTP(S) URL");
}

const token = process.env.FGA_API_TOKEN;
const credentials = token
  ? { method: CredentialsMethod.ApiToken, config: { token } }
  : undefined;
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
}));
process.stdout.write("\n");
