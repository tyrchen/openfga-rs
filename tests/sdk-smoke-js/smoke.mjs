import { OpenFgaClient } from "@openfga/sdk";

const apiUrl = process.env.FGA_API_URL;
if (!apiUrl || !/^http:\/\/(localhost|127\.0\.0\.1|\[::1\])(?::[0-9]{1,5})?\/?$/.test(apiUrl)) {
  throw new Error("FGA_API_URL must be a loopback HTTP URL");
}

const bootstrapClient = new OpenFgaClient({ apiUrl });
const created = await bootstrapClient.createStore({ name: "phase0-sdk-smoke" });
if (typeof created.id !== "string" || !/^[0-9A-HJKMNP-TV-Z]{26}$/.test(created.id)) {
  throw new Error("CreateStore returned an invalid store identifier");
}

const storeClient = new OpenFgaClient({ apiUrl, storeId: created.id });
const fetched = await storeClient.getStore();
if (fetched.id !== created.id || fetched.name !== "phase0-sdk-smoke") {
  throw new Error("GetStore did not return the SDK-created store");
}
await storeClient.deleteStore();

process.stdout.write(JSON.stringify({ sdk: "@openfga/sdk", version: "0.9.6", status: "pass" }));
process.stdout.write("\n");
