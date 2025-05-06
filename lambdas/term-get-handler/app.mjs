import { Lambda } from "@aws-sdk/client-lambda";
import schema from "../../openapi/schemas/term.json" assert { type: "json" };

const lambda = new Lambda();

function buildSelectList() {
  // Each property’s “title” is "Term__c.Field__c" or "Term__c.Relation.Field"
  return Object.values(schema.properties)
    .map(p => p.title)
    .join(", ");
}

function buildWhereClause(params) {
  const clauses = [];

  for (const [paramName, rawValue] of Object.entries(params)) {
    const prop = schema.properties[paramName];
    if (!prop) continue;

    // strip the "Term__c." prefix
    const fieldPath = prop.title.split(".").slice(1).join(".");
    // quote strings
    const needsQuotes = prop.type === "string";
    const value = needsQuotes
      ? `'${rawValue.replace(/'/g, "\\'")}'`
      : rawValue;
    clauses.push(`${fieldPath} = ${value}`);
  }

  return clauses.length ? ` WHERE ${clauses.join(" AND ")}` : "";
}

function transformRecord(rec) {
  const out = {};
  for (const [key, prop] of Object.entries(schema.properties)) {
    const path = prop.title.split(".").slice(1); // e.g. ["Owner","Name"]
    let cursor = rec;
    for (const segment of path) {
      if (cursor == null) break;
      cursor = cursor[segment];
    }
    out[key] = cursor ?? null;
  }
  return out;
}

export const handler = async (event) => {
  const qs = event.queryStringParameters ?? {};

  console.log("Received event:", JSON.stringify(event));

  const soql =
    `SELECT ${buildSelectList()}` +
    ` FROM Term__c` +
    buildWhereClause(qs);

  console.log("Built SOQL:", soql);

  // invoke sf-query Lambda
  const invokeRes = await lambda.invoke({
    FunctionName: process.env.SF_QUERY_LAMBDA_NAME,
    Payload: Buffer.from(JSON.stringify({ soql })),
  });

  console.log("Raw invoke response:", invokeRes);

  let payload;
  try {
    const invokeRes = await lambda.invoke({
      FunctionName: process.env.SF_QUERY_LAMBDA_NAME,
      Payload: Buffer.from(JSON.stringify({ soql })),
    });
    console.log("Raw invoke response:", invokeRes);

    payload = JSON.parse(Buffer.from(invokeRes.Payload).toString());
    
    console.log("Parsed payload:", payload);

  } catch (err) {
    console.error("Error invoking sf-query Lambda:", err);
    return {
      statusCode: 502,
      body: JSON.stringify({ error: "Upstream Lambda invoke failed", detail: err.message }),
    };
  }

  const records = Array.isArray(payload.records)
    ? payload.records.map(transformRecord)
    : [];
  
  console.log("Transformed records:", records);

  return {
    statusCode: 200,
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(records),
  };
};
