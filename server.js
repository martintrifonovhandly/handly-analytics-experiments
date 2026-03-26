const express = require("express");
const path = require("path");
const {
  AthenaClient,
  StartQueryExecutionCommand,
  GetQueryExecutionCommand,
  GetQueryResultsCommand,
} = require("@aws-sdk/client-athena");

const app = express();
const PORT = process.env.PORT || 3000;

const athena = new AthenaClient({
  region: "eu-central-1",
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  },
});

const DATABASE = process.env.ATHENA_DATABASE || "webmaster_analytics_prod";
const WORKGROUP = process.env.ATHENA_WORKGROUP || "webmaster-analytics-prod";
const OUTPUT_LOCATION = process.env.ATHENA_OUTPUT_LOCATION || "";

app.use(express.static(path.join(__dirname, "public")));

app.get("/api/events", async (req, res) => {
  try {
    const params = {
      QueryString: "SELECT * FROM events LIMIT 100",
      QueryExecutionContext: { Database: DATABASE },
      WorkGroup: WORKGROUP,
    };
    if (OUTPUT_LOCATION) {
      params.ResultConfiguration = { OutputLocation: OUTPUT_LOCATION };
    }
    const { QueryExecutionId } = await athena.send(
      new StartQueryExecutionCommand(params)
    );

    // Poll until query completes
    let state = "RUNNING";
    while (state === "RUNNING" || state === "QUEUED") {
      const { QueryExecution } = await athena.send(
        new GetQueryExecutionCommand({ QueryExecutionId })
      );
      state = QueryExecution?.Status?.State ?? "FAILED";
      if (state === "RUNNING" || state === "QUEUED") {
        await new Promise((r) => setTimeout(r, 1000));
      }
    }

    if (state !== "SUCCEEDED") {
      return res.status(500).json({ error: `Query finished with state: ${state}` });
    }

    const { ResultSet } = await athena.send(
      new GetQueryResultsCommand({ QueryExecutionId })
    );

    const rows = ResultSet?.Rows ?? [];
    if (rows.length === 0) {
      return res.json({ columns: [], rows: [] });
    }

    const columns = rows[0].Data.map((d) => d.VarCharValue);
    const dataRows = rows.slice(1).map((row) =>
      Object.fromEntries(
        row.Data.map((d, i) => [columns[i], d.VarCharValue ?? null])
      )
    );

    res.json({ columns, rows: dataRows });
  } catch (err) {
    console.error("Athena query error:", err);
    res.status(500).json({ error: err.message });
  }
});

app.listen(PORT, () => {
  console.log(`Server running at http://localhost:${PORT}`);
});
