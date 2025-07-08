import {
  AthenaClient,
  StartQueryExecutionCommand,
  GetQueryExecutionCommand,
  GetQueryResultsCommand
} from "@aws-sdk/client-athena";

const athena = new AthenaClient({ region: "us-west-2" });

const OUTPUT_LOCATION = "s3://athena-query-results-split/";
const DATABASE = "split";

export const handler = async (event) => {
  const key = event.queryStringParameters?.key || "dmartin";
  const daysRaw = parseInt(event.queryStringParameters?.days || "30", 10);
  const days = Math.min(Math.max(daysRaw, 1), 90);

  const sql = `
    WITH ranked AS (
      SELECT 
        key,
        splitname,
        treatment,
        date_format(from_unixtime(timestamp / 1000), '%Y-%m-%d %H:%i:%s') AS utc,
        date_trunc('day', from_unixtime(timestamp / 1000)) AS day_bucket,
        ROW_NUMBER() OVER (
          PARTITION BY splitname, treatment, date_trunc('day', from_unixtime(timestamp / 1000))
          ORDER BY timestamp DESC
        ) AS rownum
      FROM 
        impressions4
      WHERE 
        key = '${key}'
        AND from_unixtime(timestamp / 1000) >= date_add('day', -${days}, current_date)
    )
    SELECT
      key,
      splitname,
      treatment,
      utc
    FROM 
      ranked
    WHERE 
      rownum = 1
    ORDER BY 
      utc DESC
    LIMIT 100
  `;

  try {
    const start = await athena.send(new StartQueryExecutionCommand({
      QueryString: sql,
      QueryExecutionContext: { Database: DATABASE },
      ResultConfiguration: { OutputLocation: OUTPUT_LOCATION }
    }));

    const queryId = start.QueryExecutionId;
    console.log("QueryExecutionId:", queryId);

    let state = "RUNNING";
    while (state === "RUNNING" || state === "QUEUED") {
      await new Promise(res => setTimeout(res, 1000));
      const { QueryExecution } = await athena.send(new GetQueryExecutionCommand({ QueryExecutionId: queryId }));
      state = QueryExecution.Status.State;
      if (state === "FAILED") throw new Error(QueryExecution.Status.StateChangeReason);
      if (state === "CANCELLED") throw new Error("Query was cancelled");
    }

    const results = await athena.send(new GetQueryResultsCommand({ QueryExecutionId: queryId }));
    const rows = results.ResultSet.Rows.map(r => r.Data.map(cell => cell.VarCharValue));
    const headers = rows[0];
    const data = rows.slice(1).map(row => Object.fromEntries(row.map((v, i) => [headers[i], v])));

    return {
      statusCode: 200,
      body: JSON.stringify(data)
    };

  } catch (err) {
    console.error("Handler error:", err.message);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: err.message })
    };
  }
};

