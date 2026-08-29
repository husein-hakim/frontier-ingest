import fs from "node:fs/promises";
import path from "node:path";
import { SpreadsheetFile, Workbook } from "@oai/artifact-tool";

const workspace = path.resolve(process.cwd());
const inputDir = path.join(workspace, "outputs/submission");
const outputDir = process.env.WORKBOOK_OUTPUT_DIR
  ? path.resolve(process.env.WORKBOOK_OUTPUT_DIR)
  : path.join(workspace, "outputs/submission-artifact");
const previewDir = path.join(outputDir, "previews");

const readJson = async (name) =>
  JSON.parse(await fs.readFile(path.join(inputDir, name), "utf8"));

const [startups, products, papers, jobs, news, mappings] = await Promise.all([
  readJson("startup.json"),
  readJson("product.json"),
  readJson("research_paper.json"),
  readJson("job.json"),
  readJson("news.json"),
  readJson("entity_mapping_log.json"),
]);

const value = (item, route, fallback = null) => {
  let current = item;
  for (const key of route.split(".")) {
    if (current === null || current === undefined) return fallback;
    current = current[key];
  }
  return current === undefined || current === null ? fallback : current;
};

const text = (input) =>
  Array.isArray(input) ? input.join(", ") : input === null || input === undefined ? "" : String(input);

const cleanCell = (input) => {
  if (typeof input !== "string") return input;
  return input
    .toWellFormed()
    .replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F\uFFFE\uFFFF]/g, "");
};

const date = (input) => {
  if (!input) return null;
  const parsed = new Date(input);
  return Number.isNaN(parsed.getTime()) ? text(input) : parsed;
};

const specs = [
  {
    name: "Startups",
    color: "#28536B",
    note: "1,000 unique AI startups from the Y Combinator AI directory. Every row includes its canonical source URL and immutable raw-content hash.",
    columns: [
      ["Schema Version", 14], ["Record Type", 14], ["Source", 22], ["Source URL", 38],
      ["Startup Name", 24], ["Employee Count", 16], ["Website", 32], ["Description", 52],
      ["Batch", 12], ["Status", 12], ["Tags", 35], ["Collected At", 22],
      ["Content Hash", 26], ["Record Key", 26],
    ],
    data: startups.map((r) => [
      r.schema_version, r.record_type, r.source.name, r.source.url,
      value(r, "content.entityName", ""), value(r, "content.data.employeeCount", null),
      value(r, "content.website", ""), value(r, "content.description", ""),
      value(r, "content.batch", ""), value(r, "content.status", ""), text(value(r, "content.tags", [])),
      date(r.collected_at), r.source.content_hash, r.record_key,
    ]),
    dateColumns: [11],
    integerColumns: [5],
  },
  {
    name: "Products",
    color: "#5B5F97",
    note: "1,000 primary AI products linked to canonical startups. Null pricing means the source did not support a safe classification.",
    columns: [
      ["Schema Version", 14], ["Record Type", 14], ["Source", 22], ["Source URL", 38],
      ["Product Name", 24], ["Startup Name", 24], ["Pricing Model", 16], ["Tagline", 38],
      ["Website", 32], ["Description", 52], ["Batch", 12], ["Status", 12], ["Tags", 35],
      ["Collected At", 22], ["Content Hash", 26], ["Record Key", 26],
    ],
    data: products.map((r) => [
      r.schema_version, r.record_type, r.source.name, r.source.url,
      value(r, "content.productName", ""), value(r, "content.startupName", ""),
      value(r, "content.pricingModel", ""), value(r, "content.tagline", ""),
      value(r, "content.website", ""), value(r, "content.description", ""),
      value(r, "content.batch", ""), value(r, "content.status", ""), text(value(r, "content.tags", [])),
      date(r.collected_at), r.source.content_hash, r.record_key,
    ]),
    dateColumns: [13],
  },
  {
    name: "Research Papers",
    color: "#1B998B",
    note: "1,000 AI papers with associated GitHub repositories and star metrics. Star collection timestamps make the dynamic metric auditable.",
    columns: [
      ["Schema Version", 14], ["Record Type", 18], ["Source", 22], ["Source URL", 38],
      ["Title", 45], ["Authors", 45], ["Paper URL", 38], ["GitHub URL", 38],
      ["GitHub Stars", 15], ["Stars Collected At", 22], ["Stars Source", 20],
      ["Published Date", 20], ["Abstract", 55], ["Collected At", 22],
      ["Content Hash", 26], ["Record Key", 26],
    ],
    data: papers.map((r) => [
      r.schema_version, r.record_type, r.source.name, r.source.url,
      value(r, "content.title", ""), text(value(r, "content.authors", [])),
      value(r, "content.paper_url", ""), value(r, "content.github_url", ""),
      value(r, "content.github_stars", null), date(value(r, "content.github_stars_collected_at", null)),
      value(r, "content.github_stars_source", ""), date(value(r, "content.published_date", null)),
      value(r, "content.abstract", ""), date(r.collected_at), r.source.content_hash, r.record_key,
    ]),
    dateColumns: [9, 11, 13],
    integerColumns: [8],
  },
  {
    name: "Jobs",
    color: "#E76F51",
    note: "All AI-relevant jobs found across five monitored boards that were provably published within 24 hours of collection.",
    columns: [
      ["Schema Version", 14], ["Record Type", 12], ["Source", 18], ["Source URL", 38],
      ["Company", 24], ["Title", 38], ["Published At", 22], ["Remote", 12],
      ["Role Family", 16], ["Location", 28], ["Full Text", 60], ["Freshness Status", 18],
      ["Freshness Method", 24], ["Freshness Confidence", 20], ["Collected At", 22],
      ["Content Hash", 26], ["Record Key", 26],
    ],
    data: jobs.map((r) => [
      r.schema_version, r.record_type, r.source.name, r.source.url,
      value(r, "content.company", ""), value(r, "content.title", ""),
      date(value(r, "content.date", null)), value(r, "content.is_remote", false),
      value(r, "content.role_family", ""), value(r, "content.location", ""),
      value(r, "content.full_text", ""), value(r, "content.freshness_status", ""),
      value(r, "content.freshness_method", ""), value(r, "content.freshness_confidence", null),
      date(r.collected_at), r.source.content_hash, r.record_key,
    ]),
    dateColumns: [6, 14],
    percentageColumns: [13],
  },
  {
    name: "News",
    color: "#D4A017",
    note: "All AI news found across five monitored feeds that was provably published within 24 hours, with full article text and freshness evidence.",
    columns: [
      ["Schema Version", 14], ["Record Type", 12], ["Source", 20], ["Source URL", 38],
      ["Title", 45], ["Publisher", 22], ["Authors", 30], ["Published At", 22],
      ["Full Text", 60], ["Freshness Status", 18], ["Freshness Method", 24],
      ["Freshness Confidence", 20], ["Collected At", 22], ["Content Hash", 26], ["Record Key", 26],
    ],
    data: news.map((r) => [
      r.schema_version, r.record_type, r.source.name, r.source.url,
      value(r, "content.title", ""), value(r, "content.publisher", ""),
      text(value(r, "content.authors", [])), date(value(r, "content.published_at", null)),
      value(r, "content.full_text", ""), value(r, "content.freshness_status", ""),
      value(r, "content.freshness_method", ""), value(r, "content.freshness_confidence", null),
      date(r.collected_at), r.source.content_hash, r.record_key,
    ]),
    dateColumns: [7, 12],
    percentageColumns: [11],
  },
  {
    name: "Entity Mapping Log",
    color: "#6D597A",
    note: "Deterministic and fuzzy entity-resolution decisions. NEEDS_REVIEW is intentionally preserved instead of forcing unsafe merges.",
    columns: [
      ["Raw Name", 26], ["Normalized Name", 26], ["Canonical Name", 26], ["Method", 22],
      ["Confidence", 16], ["Status", 18], ["Resolver Version", 18], ["Created At", 22],
    ],
    data: mappings.map((r) => [
      r.raw_name, r.normalized_name, r.canonical_name ?? "", r.method,
      r.confidence, r.status, r.resolver_version, date(r.created_at),
    ]),
    dateColumns: [7],
    percentageColumns: [4],
  },
];

const colLetter = (index) => {
  let value = index + 1;
  let result = "";
  while (value > 0) {
    value -= 1;
    result = String.fromCharCode(65 + (value % 26)) + result;
    value = Math.floor(value / 26);
  }
  return result;
};

const workbook = Workbook.create();
for (const [sheetIndex, spec] of specs.entries()) {
  const sheet = workbook.worksheets.add(spec.name);
  sheet.showGridLines = false;
  const lastColumn = colLetter(spec.columns.length - 1);
  const lastRow = spec.data.length + 4;

  const titleRange = sheet.getRange(`A1:${lastColumn}1`);
  titleRange.merge();
  titleRange.values = [[`${spec.name} — ${spec.data.length.toLocaleString()} records`]];
  titleRange.format.fill = "#F1F3F4";
  titleRange.format.font = { bold: true, color: "#202124", size: 15 };
  titleRange.format.rowHeight = 28;
  titleRange.format.verticalAlignment = "center";

  const noteRange = sheet.getRange(`A2:${lastColumn}2`);
  noteRange.merge();
  noteRange.values = [[spec.note]];
  noteRange.format.fill = "#FFFFFF";
  noteRange.format.font = { color: "#5F6368", italic: true, size: 10 };
  noteRange.format.rowHeight = 25;

  const headerRange = sheet.getRange(`A4:${lastColumn}4`);
  headerRange.values = [spec.columns.map(([label]) => label)];
  headerRange.format.fill = "#E8EAED";
  headerRange.format.font = { bold: true, color: "#202124" };
  headerRange.format.rowHeight = 24;
  headerRange.format.verticalAlignment = "center";

  if (spec.data.length) {
    const dataRange = sheet.getRange(`A5:${lastColumn}${lastRow}`);
    dataRange.values = spec.data.map((row) => row.map(cleanCell));
    dataRange.format.font = { size: 9, color: "#1F2937" };
    dataRange.format.rowHeight = 18;
    dataRange.format.verticalAlignment = "top";
    dataRange.format.borders = {
      insideHorizontal: { style: "thin", color: "#E5E7EB" },
    };
    const table = sheet.tables.add(`A4:${lastColumn}${lastRow}`, true, `${spec.name.replace(/[^A-Za-z0-9]/g, "")}Table`);
    table.style = "TableStyleLight1";
    table.showBandedRows = false;
    table.showFilterButton = true;
  }

  for (let column = 0; column < spec.columns.length; column += 1) {
    const range = sheet.getRangeByIndexes(3, column, spec.data.length + 1, 1);
    range.format.columnWidth = spec.columns[column][1];
  }
  for (const column of spec.dateColumns ?? []) {
    if (spec.data.length) {
      sheet.getRangeByIndexes(4, column, spec.data.length, 1).setNumberFormat("yyyy-mm-dd hh:mm");
    }
  }
  for (const column of spec.integerColumns ?? []) {
    if (spec.data.length) {
      sheet.getRangeByIndexes(4, column, spec.data.length, 1).setNumberFormat("#,##0");
    }
  }
  for (const column of spec.percentageColumns ?? []) {
    if (spec.data.length) {
      sheet.getRangeByIndexes(4, column, spec.data.length, 1).setNumberFormat("0.0%");
    }
  }

  if (spec.name === "Jobs" || spec.name === "News") {
    const statusColumn = spec.name === "Jobs" ? "L" : "J";
    sheet.getRange(`${statusColumn}5:${statusColumn}${lastRow}`).conditionalFormats.add(
      "containsText",
      { text: "VERIFIED", format: { fill: "#DCFCE7", font: { color: "#166534", bold: true } } },
    );
  }
  if (spec.name === "Entity Mapping Log") {
    sheet.getRange(`F5:F${lastRow}`).conditionalFormats.add(
      "containsText",
      { text: "NEEDS_REVIEW", format: { fill: "#FEF3C7", font: { color: "#92400E", bold: true } } },
    );
  }
  sheet.freezePanes.freezeRows(4);
  sheet.freezePanes.freezeColumns(Math.min(4, spec.columns.length));
}

await fs.mkdir(previewDir, { recursive: true });
const inspection = await workbook.inspect({
  kind: "sheet,table",
  maxChars: 8_000,
  tableMaxRows: 6,
  tableMaxCols: 10,
});
console.log(inspection.ndjson);

const errors = await workbook.inspect({
  kind: "match",
  searchTerm: "#REF!|#DIV/0!|#VALUE!|#NAME\\?|#N/A",
  options: { useRegex: true, maxResults: 100 },
  summary: "final formula error scan",
});
console.log(errors.ndjson);

await fs.mkdir(outputDir, { recursive: true });
const output = await SpreadsheetFile.exportXlsx(workbook);
const outputPath = path.join(outputDir, "frontier-intelligence-data.xlsx");
await output.save(outputPath);
try {
  await fs.access(outputPath);
} catch {
  const bytes = new Uint8Array(await output.arrayBuffer());
  await fs.writeFile(outputPath, bytes);
}

const previewResults = [];
for (const spec of specs) {
  const previewPath = path.join(
    previewDir,
    `${spec.name.replace(/\s+/g, "-").toLowerCase()}.png`,
  );
  try {
    const preview = await workbook.render({
      sheetName: spec.name,
      range: `A1:${colLetter(spec.columns.length - 1)}8`,
      scale: 0.8,
    });
    const bytes = new Uint8Array(await preview.arrayBuffer());
    await fs.writeFile(previewPath, bytes);
    previewResults.push({ sheet: spec.name, previewPath, status: "rendered" });
  } catch (error) {
    previewResults.push({
      sheet: spec.name,
      status: "failed",
      message: error instanceof Error ? error.message : String(error),
    });
  }
}

console.log(JSON.stringify({
  outputPath,
  sheetCount: specs.length,
  rowCounts: Object.fromEntries(specs.map((s) => [s.name, s.data.length])),
  previewResults,
}));
