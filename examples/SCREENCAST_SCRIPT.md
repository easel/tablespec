# tablespec Screencast Script

**Runtime:** ~3 minutes
**Tools:** Docker, VHS (charmbracelet/vhs)
**Record:** `vhs examples/demo.tape`

---

## COLD OPEN

> Terminal appears with a dark theme. Two comment lines fade in:

```
# tablespec — Universal Metadata Format for table schemas
# A complete walkthrough: schema loading, generation, validation, and PySpark
```

**NARRATOR:** tablespec is a Python library for defining, validating, and
generating table schemas using a single YAML-based format called UMF —
Universal Metadata Format. Let's see what it can do.

---

## SCENE 1 — "The Schema"

> The example UMF YAML file is displayed on screen.

```yaml
version: "1.0"
table_name: Medical_Claims
description: Healthcare claims and billing information
columns:
  - name: claim_id
    data_type: VARCHAR
    length: 50
    nullable:
      MD: false   # Medicaid
      MP: false   # Medicare Part D
      ME: false   # Medicare
  - name: claim_amount
    data_type: DECIMAL
    precision: 10
    scale: 2
    nullable:
      MD: true
      MP: true
      ME: true
  - name: provider_id
    data_type: VARCHAR
    length: 20
```

**NARRATOR:** This is a UMF schema for a healthcare claims table. Three
columns, each with a data type, length, and nullable configuration per
Line of Business — Medicaid, Medicare Part D, and Medicare. One YAML file
is the single source of truth for everything downstream.

---

## SCENE 2 — "The Build"

> Docker image builds with PySpark 4.0, Java 21, and tablespec.

**NARRATOR:** We're building a Docker image with PySpark 4.0 and
tablespec installed. This mirrors what you'd have on Databricks — same
Spark, same library, same behavior.

---

## SCENE 3 — "The Demo"

The demo runs in Docker. Each section appears sequentially:

### ACT 1: Load & Inspect (Section 1)

> Output shows table name, description, columns, nullable config.

**NARRATOR:** We load the UMF YAML into a Pydantic model. Every field is
type-checked. The nullable config tells us which columns are required in
which LOB — claim_id is required everywhere, but claim_amount can be null.

---

### ACT 2: Schema Generation (Section 2)

> SQL DDL, PySpark StructType, and JSON Schema appear in sequence.

**NARRATOR:** From one UMF file, we generate three schema formats. SQL DDL
for data warehouses. PySpark StructType for Spark jobs. JSON Schema for API
validation. One source, many targets.

---

### ACT 3: Type Mappings (Section 3)

> A table shows VARCHAR -> StringType -> string -> StringType across systems.

**NARRATOR:** The type mapping engine converts between UMF, PySpark, JSON
Schema, and Great Expectations. VARCHAR becomes StringType in Spark, string
in JSON, StringType in GX. DECIMAL stays DECIMAL with precision preserved.

---

### ACT 4: Domain Type Inference (Section 4)

> Column names are matched to domain types with confidence scores.

**NARRATOR:** tablespec ships with 42 domain types. Feed it a column name
like "provider_npi" and it recognizes it as an NPI — National Provider
Identifier — with 100% confidence. It even knows the validation rule:
a 10-digit regex. state_code maps to US state codes. member_email maps to
email. All automatic.

---

### ACT 5: Great Expectations Baseline (Section 5)

> 13 expectations are generated with severity levels.

**NARRATOR:** From the same UMF, we generate a baseline Great Expectations
suite. 13 expectations: column existence, type validation, nullability
constraints, length checks. Each tagged with a severity — critical for
data integrity, warning for quality, info for structural checks. No manual
GX authoring needed.

---

### ACT 6: LLM Prompt Generation (Section 6)

> Prompt lengths and a preview are displayed.

**NARRATOR:** tablespec generates structured prompts for LLMs. A
documentation prompt asks an AI to analyze the table's business purpose,
data flow, and compliance considerations. A validation prompt asks it to
generate multi-column GX rules that go beyond what baseline can do
automatically. The prompts include all column metadata, sample values, and
domain context.

---

### ACT 7: UMF Diffing (Section 7)

> Two changes detected: a new column and a modified description.

**NARRATOR:** Schema evolution tracking. We modified the claims table —
added a service_date column and updated a description. UMFDiff detects
both changes instantly. This powers changelog generation and schema review
workflows.

---

### ACT 8: CLI Authoring Commands (Scene 13)

> CLI commands add a column, modify its type, rename with alias, set a domain type, and remove a validation expectation.

**NARRATOR:** CLI commands for schema authoring. Add a column, modify its
type, rename it with alias preservation. Assign domain types from the
built-in registry. And manage validation expectations — all without
touching YAML directly.

---

### ACT 9a: Spark Session (Section 14)

> Spark 4.0.1 session is created. A DataFrame with 5 claims is displayed.

**NARRATOR:** Now we enter PySpark territory. Creating a Spark session
and a sample DataFrame with five claims.

---

### ACT 9b: Profiling (Section 9)

> SparkToUmfMapper infers column types from the DataFrame.

**NARRATOR:** SparkToUmfMapper infers a UMF schema from the DataFrame.
Column names, types, and nullability, all detected automatically.

---

### ACT 9c: Validation (Section 10)

> One validation error: claim_amount has the wrong data type.

**NARRATOR:** TableValidator checks the DataFrame against the UMF spec.
It catches type drift that causes silent data corruption.

---

### ACT 9d: Sample Data Generation (Section 11)

> 100 rows of claims and providers are generated from UMF specs.

**NARRATOR:** Sample data generation from UMF specs. 100 rows per table,
respecting types, nullable rules, and domain constraints.

---

## CLOSING

> "Demo complete!" banner appears.

**NARRATOR:** That's tablespec. One YAML schema drives SQL generation,
Spark schemas, Great Expectations, domain inference, validation, profiling,
LLM prompts, and sample data. Define once, use everywhere.

---

## Production Notes

**To record the screencast:**

```bash
# Build the Docker image first (one-time)
docker build -t tablespec-demo -f examples/Dockerfile.demo .

# Record with VHS
vhs examples/demo.tape
```

**Outputs:**
- `examples/demo.gif` — animated GIF for README / docs
- `examples/demo.mp4` — video for presentations

**To customize:**
- Edit `examples/demo.tape` for timing, theme, font
- Edit `examples/demo.py` to add/remove sections
- The VHS tape runs the demo inside Docker for reproducibility
