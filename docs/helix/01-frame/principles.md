# Design Principles: tablespec

1. **UMF is the single source of truth.** All schema representations are derived from UMF; conversions should be bidirectional where possible.

2. **Pure Python core, optional heavy dependencies.** PySpark is opt-in. The core library must function without it.

3. **Type safety at the boundary.** Pydantic models enforce constraints at runtime. Invalid schemas fail fast with clear errors.

4. **Deterministic outputs.** Schema generation and baseline expectations produce identical output for identical input.

5. **Healthcare domain awareness.** Per-LOB nullable configuration, healthcare-specific validation patterns, and domain-aware relationship discovery are first-class concerns.

6. **Read and write integration.** Great Expectations integration is bidirectional: generate expectations from UMF, extract constraints back into UMF.

7. **Evidence over assumption.** Profiling data enriches UMF but does not override explicit definitions.

8. **Minimal, focused API.** Each module has a clear responsibility. No catch-all utilities.
