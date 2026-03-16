"""Tablespec CLI - UMF schema management and conversion.

Examples:
    # Convert JSON to split
    tablespec convert outreach_list.json tables/outreach_list/

    # Convert split to JSON
    tablespec convert tables/outreach_list/ outreach_list.json

    # Validate UMF
    tablespec validate tables/outreach_list/

    # Show UMF info
    tablespec info tables/outreach_list/

    # Batch convert
    tablespec batch-convert tables/ output/ --format json

"""

from pathlib import Path

from pydantic import ValidationError
from rich.console import Console
from rich.table import Table as RichTable
import typer

from tablespec.excel_converter import ExcelToUMFConverter, UMFToExcelConverter
from tablespec.inference.domain_types import DomainTypeInference, DomainTypeRegistry
from tablespec.umf_loader import UMFFormat, UMFLoader

# validator module is not yet ported; commands that depend on it will be
# registered only when the module is available.
try:
    from tablespec.validator import (  # type: ignore[import-not-found]
        ValidationContext,
        convert_table,
        show_table_info,
        validate_pipeline,
        validate_table,
    )

    _HAS_VALIDATOR = True
except ImportError:
    _HAS_VALIDATOR = False

app = typer.Typer(
    name="tablespec",
    help="Work with UMF (Universal Metadata Format) table schemas",
)
console = Console()

# Module-level validation context (process lifetime caching) - only when validator is available
_validation_context = ValidationContext() if _HAS_VALIDATOR else None


if _HAS_VALIDATOR:

    @app.command()
    def convert(
        source: Path = typer.Argument(
            ...,
            help="Source UMF file or directory",
            exists=True,
        ),
        dest: Path = typer.Argument(
            ...,
            help="Destination path",
        ),
        format: str | None = typer.Option(
            None,
            "--format",
            "-f",
            help="Target format: 'split' or 'json' (auto-detected if not specified)",
        ),
        force: bool = typer.Option(
            False,
            "--force",
            help="Overwrite existing destination",
        ),
    ) -> None:
        """Convert UMF between split and JSON formats.

        Examples:
          # Split to JSON (for artifacts)
          tablespec convert tables/outreach_list/ outreach_list.json

          # JSON to split (for editing)
          tablespec convert outreach_list.json tables/outreach_list/

          # Explicit format
          tablespec convert tables/ output/ --format json

        """
        assert _validation_context is not None
        try:
            # Auto-detect source format
            source_format = _validation_context.converter.detect_format(source)

            # Determine target format
            if format:
                if format.lower() in ("split", "s"):
                    target_format = UMFFormat.SPLIT
                elif format.lower() in ("json", "j"):
                    target_format = UMFFormat.JSON
                else:
                    console.print(
                        f"[red]Error:[/red] Unknown format '{format}'. Use 'split' or 'json'."
                    )
                    raise typer.Exit(1)
            # Infer from source
            elif source_format == UMFFormat.SPLIT:
                target_format = UMFFormat.JSON  # Default: split -> JSON (artifact)
            else:  # JSON
                target_format = UMFFormat.SPLIT  # Default: JSON -> split (for editing)

            # Check if target exists
            if dest.exists() and not force:
                console.print(
                    f"[red]Error:[/red] {dest} already exists. Use --force to overwrite."
                )
                raise typer.Exit(1)

            # Convert
            console.print(f"[cyan]Converting[/cyan] {source} -> {dest}")
            console.print(f"[dim]Format: {source_format.value} -> {target_format.value}[/dim]")

            convert_table(source, dest, target_format=target_format, context=_validation_context)

            console.print("[green]Done.[/green] Conversion complete!")

        except Exception as e:
            console.print(f"[red]Error:[/red] {e}")
            raise typer.Exit(1)

    @app.command()
    def validate(
        path: Path = typer.Argument(
            ...,
            help="UMF file or directory to validate",
            exists=True,
        ),
        verbose: bool = typer.Option(
            False,
            "--verbose",
            "-v",
            help="Show detailed validation errors",
        ),
    ) -> None:
        """Validate UMF schema for correctness.

        Performs comprehensive validation including:
        - JSON schema validation against UMF specification
        - UMF schema structure (Pydantic models)
        - Filename pattern correctness
        - Column naming conventions (lowercase_snake_case)
        - Great Expectations validation rules (if present)
        - Expectation type compatibility with GX library
        - Relationship integrity (automatic when multiple tables present)

        Examples:
          tablespec validate tables/outreach_list/
          tablespec validate outreach_list.umf.yaml -v
          tablespec validate tables/

        """
        assert _validation_context is not None
        try:
            console.print(f"[cyan]Validating[/cyan] {path}...")

            # Determine if path is single table or pipeline
            if path.is_dir() and (path / "schema.yaml").exists():
                # Single table validation
                success, errors = validate_table(path, _validation_context, verbose=verbose)

                if success:
                    # Show summary
                    umf = _validation_context.load_umf(path)
                    console.print("[green]Valid[/green] UMF schema")
                    console.print(f"  [cyan]Table:[/cyan] {umf.table_name} ({umf.canonical_name})")
                    console.print(f"    [cyan]Columns:[/cyan] {len(umf.columns)}")
                    if umf.file_format and umf.file_format.filename_pattern:
                        console.print("    [cyan]Filename pattern:[/cyan] Valid")
                    if umf.validation_rules and umf.validation_rules.expectations:
                        console.print(
                            f"    [cyan]Expectations:[/cyan] "
                            f"{len(umf.validation_rules.expectations)}"
                        )
                    if umf.relationships and umf.relationships.foreign_keys:
                        console.print(
                            f"    [cyan]Foreign keys:[/cyan] "
                            f"{len(umf.relationships.foreign_keys)}"
                        )
                    if hasattr(umf, "derivations") and umf.derivations:
                        surv_mappings = len(umf.derivations.get("mappings", {}))
                        console.print(f"    [cyan]Survivorship mappings:[/cyan] {surv_mappings}")
                else:
                    console.print("[red]FAIL[/red] Validation failed")
                    for error in errors:
                        console.print(f"  {error}")
                    raise typer.Exit(1)
            elif path.is_dir():
                # Pipeline validation
                results = validate_pipeline(path, _validation_context, verbose=verbose)

                failed_tables = {name: errs for name, errs in results.items() if errs}

                if failed_tables:
                    console.print("[red]FAIL[/red] Validation failed")
                    for table_name, errs in failed_tables.items():
                        console.print(f"\n[red]{table_name}:[/red]")
                        for error in errs:
                            console.print(f"  {error}")
                    raise typer.Exit(1)
                console.print(f"[green]Valid[/green] All {len(results)} tables passed validation")
            else:
                # Single file validation
                success, errors = validate_table(path, _validation_context, verbose=verbose)

                if success:
                    umf = _validation_context.load_umf(path)
                    console.print("[green]Valid[/green] UMF schema")
                    console.print(f"  [cyan]Table:[/cyan] {umf.table_name} ({umf.canonical_name})")
                    console.print(f"    [cyan]Columns:[/cyan] {len(umf.columns)}")
                else:
                    console.print("[red]FAIL[/red] Validation failed")
                    for error in errors:
                        console.print(f"  {error}")
                    raise typer.Exit(1)

        except Exception as e:
            console.print(f"[red]Error:[/red] {e}")
            raise typer.Exit(1)

    @app.command()
    def info(
        path: Path = typer.Argument(
            ...,
            help="UMF file or directory",
            exists=True,
        ),
    ) -> None:
        """Display UMF schema information.

        Examples:
          tablespec info tables/outreach_list/
          tablespec info outreach_list.umf.yaml

        """
        assert _validation_context is not None
        try:
            show_table_info(path, _validation_context)
            umf = _validation_context.load_umf(path)

            # Title
            console.print(f"\n[bold cyan]{umf.canonical_name}[/bold cyan]")
            console.print(f"  [dim]Table Name:[/dim] {umf.table_name}")

            if umf.description:
                console.print(f"  [dim]Description:[/dim] {umf.description}")

            console.print(f"  [dim]Version:[/dim] {umf.version}")

            # Columns summary
            console.print(f"\n[bold cyan]Columns ({len(umf.columns)}):[/bold cyan]")
            table = RichTable(show_header=True, show_lines=False)
            table.add_column("Name", style="cyan")
            table.add_column("Type", style="magenta")
            table.add_column("Nullable", justify="center")
            table.add_column("Source")

            for col in umf.columns[:10]:  # First 10
                nullable = "Y" if col.is_nullable_for_all_contexts() else "N"
                table.add_row(col.name, col.data_type, nullable, col.source or "data")

            console.print(table)
            if len(umf.columns) > 10:
                console.print(f"  [dim]... and {len(umf.columns) - 10} more[/dim]")

            # Validation summary
            if umf.validation_rules and umf.validation_rules.expectations:
                exp_count = len(umf.validation_rules.expectations)
                console.print(f"\n[bold cyan]Validation:[/bold cyan] {exp_count} expectations")

            # Relationships
            if umf.relationships:
                fk_count = len(umf.relationships.foreign_keys or [])
                ref_count = len(umf.relationships.referenced_by or [])
                if fk_count or ref_count:
                    console.print(
                        "\n[bold cyan]Relationships:[/bold cyan] "
                        + f"{fk_count} outgoing, {ref_count} incoming"
                    )

            console.print()

        except Exception as e:
            console.print(f"[red]Error:[/red] {e}")
            raise typer.Exit(1)


@app.command()
def batch_convert(
    source_dir: Path = typer.Argument(
        ...,
        help="Directory containing UMF files/dirs",
        exists=True,
    ),
    dest_dir: Path = typer.Argument(
        ...,
        help="Output directory",
    ),
    format: str = typer.Option(
        ...,
        "--format",
        "-f",
        help="Target format: 'split' or 'json'",
    ),
    pattern: str = typer.Option(
        "*.umf.yaml",
        "--pattern",
        "-p",
        help="File pattern to match",
    ),
    force: bool = typer.Option(
        False,
        "--force",
        help="Overwrite existing files",
    ),
) -> None:
    """Batch convert multiple UMF files.

    Examples:
      # Convert all JSON files to split
      tablespec batch-convert tables/ split-tables/ --format split

      # Convert all split dirs to JSON
      tablespec batch-convert split-tables/ tables/ --format json

    """
    converter = UMFLoader()

    # Parse format
    if format.lower() in ("split", "s"):
        target_format = UMFFormat.SPLIT
    elif format.lower() in ("json", "j"):
        target_format = UMFFormat.JSON
    else:
        console.print(f"[red]Error:[/red] Unknown format '{format}'. Use 'split' or 'json'.")
        raise typer.Exit(1)

    # Find files to convert
    if target_format == UMFFormat.SPLIT:
        # Converting TO split: find JSON files
        files = sorted(source_dir.rglob(pattern))
        # Also add .json files if pattern doesn't match them
        if "json" not in pattern:
            json_files = list(source_dir.rglob("*.umf.json"))
            files = sorted(set(files) | set(json_files))
    else:
        # Converting TO JSON: find split directories (have table.yaml)
        files = sorted({d.parent for d in source_dir.rglob("table.yaml")})

    if not files:
        console.print("[yellow]Warning:[/yellow] No files found matching pattern")
        return

    console.print(f"[cyan]Found {len(files)} files to convert[/cyan]")

    dest_dir.mkdir(parents=True, exist_ok=True)
    success_count = 0
    error_count = 0

    for file in files:
        try:
            # Determine output path
            if target_format == UMFFormat.SPLIT:
                # file.stem = "outreach_list.umf" or "outreach_list" -> "outreach_list"
                table_name = file.stem.replace(".umf", "").replace(".json", "")
                dest = dest_dir / table_name
            else:  # JSON
                # Convert .yaml/.json to .json
                table_name = file.stem.replace(".umf", "").replace(".json", "")
                dest = dest_dir / f"{table_name}.json"

            # Skip if exists and not force
            if dest.exists() and not force:
                console.print(f"  [yellow]SKIP[/yellow] {file} (exists, use --force)")
                continue

            console.print(f"  [dim]->[/dim] {file}")
            converter.convert(file, dest, target_format=target_format)
            success_count += 1

        except Exception as e:
            console.print(f"  [red]FAIL[/red] {file}: {e}")
            error_count += 1

    console.print()
    console.print(f"[green]Done.[/green] Complete: {success_count} converted", end="")
    if error_count:
        console.print(f", {error_count} errors", style="red")
    else:
        console.print()


@app.command()
def export_excel(
    source: Path = typer.Argument(
        ...,
        help="Source UMF file or directory",
        exists=True,
    ),
    dest: Path = typer.Argument(
        ...,
        help="Destination Excel file (.xlsx)",
    ),
    force: bool = typer.Option(
        False,
        "--force",
        help="Overwrite destination file if it already exists",
    ),
) -> None:
    """Export UMF to Excel workbook for human-friendly editing.

    Creates a structured Excel workbook with:
    - Data validation (dropdowns, constraints)
    - Helper columns (validation status, suggestions)
    - Instructions and examples
    - Support for all UMF features

    Examples:
      tablespec export-excel table.umf.yaml table.xlsx
      tablespec export-excel tables/medical_claims/ claims.xlsx --force

    """
    try:
        # Check if destination exists
        if dest.exists() and not force:
            console.print(f"[red]Error:[/red] File already exists: {dest}")
            console.print("[yellow]Use --force to overwrite[/yellow]")
            raise typer.Exit(1)

        # Load UMF
        console.print(f"[cyan]Loading UMF[/cyan] from {source}...")
        loader = UMFLoader()
        umf = loader.load(source)

        # Convert to Excel
        console.print("[cyan]Creating Excel workbook[/cyan]...")
        excel_converter = UMFToExcelConverter()
        workbook = excel_converter.convert(umf)

        # Delete existing file if force is True (openpyxl doesn't fully overwrite)
        if dest.exists() and force:
            dest.unlink()

        # Save
        workbook.save(dest)
        console.print(f"[green]Done.[/green] Excel workbook created: {dest}")
        console.print("[dim]Now you can open this file in Excel and make edits![/dim]")

    except FileNotFoundError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)
    except ValidationError as e:
        console.print("[red]Error:[/red] Invalid UMF")
        for error in e.errors()[:5]:
            console.print(f"  {error['loc']}: {error['msg']}")
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)


@app.command()
def import_excel(
    source: Path = typer.Argument(
        ...,
        help="Source Excel file (.xlsx)",
        exists=True,
    ),
    dest: Path = typer.Argument(
        ...,
        help="Destination directory (split format)",
    ),
    force: bool = typer.Option(
        False,
        "--force",
        help="Overwrite existing destination",
    ),
) -> None:
    """Import Excel workbook to UMF split format.

    Validates the Excel workbook and converts to UMF in split format
    (git-friendly directory structure):
    - Full validation with helpful error messages
    - Identifies incompatibilities before conversion
    - Supports round-trip conversion

    Examples:
      tablespec import-excel table.xlsx tables/outreach_list/

    """
    try:
        # Convert from Excel
        console.print(f"[cyan]Loading Excel workbook[/cyan] from {source}...")
        converter = ExcelToUMFConverter()
        umf, _metadata = converter.convert(source)  # Unpack tuple

        # Check if destination exists
        if dest.exists() and not force:
            console.print(f"[red]Error:[/red] {dest} already exists. Use --force to overwrite.")
            raise typer.Exit(1)

        # Handle --force by removing existing destination
        if dest.exists() and force:
            import shutil

            if dest.is_dir():
                shutil.rmtree(dest)
            else:
                dest.unlink()

        # Save in split format
        console.print(f"[cyan]Saving to[/cyan] {dest}...")
        loader = UMFLoader()
        loader.save(umf, dest, UMFFormat.SPLIT)

        console.print(f"[green]Done.[/green] UMF saved successfully: {dest}")

    except FileNotFoundError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)
    except ValidationError as e:
        console.print("[red]Invalid UMF:[/red]")
        for error in e.errors()[:5]:
            console.print(f"  {error['loc']}: {error['msg']}")
        raise typer.Exit(1)
    except ValueError as e:
        console.print(f"[red]Validation Error:[/red] {e}")
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)


@app.command()
def domains_list(
    format: str = typer.Option(
        "text",
        "--format",
        "-f",
        help="Output format: 'text' or 'json'",
    ),
) -> None:
    """List all registered domain types.

    Shows a table of all available domain types with their descriptions.

    Examples:
      tablespec domains-list
      tablespec domains-list --format json

    """
    try:
        registry = DomainTypeRegistry()
        domain_types = registry.list_domain_types()

        if format.lower() == "json":
            # JSON output
            import json

            data = []
            for name in domain_types:
                dt = registry.get_domain_type(name)
                if dt:
                    data.append(
                        {
                            "name": name,
                            "title": dt.get("name", ""),
                            "description": dt.get("description", ""),
                        }
                    )
            console.print(json.dumps(data, indent=2))
        else:
            # Text output with table
            if not domain_types:
                console.print("[yellow]No domain types found[/yellow]")
                return

            table = RichTable(show_header=True, show_lines=False, title="Domain Types")
            table.add_column("Name", style="cyan")
            table.add_column("Title", style="green")
            table.add_column("Description", style="white")

            for name in sorted(domain_types):
                dt = registry.get_domain_type(name)
                if dt:
                    title = dt.get("name", "")
                    description = dt.get("description", "")
                    # Truncate long descriptions
                    if len(description) > 60:
                        description = description[:57] + "..."
                    table.add_row(name, title, description)

            console.print(table)
            console.print(f"\n[dim]Total: {len(domain_types)} domain types[/dim]")

    except FileNotFoundError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)


@app.command()
def domains_show(
    name: str = typer.Argument(
        ...,
        help="Domain type name (e.g., 'us_state_code')",
    ),
    format: str = typer.Option(
        "yaml",
        "--format",
        "-f",
        help="Output format: 'yaml' or 'json'",
    ),
) -> None:
    """Show full definition of a domain type.

    Displays the complete YAML or JSON definition including detection rules,
    validation specifications, and sample generation methods.

    Examples:
      tablespec domains-show us_state_code
      tablespec domains-show email --format json
      tablespec domains-show npi

    """
    try:
        registry = DomainTypeRegistry()
        domain_type = registry.get_domain_type(name)

        if not domain_type:
            console.print(f"[red]Error:[/red] Domain type '{name}' not found")
            raise typer.Exit(1)

        if format.lower() == "json":
            import json

            console.print(json.dumps({name: domain_type}, indent=2))
        else:
            # YAML output
            import yaml

            output = yaml.dump({name: domain_type}, default_flow_style=False, sort_keys=False)
            console.print(output)

    except FileNotFoundError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)


@app.command()
def domains_infer(
    column_name: str = typer.Option(
        ...,
        "--column",
        "-c",
        help="Column name to analyze",
    ),
    description: str = typer.Option(
        None,
        "--description",
        "-d",
        help="Column description (optional)",
    ),
    samples: str = typer.Option(
        None,
        "--samples",
        "-s",
        help="Sample values (comma-separated, optional)",
    ),
) -> None:
    """Infer domain type for a column.

    Uses column name, description, and sample values to detect the most likely
    domain type and confidence score.

    Examples:
      tablespec domains-infer --column member_id
      tablespec domains-infer --column state --description "State code abbreviation"
      tablespec domains-infer --column phone --samples "5551234567,555-123-4567"

    """
    try:
        inference = DomainTypeInference()

        # Parse sample values
        sample_values = None
        if samples:
            sample_values = [s.strip() for s in samples.split(",")]

        # Infer
        domain_type, confidence = inference.infer_domain_type(
            column_name,
            description=description,
            sample_values=sample_values,
        )

        if domain_type:
            console.print(f"[green]Found[/green] Inferred domain type: [cyan]{domain_type}[/cyan]")
            console.print(f"  [dim]Confidence:[/dim] {confidence:.1%}")

            # Show details
            registry = DomainTypeRegistry()
            dt = registry.get_domain_type(domain_type)
            if dt:
                console.print(f"  [dim]Title:[/dim] {dt.get('name', '')}")
                console.print(f"  [dim]Description:[/dim] {dt.get('description', '')}")
        else:
            console.print("[yellow]No domain type found with high confidence[/yellow]")
            console.print("  Try providing more context (description, sample values)")

    except FileNotFoundError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)


@app.callback(invoke_without_command=True)
def version_callback(ctx: typer.Context) -> None:
    """Show version info or help."""
    if ctx.invoked_subcommand is None:
        if ctx.params.get("version"):
            from tablespec import __version__

            console.print(f"tablespec version {__version__}")
        else:
            console.print(ctx.get_help())
        raise typer.Exit


if __name__ == "__main__":
    app()
