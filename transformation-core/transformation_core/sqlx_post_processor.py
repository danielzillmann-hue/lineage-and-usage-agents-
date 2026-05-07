"""
SQLX Post-Processor

Applies consistent post-processing sanitization to all generated SQLX files,
regardless of source (stored procedures, Informatica, views, etc.).

This module centralizes quality improvements that are applied after LLM generation
to catch common issues and ensure consistent output quality.
"""

import re
import logging
from typing import Tuple, List, Dict

logger = logging.getLogger(__name__)

KNOWN_ACRONYMS = {'ID', 'ATS', 'EGM', 'IGT', 'ETL', 'SCD', 'BU', 'DT', 'FK', 'PK', 'BQ', 'UDS', 'CDW'}


def _to_pascal_case(name: str) -> str:
    """snake_case -> PascalCase with acronym preservation.

    gaming_date -> GamingDate
    site_id -> SiteID
    ats_rev_dy_id -> ATSRevDyID
    GamingDate -> GamingDate (no underscores = no change)
    """
    if '_' not in name:
        return name
    parts = [p for p in name.split('_') if p]
    result = []
    for part in parts:
        if part.upper() in KNOWN_ACRONYMS:
            result.append(part.upper())
        else:
            result.append(part[0].upper() + part[1:].lower() if len(part) > 1 else part.upper())
    return ''.join(result)


# Import shortcut mappings and shared transformation library
try:
    from transformation_core.external_declaration_generator import SHORTCUT_MAPPINGS
except ImportError:
    SHORTCUT_MAPPINGS = {}

try:
    from transformation_core.shared_transformation_library import get_library as get_shared_transformations
    SHARED_LIB_AVAILABLE = True
except ImportError:
    SHARED_LIB_AVAILABLE = False


class SQLXPostProcessor:
    """Post-processor for generated SQLX content."""

    def __init__(self, site_id_var: str = "${dataform.projectConfig.vars.site_id}",
                 property_id_var: str = "${dataform.projectConfig.vars.property_id}"):
        """Initialize the post-processor.

        Args:
            site_id_var: Dataform variable to use for site IDs
            property_id_var: Dataform variable to use for property IDs
        """
        self.site_id_var = site_id_var
        self.property_id_var = property_id_var

    # Tables excluded from BigQuery output — all refs, JOINs, and subqueries
    # referencing these tables are stripped from generated SQLX.
    # m_etl_jobdet: ETL job tracking metadata — not needed in Dataform pipeline
    EXCLUDED_TABLES = {'m_etl_jobdet'}

    def remove_excluded_table_refs(self, content: str) -> Tuple[str, List[str]]:
        """Remove all references to excluded tables from SQLX content.

        Strips:
        - Scalar subqueries: (SELECT ... FROM ${ref('m_etl_jobdet')} ...) AS alias
        - LEFT JOIN lines: LEFT JOIN ${ref('w_validationcodes')} AS WV\n  ON ...
        - Source comments mentioning these tables
        """
        warnings = []
        original = content

        for table in self.EXCLUDED_TABLES:
            # Match both single and double quoted refs: ${ref('table')} and ${ref("table")}
            ref_pattern = r"\$\{ref\(['\"]" + re.escape(table) + r"['\"]\)\}"

            # 1. Remove scalar subqueries that reference this table:
            #    (SELECT col FROM ${ref('table')} WHERE ...) AS alias
            #    These appear as a single column expression ending with ) AS <alias>
            #    Use greedy match from SELECT to the last )) AS pattern since
            #    subqueries may contain nested parens (IF, CASE, etc.)
            subquery_pattern = (
                r',?\s*\(SELECT\s+\w+\s+FROM\s+' + ref_pattern + r'\s+WHERE\s+.+?\)\s+AS\s+\w+'
            )
            new_content = re.sub(subquery_pattern, '', content, flags=re.IGNORECASE)
            if new_content != content:
                warnings.append(f"Removed scalar subquery referencing excluded table '{table}'")
                content = new_content

            # 2. Remove JOIN blocks (LEFT, INNER, CROSS) referencing this table.
            #    Handles: JOIN ... AS alias\n  ON ..., and JOIN ... AS alias ON ... (same line)
            for join_type in ['LEFT', 'INNER', 'CROSS']:
                # Multi-line: JOIN ref AS alias\n  ON ...
                pat = (
                    r'\n?\s*' + join_type + r'\s+JOIN\s+' + ref_pattern
                    + r'(?:\s+AS\s+\w+)?\s*\n\s*ON\s+[^\n]+'
                )
                new_content = re.sub(pat, '', content, flags=re.IGNORECASE)
                if new_content != content:
                    warnings.append(f"Removed {join_type} JOIN referencing excluded table '{table}'")
                    content = new_content
                # Same-line: JOIN ref AS alias ON ...
                pat2 = (
                    r'\n?\s*' + join_type + r'\s+JOIN\s+' + ref_pattern
                    + r'(?:\s+AS\s+\w+)?\s+ON\s+[^\n]+'
                )
                new_content = re.sub(pat2, '', content, flags=re.IGNORECASE)
                if new_content != content:
                    warnings.append(f"Removed {join_type} JOIN referencing excluded table '{table}'")
                    content = new_content

            # 2b. Handle plain JOIN (no LEFT/INNER/CROSS prefix)
            plain_join = (
                r'\n?\s*JOIN\s+' + ref_pattern + r'(?:\s+AS\s+\w+)?\s*\n?\s*ON\s+[^\n]+'
            )
            new_content = re.sub(plain_join, '', content, flags=re.IGNORECASE)
            if new_content != content:
                warnings.append(f"Removed JOIN referencing excluded table '{table}'")
                content = new_content

            # 3. Remove bare table references (without ${ref()}) in FROM clauses:
            #    "from m_etl_jobdet , other" -> "from other"
            bare_table = re.escape(table)
            content = re.sub(
                r'\b' + bare_table + r'\s*,\s*', '', content, flags=re.IGNORECASE
            )
            content = re.sub(
                r',\s*' + bare_table + r'\b', '', content, flags=re.IGNORECASE
            )
            # Remove WHERE/AND clauses referencing bare table.column (e.g., m_etl_jobdet.siteid = ...)
            content = re.sub(
                r'\n\s*(?:and)\s+' + bare_table + r'\.\w+\s*=\s*[^\n]+',
                '', content, flags=re.IGNORECASE
            )

            # 3b. Remove standalone ${ref()} lines (FROM, comma-separated)
            standalone_pattern = (
                r'\n?\s*(?:FROM|,)\s+' + ref_pattern + r'(?:\s+AS\s+\w+)?\s*(?=\n|$)'
            )
            new_content = re.sub(standalone_pattern, '', content, flags=re.IGNORECASE)
            if new_content != content:
                warnings.append(f"Removed standalone reference to excluded table '{table}'")
                content = new_content

            # 3c. Remove CROSS JOIN subqueries: CROSS JOIN (SELECT ... FROM ${ref('table')} ...) AS alias
            cross_join_subquery = (
                r'\n?\s*CROSS\s+JOIN\s+\(SELECT\s+.+?' + ref_pattern + r'.+?\)\s*(?:AS\s+\w+)?[^\n]*'
            )
            new_content = re.sub(cross_join_subquery, '', content, flags=re.IGNORECASE)
            if new_content != content:
                warnings.append(f"Removed CROSS JOIN subquery referencing excluded table '{table}'")
                content = new_content

            # 3d. Remove SET statements: SET var = (SELECT ... FROM ${ref('table')} ...)
            set_pattern = (
                r'\n?\s*SET\s+\w+\s*=\s*\(SELECT\s+.+?' + ref_pattern + r'.+?\)\s*;?'
            )
            new_content = re.sub(set_pattern, '', content, flags=re.IGNORECASE)
            if new_content != content:
                warnings.append(f"Removed SET statement referencing excluded table '{table}'")
                content = new_content

            # 3e. Remove UPDATE/INSERT INTO/DELETE FROM statements targeting this table
            for dml in ['UPDATE', 'INSERT\\s+INTO', 'DELETE\\s+FROM']:
                dml_pattern = (
                    r'\n?\s*' + dml + r'\s+' + ref_pattern + r'[^\n]*'
                )
                new_content = re.sub(dml_pattern, '', content, flags=re.IGNORECASE)
                if new_content != content:
                    warnings.append(f"Removed DML statement targeting excluded table '{table}'")
                    content = new_content

            # 3f. Remove WHERE clauses with bare table.column (no spaces around =)
            content = re.sub(
                r'\n\s*(?:where|and)\s+' + bare_table + r'\.\w+\s*=\s*[^\n]+',
                '', content, flags=re.IGNORECASE
            )

            # 3g. Remove any remaining lines that are just a ref to this table
            #     (continuation lines from multi-line statements, e.g., "FROM\n  ${ref('table')};")
            remaining_ref = r'\n\s*' + ref_pattern + r'\s*[;(]?\s*(?:\([^)]*\)\s*)?[;]?\s*$'
            new_content = re.sub(remaining_ref, '', content, flags=re.IGNORECASE | re.MULTILINE)
            if new_content != content:
                warnings.append(f"Removed remaining ref line for excluded table '{table}'")
                content = new_content

            # 4. Clean up source comments: remove table name from "-- Sources:" line only
            def _clean_sources_comment(m):
                line = m.group(0)
                cleaned = re.sub(
                    r',?\s*' + re.escape(table) + r'\s*,?',
                    ', ',
                    line,
                    flags=re.IGNORECASE
                )
                # Tidy up: remove leading/trailing comma-space artifacts
                cleaned = re.sub(r':\s*,\s*', ': ', cleaned)
                cleaned = re.sub(r',\s*$', '', cleaned)
                return cleaned
            content = re.sub(r'^-- Sources:.*$', _clean_sources_comment, content, flags=re.MULTILINE)

        # 5. Remove dangling column references to removed JOIN aliases.
        #    When a JOIN is removed (e.g., LEFT JOIN ... AS MEJ), columns like
        #    "MEJ.ETLJobID AS ETLJobDtlID" become invalid. Remove these lines.
        #    Known aliases: MEJ (m_etl_jobdet), WVC/WV (w_validationcodes)
        #    IMPORTANT: Only remove if the alias's JOIN is NOT present in the file.
        #    Deferred lookups (cte_deferred_joined) legitimately produce these JOINs.
        excluded_aliases = {'MEJ', 'WVC', 'WV', 'JD'}
        for alias in excluded_aliases:
            # Check if this alias actually exists as a JOIN in the file.
            # If "AS WVC" (or similar) appears in a JOIN clause, the alias is valid.
            alias_join_pat = re.compile(
                rf'\bAS\s+{re.escape(alias)}\b', re.IGNORECASE
            )
            if alias_join_pat.search(content):
                # Alias has a valid JOIN — don't strip its column references
                continue

            # Remove column lines like "alias.Column AS OutputName," or "alias.Column,"
            # Use negative lookbehind for ON/AND/OR to avoid stripping JOIN conditions.
            col_pat = (
                r'(?<!\bON )(?<!\bAND )(?<!\bOR )'
                r',?\s*\n?\s*' + re.escape(alias) + r'\.\w+(?:\s+AS\s+\w+)?\s*,?'
            )
            new_content = re.sub(col_pat, '', content, flags=re.IGNORECASE)
            if new_content != content:
                warnings.append(f"Removed dangling column reference for alias '{alias}'")
                content = new_content

        # 5b. Remove columns that were produced by excluded table lookups.
        #     ETLJobDtlID/EtlJobDtlID comes exclusively from m_etl_jobdet.
        #     These fields are legacy Sybase ETL tracking columns that have no
        #     equivalent in BigQuery — always strip them from output.
        #     ValidCodeDesc/VALIDCODEDESC comes from w_validationcodes — but ONLY
        #     when the WVC lookup JOIN was removed. If WVC JOIN is present (deferred
        #     lookups), ValidCodeDesc is a legitimate output column.
        has_wvc_join = bool(re.search(r'\bAS\s+WVC\b', content, re.IGNORECASE))
        excluded_columns = [r'ETLJobD(?:tl|et)ID', r'EtlJobD(?:tl|et)ID']
        if not has_wvc_join:
            excluded_columns.append(r'ValidCodeDesc')
        for col_pat in excluded_columns:
            # Match standalone column lines: "alias.Column AS OutputName," or "alias.Column,"
            # Must be on its own line (preceded by newline+whitespace) to avoid hitting ORDER BY refs.
            # Replaces the entire line (including newline) but NOT the preceding line's trailing comma.
            pat = r'\n[ \t]*,?[ \t]*\w+\.' + col_pat + r'(?:\s+AS\s+\w+)?\s*,?[ \t]*(?=\n|$)'
            new_content = re.sub(pat, '', content, flags=re.IGNORECASE)
            if new_content != content:
                warnings.append(f"Removed excluded-table-derived column matching '{col_pat}'")
                content = new_content
            # Also remove inline references in ORDER BY / PARTITION BY:
            # ", a.ETLJobDtlID DESC" or ", a.ETLJobDtlID ASC" -> ""
            order_pat = r',\s*\w+\.' + col_pat + r'\s+(?:ASC|DESC)\b'
            new_content = re.sub(order_pat, '', content, flags=re.IGNORECASE)
            if new_content != content:
                warnings.append(f"Removed excluded-column ORDER BY reference matching '{col_pat}'")
                content = new_content

        # Clean up artifacts: double commas, trailing commas before FROM, blank lines
        if content != original:
            content = re.sub(r',\s*,', ',', content)  # double commas
            content = re.sub(r',\s*\n(\s*FROM\b)', r'\n\1', content)  # trailing comma before FROM
            content = re.sub(r'\n\s*\n\s*\n', '\n\n', content)  # triple blank lines
            # Clean up leading commas after removal
            content = re.sub(r'SELECT\s*\n\s*,', 'SELECT\n ', content, flags=re.IGNORECASE)

        return content, warnings

    def process(self, content: str, source_name: str = "unknown") -> Tuple[str, List[str]]:
        """Apply all post-processing to SQLX content.

        Args:
            content: The SQLX content to process
            source_name: Name of the source file/procedure for logging

        Returns:
            Tuple of (processed_content, list_of_warnings)
        """
        all_warnings = []

        # 0-pre-pre. Strip issue scanner annotations (header blocks + inline markers)
        # The annotate_sqlx() function in issue_scanner.py adds diagnostic comments
        # that should not appear in production output
        content = self.strip_issue_annotations(content)

        # 0-pre. Remove excluded table references (m_etl_jobdet, w_validationcodes)
        # These tables are not needed in BigQuery — strip subqueries, JOINs, and columns
        content, excluded_warnings = self.remove_excluded_table_refs(content)
        all_warnings.extend(excluded_warnings)

        # 0a. Fix hardcoded BigQuery table references FIRST (before *= conversion)
        # This must run first to preserve backticked table names before operator replacement
        content, hardcoded_bq_warnings = self.fix_hardcoded_bq_tables(content)
        all_warnings.extend(hardcoded_bq_warnings)

        # 0b. Fix Sybase *= and =* outer join operators
        content, outer_join_warnings = self.fix_sybase_outer_join_operators(content)
        all_warnings.extend(outer_join_warnings)

        # 1. Sanitize Sybase-specific syntax
        content, sybase_warnings = self.sanitize_sybase_syntax(content)
        all_warnings.extend(sybase_warnings)

        # 1b. Fix invalid Dataform reference syntax (resolve, source -> ref)
        content, ref_warnings = self.fix_invalid_reference_syntax(content)
        all_warnings.extend(ref_warnings)

        # 1c. Fix missing commas between SELECT column expressions
        # Must run BEFORE CTE validation so column lists parse correctly
        content, comma_warnings = self.fix_missing_select_commas(content)
        all_warnings.extend(comma_warnings)

        # 1d. Fix FROM/JOIN glued to previous token (missing newline)
        # Template generator sometimes produces "aliaseFROM table" or "commentFROM cte"
        content, glued_from_warnings = self.fix_glued_from_keyword(content)
        all_warnings.extend(glued_from_warnings)

        # 2. Validate temp table refs (warning only)
        temp_warnings = self.validate_temp_table_refs(content)
        all_warnings.extend(temp_warnings)

        # 3. Fix duplicate JOIN aliases (e.g., DS, DS, DS -> DS, DS2, DS3)
        # IMPORTANT: This must use precise patterns to avoid renaming SQL keywords
        content, join_alias_warnings = self.fix_duplicate_join_aliases(content)
        all_warnings.extend(join_alias_warnings)

        # 3b. Detect duplicate JOINs by table name (warning only)
        join_warnings = self.detect_duplicate_joins(content)
        all_warnings.extend(join_warnings)

        # 4. Normalize site IDs
        content, site_changes = self.normalize_site_ids(content)
        all_warnings.extend(site_changes)

        # 4b. Normalize Dataform variable names to match workflow_settings.yaml
        content, var_changes = self.normalize_dataform_variables(content)
        all_warnings.extend(var_changes)

        # 4b2. Fix SUBSTR($$Property_no, 2, 3) legacy pattern
        content, property_warnings = self.fix_property_no_substr(content)
        all_warnings.extend(property_warnings)

        # 4c. Fix invalid SQL patterns (CASE True WHEN, IS NULL LIMIT, etc.)
        content, sql_fix_changes = self.fix_invalid_sql_patterns(content)
        all_warnings.extend(sql_fix_changes)

        # 4c2. Fix trailing comma before FROM (including when comment lines intervene)
        # Safety net for template generator leaving orphan comments in SELECT lists.
        # Upstream fix: sqlx_template_generator.py _generate_cte_joined() defers comments.
        content, trailing_comma_warnings = self.fix_trailing_comma_before_from(content)
        all_warnings.extend(trailing_comma_warnings)

        # 4d. Fix integer-as-boolean in CASE WHEN (DECODE(TRUE, int_col) → WHEN col = 1)
        content, int_bool_warnings = self.fix_integer_as_boolean(content)
        all_warnings.extend(int_bool_warnings)

        # 4e. Fix incorrect raw layer references (v_dacom_*_mel -> s_dacom_*)
        content, raw_ref_changes = self.fix_raw_layer_references(content)
        all_warnings.extend(raw_ref_changes)

        # 5. Detect comma joins (warning only)
        has_comma_joins = self.detect_comma_joins(content)
        if has_comma_joins:
            all_warnings.append("Old-style comma joins detected - consider converting to explicit JOINs")

        # 5b. Convert [REF: xxx] placeholders to ${ref('xxx')} in SQL code
        # This must run BEFORE escape_refs_in_comments
        content, ref_placeholder_changes = self.fix_ref_placeholders(content)
        all_warnings.extend(ref_placeholder_changes)

        # 6. Escape refs in comments
        content = self.escape_refs_in_comments(content)

        # 7. Remove schema prefixes before ${ref()} - e.g., sybaseadmin.${ref(...)}
        content, schema_warnings = self.remove_schema_prefixes(content)
        all_warnings.extend(schema_warnings)

        # 8. Fix bare table references - wrap known tables in ${ref()}
        content, bare_ref_warnings = self.fix_bare_table_references(content)
        all_warnings.extend(bare_ref_warnings)

        # 9. Detect SELECT * with multiple JOINs (warning only)
        select_star_warnings = self.detect_select_star_joins(content)
        all_warnings.extend(select_star_warnings)

        # 10. Fix circular/self references (replace schema.self with ${self()})
        content, circular_fixes = self.fix_circular_references(content, source_name)
        all_warnings.extend(circular_fixes)

        # 11. Validate and fix CTE column references
        content, column_warnings = self.fix_cte_column_references(content)
        all_warnings.extend(column_warnings)

        # 12. Replace shortcut references with target tables
        content, shortcut_warnings = self.replace_shortcut_references(content)
        all_warnings.extend(shortcut_warnings)

        # 13. Strip cdw_ prefix from refs (cdw_d_patron -> d_patron)
        content, cdw_warnings = self.strip_cdw_prefix_from_refs(content)
        all_warnings.extend(cdw_warnings)

        # 14. Fix CASE alias references in same SELECT (BigQuery doesn't allow this)
        content, case_alias_warnings = self.fix_case_alias_references(content)
        all_warnings.extend(case_alias_warnings)

        # 15. Remove ORDER BY from TABLE definitions (ignored by BigQuery)
        content, order_by_warnings = self.remove_order_by_from_tables(content)
        all_warnings.extend(order_by_warnings)

        # 15b. Remove ORDER BY from inside CTEs (BigQuery rejects ORDER BY without LIMIT in CTEs)
        content, cte_order_warnings = self.remove_order_by_from_ctes(content)
        all_warnings.extend(cte_order_warnings)

        # 16. Standardize key column types (SiteID->STRING, DayID->INT64, etc.)
        content, type_warnings = self.standardize_key_column_types(content)
        all_warnings.extend(type_warnings)

        # 17. Fix datetime columns cast as STRING -> TIMESTAMP
        content, datetime_warnings = self.fix_datetime_column_types(content)
        all_warnings.extend(datetime_warnings)

        # 18. Standardize column casing for common columns
        content, casing_warnings = self.standardize_column_casing(content)
        all_warnings.extend(casing_warnings)

        # 19. Fix staging table dependency cycles (remove ${ref()} from OPTIONS)
        content, cycle_warnings = self.fix_staging_table_cycles(content)
        all_warnings.extend(cycle_warnings)

        # 20. Fix invalid partition columns (STRING -> _ingestion_timestamp)
        content, partition_warnings = self.fix_invalid_partition_columns(content)
        all_warnings.extend(partition_warnings)

        # 21. Add missing semicolons to operations type files
        content, semicolon_warnings = self.fix_missing_semicolons(content)
        all_warnings.extend(semicolon_warnings)

        # 22. Fix unbalanced parentheses (stray closing parens at end of lines)
        # Run BEFORE procedure paren fix so we don't undo it
        content, paren_warnings = self.fix_unbalanced_parentheses(content)
        all_warnings.extend(paren_warnings)

        # 23. Fix missing closing paren in procedure parameters
        content, proc_paren_warnings = self.fix_procedure_missing_param_paren(content)
        all_warnings.extend(proc_paren_warnings)

        # 23b. Fix unclosed OPTIONS block before BEGIN
        content, options_warnings = self.fix_unclosed_options_block(content)
        all_warnings.extend(options_warnings)

        # 23c. Fix backtick-quoted procedure names wrapping ${self.schema}
        content, backtick_warnings = self.fix_backtick_procedure_names(content)
        all_warnings.extend(backtick_warnings)

        # 24. Add missing database property to declarations
        content, db_warnings = self.fix_declaration_missing_database(content)
        all_warnings.extend(db_warnings)

        # 24b. Quote unquoted database/schema values in declarations
        content, quote_warnings = self.fix_unquoted_declaration_values(content)
        all_warnings.extend(quote_warnings)

        # 25. Quote reserved keyword column names
        content, keyword_warnings = self.fix_reserved_keyword_columns(content)
        all_warnings.extend(keyword_warnings)

        # 26. Fix repeated alias prefixes (e.g., src.src.col -> src.col)
        content, repeated_prefix_warnings = self.fix_repeated_src_prefix(content)
        all_warnings.extend(repeated_prefix_warnings)

        # 26b. Fix IS NULL = TRUE/FALSE anti-patterns
        # IS NOT NULL = FALSE → IS NULL, IS NULL = TRUE → IS NULL, etc.
        content, null_compare_warnings = self.fix_is_null_comparison(content)
        all_warnings.extend(null_compare_warnings)

        # 27. Fix date format strings (yyyymmdd -> %Y%m%d)
        content, date_format_warnings = self.fix_date_format_strings(content)
        all_warnings.extend(date_format_warnings)

        # 28. Detect unused CTEs (warning only - risky to auto-remove)
        content, unused_cte_warnings = self.remove_unused_ctes(content)
        all_warnings.extend(unused_cte_warnings)

        # 29. Detect scalar subquery lookups (warning - suggest conversion to JOINs)
        scalar_subquery_warnings = self.detect_scalar_subquery_lookups(content)
        all_warnings.extend(scalar_subquery_warnings)

        # 29b. Wrap bare column/column division with SAFE_DIVIDE
        content, safe_div_warnings = self.fix_bare_division(content)
        all_warnings.extend(safe_div_warnings)

        # 30. Fix truncated SQL (add missing closing parens/END statements)
        content, truncation_warnings = self.fix_truncated_sql(content)
        all_warnings.extend(truncation_warnings)

        # 31. Fix missing uniqueKey columns in SELECT output
        content, uniquekey_warnings = self.fix_missing_uniquekey_columns(content)
        all_warnings.extend(uniquekey_warnings)

        # 32. Rename final SELECT output columns to PascalCase
        content, pascal_warnings = self.rename_final_select_columns_to_pascal_case(content)
        all_warnings.extend(pascal_warnings)

        # 33. Align clusterBy/partitionBy/uniqueKey with actual SELECT aliases
        # Must run AFTER PascalCase rename (step 32) so aliases are finalized
        content, config_col_warnings = self.fix_config_column_references(content)
        all_warnings.extend(config_col_warnings)

        # Log warnings
        if all_warnings:
            logger.warning(f"Post-processing warnings for {source_name}:")
            for warning in all_warnings:
                logger.warning(f"  - {warning}")

        return content, all_warnings

    def strip_issue_annotations(self, content: str) -> str:
        """Strip issue scanner annotations from SQLX content.

        The annotate_sqlx() function in issue_scanner.py adds:
        1. Header blocks: -- ========== ... REMAINING ISSUES ... -- ==========
        2. Inline markers: -- !! [C] FORWARD REF: col not in parent CTE
                           -- !! [H] star expansion conflict: ...
                           -- !! [W] LOW SIMILARITY (0%): ...
                           -- !! [I] EMPTY CTE (passthrough only)

        These are diagnostic annotations that should not appear in production output.
        """
        lines = content.split('\n')
        result = []
        in_header_block = False

        for line in lines:
            stripped = line.strip()

            # Detect header block boundaries
            if stripped.startswith('-- =====') and (in_header_block or 'REMAINING ISSUES' in '\n'.join(lines[max(0, len(result)-1):len(result)+5])):
                # Check if next few lines contain REMAINING ISSUES (start of block)
                if not in_header_block:
                    # Look ahead to confirm this is a scanner header, not a decorative separator
                    idx = lines.index(line, len(result))
                    look_ahead = '\n'.join(lines[idx:idx+5])
                    if 'REMAINING ISSUES' in look_ahead:
                        in_header_block = True
                        continue
                else:
                    # End of header block
                    in_header_block = False
                    continue

            # Skip lines inside header block
            if in_header_block:
                continue

            # Skip inline markers: -- !! [C/H/W/I] ...
            if re.match(r'^[\s]*-- !!\s*\[', stripped):
                continue

            # Skip stale TRUNCATED FILE warnings from previous post-processor runs
            if stripped.startswith('-- WARNING: TRUNCATED FILE'):
                continue
            if stripped == '-- This file appears incomplete and may need manual review or regeneration.':
                continue

            result.append(line)

        return '\n'.join(result)

    def fix_glued_from_keyword(self, content: str) -> Tuple[str, List[str]]:
        """Fix FROM/JOIN keywords glued to the previous token without whitespace.

        The template generator sometimes produces lines like:
          D_Site.SctyCde AS DSiteSctycdeFROM ${ref('table')}
          -- Post-join expressionsFROM cte_expressions AS src
          DG_2.LicID AS LicIDFROM cte_expressions AS src
          -- Deferred lookup resultsFROM cte_post_joined AS p

        This happens when the last SELECT column (or a comment line) is concatenated
        directly with the FROM clause without a newline separator. The bug exists in
        12+ locations in sqlx_template_generator.py across _generate_cte_source(),
        _generate_cte_lookup(), _build_expression_ctes(), _generate_simple_sq_cte().

        Fix: Insert a newline before FROM/JOIN/INNER/LEFT/RIGHT/CROSS/FULL when they
        appear directly after a non-whitespace character (not preceded by space/tab/newline).
        """
        warnings = []

        # Pattern: word char or ) or ' or } directly followed by FROM/JOIN keywords
        # Must NOT match inside words like "TRANSFORM" or "PERFORM"
        # The lookbehind ensures the char before FROM is not a letter that's part of
        # a legitimate word — we only split when FROM follows an alias, paren, comment, etc.
        # Negative cases to avoid: TRANSFORM, PERFORM, INFORM, UNIFORM
        keywords = ['FROM', 'INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'CROSS JOIN', 'FULL JOIN',
                     'LEFT OUTER JOIN', 'RIGHT OUTER JOIN', 'FULL OUTER JOIN']

        for keyword in keywords:
            # Match: non-whitespace char (not a letter) followed by the keyword
            # OR: a letter followed by the keyword where the letter is NOT part of
            # a known word ending (like transFORM, perFORM)
            # Simplest safe approach: match any non-whitespace before FROM,
            # but require the char before FROM to not form a valid word with FROM
            pattern = rf'(\S)((?:{re.escape(keyword)})\s)'
            for match in re.finditer(pattern, content, re.IGNORECASE):
                char_before = match.group(1)
                keyword_text = match.group(2)
                pos = match.start()

                # Skip if the char before + keyword form a valid SQL word
                # e.g., "TRANSFORM", "PERFORM", "CROSSFROM" (unlikely but safe)
                # Look back up to 10 chars to check for known words
                start_check = max(0, pos - 10)
                prefix = content[start_check:pos + 1]
                # Common words that end with letters before FROM: TRANSFORM, PERFORM, INFORM
                if re.search(r'(?:TRANS|PER|IN|UNI)$', prefix, re.IGNORECASE) and keyword_text.strip().upper() == 'FROM':
                    continue

                # Also skip if inside a ${ref()} or ${...} template expression
                # Check if we're between ${ and }
                last_open = content.rfind('${', 0, pos)
                last_close = content.rfind('}', 0, pos)
                if last_open > last_close:
                    continue

                # This is a genuine glued keyword — insert newline
                warnings.append(f"Fixed FROM/JOIN glued to previous token at position {pos}")

        # Now do the actual replacement (single pass)
        # Replace: non-whitespace followed by FROM/JOIN keywords with newline + indentation
        # Pre-compute config block boundary to skip matches inside config { ... }
        # The config block contains description strings where "FROM" is plain text,
        # not a SQL keyword. E.g., description: "table - bank (from CDW mapping)"
        config_block_end = self._get_config_block_end(content)

        def _fix_glued(m):
            char_before = m.group(1)
            keyword = m.group(2)

            # Skip matches inside the config block (descriptions, names, etc.)
            if config_block_end > 0 and m.start() < config_block_end:
                return m.group(0)

            # Skip TRANSFORM, PERFORM, INFORM, UNIFORM
            prefix_start = max(0, m.start() - 10)
            prefix_text = content[prefix_start:m.start() + 1]
            if re.search(r'(?:TRANS|PER|IN|UNI)$', prefix_text, re.IGNORECASE) and keyword.strip().upper().startswith('FROM'):
                return m.group(0)

            # Skip inside ${...} templates
            last_open = content.rfind('${', 0, m.start())
            last_close = content.rfind('}', 0, m.start())
            if last_open > last_close:
                return m.group(0)

            # Determine indentation from context — find the start of the current line
            line_start = content.rfind('\n', 0, m.start())
            if line_start == -1:
                line_start = 0
            else:
                line_start += 1
            current_line = content[line_start:m.start()]
            # Count leading whitespace of current line
            indent_match = re.match(r'^(\s*)', current_line)
            indent = indent_match.group(1) if indent_match else '    '

            return f"{char_before}\n{indent}{keyword}"

        # Build combined pattern for all keywords
        kw_pattern = '|'.join(re.escape(k) for k in keywords)
        combined_pattern = rf'(\S)((?:{kw_pattern})\s)'

        fix_count = 0

        def _fix_glued_counted(m):
            nonlocal fix_count
            result = _fix_glued(m)
            if result != m.group(0):
                fix_count += 1
            return result

        new_content = re.sub(combined_pattern, _fix_glued_counted, content, flags=re.IGNORECASE)

        if fix_count > 0:
            warnings = [f"Fixed FROM/JOIN glued to previous token ({fix_count} locations)"]

        return new_content, warnings

    def fix_integer_as_boolean(self, content: str) -> Tuple[str, List[str]]:
        """Fix CASE WHEN <column> THEN where column is an integer, not boolean.

        Informatica DECODE(TRUE, Is_EGM, ...) converts to CASE WHEN Is_EGM THEN ...
        but BigQuery requires a boolean condition. If Is_EGM is INT64, this fails.

        Pattern: WHEN alias.column THEN  (dot-qualified, no comparison operator)
        Fix:     WHEN alias.column = 1 THEN

        Also matches: WHEN variable_name THEN (bare name with underscores)
        Skip if: already has comparison operator (=, <, >, !=, IS, IN, LIKE, BETWEEN, NOT)
        Skip if: name is a known SQL keyword (TRUE, FALSE, NULL, EXISTS)
        """
        warnings = []

        # Match WHEN <name> THEN where name has no comparison operator
        # Dot-qualified: alias.col  OR  bare with underscore: var_name
        sql_keywords = {'TRUE', 'FALSE', 'NULL', 'NOT', 'EXISTS', 'ELSE', 'END',
                        'CASE', 'AND', 'OR', 'IN', 'IS', 'LIKE', 'BETWEEN'}

        def _fix_when(m):
            name = m.group(1)
            bare = name.split('.')[-1] if '.' in name else name
            if bare.upper() in sql_keywords:
                return m.group(0)
            # Check if this looks like a column/variable (has dot, underscore, or flag-like name)
            if '.' in name or '_' in bare:
                return f"WHEN {name} = 1 THEN"
            return m.group(0)

        # Pattern: WHEN <word or word.word> THEN (no operator between)
        # Negative lookbehind: skip CASE <expr> WHEN (simple CASE syntax uses value, not condition)
        new_content = re.sub(
            r'\bWHEN\s+((?:\w+\.)?\w+)\s+THEN\b',
            _fix_when,
            content,
            flags=re.IGNORECASE
        )

        if new_content != content:
            # Count fixes
            orig_count = len(re.findall(r'\bWHEN\s+(?:(?:\w+\.)?\w+)\s+THEN\b', content, re.IGNORECASE))
            new_count = len(re.findall(r'\bWHEN\s+(?:(?:\w+\.)?\w+)\s+THEN\b', new_content, re.IGNORECASE))
            fixed = orig_count - new_count
            if fixed > 0:
                warnings.append(f"Fixed {fixed} integer-as-boolean CASE WHEN conditions (added = 1)")

        return new_content, warnings

    def fix_missing_select_commas(self, content: str) -> Tuple[str, List[str]]:
        """Fix missing commas between SELECT column expressions.

        LLMs sometimes omit commas between column references in long SELECT
        lists, producing invalid SQL like:
            agg.NetRevRetlTency
            agg.BuyIn,

        This fix detects lines ending with an identifier (no comma) followed
        by a line starting with a column reference pattern, and adds the
        missing comma.

        Only fixes inside SELECT...FROM blocks. Skips lines that end with
        SQL keywords (FROM, WHERE, JOIN, ON, AND, OR, etc.) since those are
        clause boundaries, not missing commas.
        """
        warnings = []
        lines = content.split('\n')
        new_lines = []
        fix_count = 0

        # SQL keywords that legitimately end a line without a comma
        clause_keywords = {
            'from', 'where', 'join', 'on', 'and', 'or', 'not', 'in', 'as',
            'select', 'distinct', 'inner', 'left', 'right', 'outer', 'cross',
            'full', 'group', 'order', 'having', 'limit', 'union', 'all',
            'insert', 'into', 'update', 'delete', 'set', 'values', 'begin',
            'end', 'if', 'then', 'else', 'when', 'case', 'by', 'between',
            'exists', 'like', 'is', 'null', 'true', 'false', 'with', 'over',
            'partition', 'declare', 'create', 'replace', 'procedure', 'table',
            'temp', 'temporary', 'view', 'function', 'returns', 'options',
        }

        for i, line in enumerate(lines):
            stripped = line.rstrip()
            new_lines.append(line)

            # Skip empty lines, comments-only lines, last line
            if not stripped or stripped.lstrip().startswith('--') or i + 1 >= len(lines):
                continue

            # Strip inline comments before analyzing the SQL content
            # e.g., "t1.PatronNumber, --this field will be used..." → "t1.PatronNumber,"
            sql_part = re.sub(r'\s*--.*$', '', stripped)
            if not sql_part.strip():
                continue

            # Line must end with an identifier (no trailing comma, paren, semicolon, etc.)
            # Pattern: ends with word char, optionally preceded by alias.
            if not re.search(r'\w\s*$', sql_part):
                continue

            # Extract the last word on this line (from SQL part, not comment)
            last_word_match = re.search(r'(\w+)\s*$', sql_part)
            if not last_word_match:
                continue
            last_word = last_word_match.group(1).lower()

            # Skip if the last word is a SQL keyword
            if last_word in clause_keywords:
                continue

            # Check if line already ends with a comma (in the SQL part)
            if sql_part.rstrip().endswith(','):
                continue

            # Check the next non-empty line
            next_line = lines[i + 1].strip()
            if not next_line or next_line.startswith('--') or next_line.startswith('/*'):
                continue

            # Next line should start with a column reference pattern:
            # alias.Column or just ColumnName (with optional leading whitespace)
            next_is_column = re.match(
                r'(\w+\.)?(\w+)\s*(,|AS\b|\)?\s*$)', next_line, re.IGNORECASE
            )
            if not next_is_column:
                continue

            next_first_word = re.match(r'(\w+)', next_line)
            if next_first_word and next_first_word.group(1).lower() in clause_keywords:
                continue

            # Heuristic: this line is a column ref missing its trailing comma
            # Insert comma after the SQL part, preserving any inline comment
            comment_match = re.search(r'\s*--.*$', stripped)
            if comment_match:
                sql_end = comment_match.start()
                new_lines[-1] = stripped[:sql_end].rstrip() + ',' + stripped[sql_end:]
            else:
                new_lines[-1] = stripped + ','
            fix_count += 1

        if fix_count > 0:
            content = '\n'.join(new_lines)
            warnings.append(f"Fixed {fix_count} missing comma(s) between SELECT columns")

        return content, warnings

    def fix_repeated_src_prefix(self, content: str) -> Tuple[str, List[str]]:
        """Fix double-nested alias prefixes like src.src.col -> src.col.

        This is a safety net for cases where _convert_lookup_condition() or
        _build_from_clause() accidentally double-prefixes already-qualified columns.

        Returns:
            Tuple of (fixed_content, list_of_warnings)
        """
        warnings = []
        # Pattern: word.word. where both words are the same (case-insensitive)
        # e.g., src.src.col, DD.DD.DayID, FT.FT.Amount
        original = content
        content = re.sub(r'\b(\w+)\.\1\.', r'\1.', content, flags=re.IGNORECASE)
        if content != original:
            warnings.append("Fixed repeated alias prefix (e.g., src.src.col -> src.col)")
        return content, warnings

    def fix_missing_uniquekey_columns(self, content: str) -> Tuple[str, List[str]]:
        """Fix missing uniqueKey columns in SELECT output.

        When the config has uniqueKey: ["CASINOLOCDETID"] but the SELECT has
        j.C_CASINOLOCDETID (with a prefix), this adds an alias so the uniqueKey column
        is properly available in the output.

        Returns:
            Tuple of (fixed_content, list_of_warnings)
        """
        warnings = []

        # Skip if no config block
        config_match = re.search(r'config\s*\{([^}]+)\}', content, re.DOTALL)
        if not config_match:
            return content, warnings

        config_block = config_match.group(1)

        # Extract uniqueKey array from config
        unique_key_match = re.search(r'uniqueKey\s*:\s*\[([^\]]+)\]', config_block)
        if not unique_key_match:
            return content, warnings

        # Parse uniqueKey column names
        key_str = unique_key_match.group(1)
        unique_keys = [k.strip().strip('"').strip("'") for k in key_str.split(',')]
        unique_keys = [k for k in unique_keys if k]  # Remove empty strings

        if not unique_keys:
            return content, warnings

        # Find the final SELECT (not in CTEs)
        # Look for SELECT after the last WITH/CTE block ends, or the main SELECT
        sql_body = content[config_match.end():]

        # Find the final SELECT statement (after WITH block)
        final_select_match = None
        for m in re.finditer(r'\bSELECT\b', sql_body, re.IGNORECASE):
            # Check if this SELECT is after all CTE definitions
            # A final SELECT is typically the one that follows a closing paren from CTEs
            prefix = sql_body[:m.start()]
            # Count opening/closing parens to ensure we're at the outer level
            # The final SELECT typically follows WHERE the CTE parentheses are balanced
            if prefix.count('(') <= prefix.count(')'):
                final_select_match = m
                break

        if not final_select_match:
            # Fallback: find the last SELECT
            final_select_match = None
            for m in re.finditer(r'\bSELECT\b', sql_body, re.IGNORECASE):
                final_select_match = m
            if not final_select_match:
                return content, warnings

        # Extract SELECT column list
        select_start = final_select_match.start()

        # Find WHERE or end of statement to get SELECT columns
        from_match = re.search(r'\bFROM\b', sql_body[select_start:], re.IGNORECASE)
        if not from_match:
            return content, warnings

        select_columns_str = sql_body[select_start + 6:select_start + from_match.start()]

        # Parse column aliases (everything after AS, or the column name itself)
        # Column format: alias.col or alias.col AS output_name
        output_columns = set()
        for col_def in select_columns_str.split(','):
            col_def = col_def.strip()
            if not col_def:
                continue

            # Check for AS alias
            as_match = re.search(r'\bAS\s+(\w+)', col_def, re.IGNORECASE)
            if as_match:
                output_columns.add(as_match.group(1).upper())
            else:
                # No AS, use the column name (after dot if present)
                name_match = re.search(r'\.(\w+)\s*$', col_def)
                if name_match:
                    output_columns.add(name_match.group(1).upper())
                else:
                    # Bare column name
                    bare_match = re.search(r'\b(\w+)\s*$', col_def)
                    if bare_match:
                        output_columns.add(bare_match.group(1).upper())

        # Check which uniqueKey columns are missing
        missing_keys = []
        for key in unique_keys:
            if key.upper() not in output_columns:
                missing_keys.append(key)

        if not missing_keys:
            return content, warnings

        # For each missing key, look for a column with a matching suffix
        # e.g., if CASINOLOCDETID is missing, look for C_CASINOLOCDETID or P_CASINOLOCDETID
        additions = []
        for key in missing_keys:
            key_upper = key.upper()

            # Search for columns ending with the key name
            pattern = rf'\b(\w+)\.(\w+_{key_upper})\b'
            match = re.search(pattern, select_columns_str, re.IGNORECASE)
            if match:
                alias = match.group(1)
                full_col = match.group(2)
                additions.append(f'  {alias}.{full_col} AS {key}')
                warnings.append(f"Added uniqueKey column alias: {alias}.{full_col} AS {key}")
            else:
                # Try searching in the SQL body for any column matching
                # Look for alias.X_KEYNAME or alias.KEYNAME patterns with common table aliases
                found = False

                # 1. First try: look for prefixed version in the final SELECT's columns
                # Only search within the final SELECT, not the whole SQL body,
                # to avoid picking up CTE-scoped aliases (e.g., src.) that are
                # not in scope in the final SELECT.
                select_pattern = rf'\b(\w+)\.((?:\w+_)?{key_upper})\b'
                for select_match_inner in re.finditer(select_pattern, select_columns_str, re.IGNORECASE):
                    alias = select_match_inner.group(1)
                    full_col = select_match_inner.group(2)
                    additions.append(f'  {alias}.{full_col} AS {key}')
                    warnings.append(f"Added uniqueKey column alias: {alias}.{full_col} AS {key}")
                    found = True
                    break

                # 2. Second try: look for the exact column name in CTEs
                if not found:
                    # Extract the final SELECT's FROM alias
                    from_alias_match = re.search(r'\bFROM\s+\w+\s+AS\s+(\w+)', sql_body[select_start:], re.IGNORECASE)
                    final_alias = from_alias_match.group(1) if from_alias_match else 'j'

                    # Search for the column in any CTE
                    cte_col_pattern = rf'\b{key_upper}\b'
                    if re.search(cte_col_pattern, sql_body, re.IGNORECASE):
                        # Column exists somewhere - add with the final SELECT alias
                        additions.append(f'  {final_alias}.{key}')
                        warnings.append(f"Added uniqueKey column: {final_alias}.{key}")
                        found = True

                if not found:
                    warnings.append(f"WARNING: uniqueKey column '{key}' not found - manual review needed")

        if not additions:
            return content, warnings

        # Insert the additions into the SELECT clause
        # Find the position just before FROM
        from_pos = config_match.end() + select_start + from_match.start()

        # Insert before FROM, adding comma after existing columns
        insert_text = ',\n' + ',\n'.join(additions) + '\n'
        content = content[:from_pos] + insert_text + content[from_pos:]

        return content, warnings

    def fix_invalid_reference_syntax(self, content: str) -> Tuple[str, List[str]]:
        """Fix invalid Dataform reference syntax generated by LLM.

        The LLM sometimes generates invalid syntax like:
        - ${resolve("schema", "table")} -> should be ${ref('table')}
        - ${source('schema', 'table')} -> should be ${ref('table')}
        - ${resolve('table')} -> should be ${ref('table')}

        Returns:
            Tuple of (fixed_content, list_of_warnings)
        """
        warnings = []
        original = content

        # Fix ${resolve("schema", "table")} -> ${ref('table')}
        # Handles both double and single quotes
        pattern1 = r'\$\{resolve\s*\(\s*["\'][\w_]+["\']\s*,\s*["\'](\w+)["\']\s*\)\}'
        if re.search(pattern1, content, re.IGNORECASE):
            content = re.sub(pattern1, r"${ref('\1')}", content, flags=re.IGNORECASE)
            warnings.append("Fixed ${resolve('schema', 'table')} -> ${ref('table')}")

        # Fix ${resolve('table')} -> ${ref('table')} (single argument form)
        pattern2 = r'\$\{resolve\s*\(\s*["\'](\w+)["\']\s*\)\}'
        if re.search(pattern2, content, re.IGNORECASE):
            content = re.sub(pattern2, r"${ref('\1')}", content, flags=re.IGNORECASE)
            warnings.append("Fixed ${resolve('table')} -> ${ref('table')}")

        # Fix ${source('schema', 'table')} -> ${ref('table')}
        # Note: In dbt, source() is valid, but in Dataform we use ref() for everything
        pattern3 = r'\$\{source\s*\(\s*["\'][\w_]+["\']\s*,\s*["\'](\w+)["\']\s*\)\}'
        if re.search(pattern3, content, re.IGNORECASE):
            content = re.sub(pattern3, r"${ref('\1')}", content, flags=re.IGNORECASE)
            warnings.append("Fixed ${source('schema', 'table')} -> ${ref('table')}")

        # Fix ${source('table')} -> ${ref('table')} (single argument form)
        pattern4 = r'\$\{source\s*\(\s*["\'](\w+)["\']\s*\)\}'
        if re.search(pattern4, content, re.IGNORECASE):
            content = re.sub(pattern4, r"${ref('\1')}", content, flags=re.IGNORECASE)
            warnings.append("Fixed ${source('table')} -> ${ref('table')}")

        # Fix ${ref('schema', 'table')} -> ${ref('table')}
        # Dataform ref() only takes one argument (the table name), not schema
        pattern4b = r'\$\{ref\s*\(\s*["\'][\w_]+["\']\s*,\s*["\'](\w+)["\']\s*\)\}'
        if re.search(pattern4b, content, re.IGNORECASE):
            content = re.sub(pattern4b, r"${ref('\1')}", content, flags=re.IGNORECASE)
            warnings.append("Fixed ${ref('schema', 'table')} -> ${ref('table')}")

        # NOTE: Do NOT normalize ref names to lowercase. Dataform resolves ${ref()}
        # case-sensitively against the declaration 'name' field, which must match
        # the actual BigQuery table name (e.g., "LinkConfig", "D_Site").
        # The generator's _get_ref_name() already produces the correct case.

        return content, warnings

    def sanitize_sybase_syntax(self, content: str) -> Tuple[str, List[str]]:
        """Convert remaining Sybase-specific syntax to BigQuery equivalents.

        Returns:
            Tuple of (sanitized_content, list_of_warnings)
        """
        warnings = []

        # @@rowcount -> @@row_count (BigQuery scripting syntax)
        if '@@rowcount' in content.lower():
            content = re.sub(r'@@rowcount', '@@row_count', content, flags=re.IGNORECASE)
            warnings.append("Converted @@rowcount to @@row_count (BigQuery scripting)")

        # @@error -> BigQuery uses EXCEPTION handling
        # Replace with valid BigQuery syntax to avoid hard failures
        if '@@error' in content.lower():
            content = re.sub(
                r'@@error\.message',
                'ERROR_MESSAGE() /* BigQuery exception handling */',
                content,
                flags=re.IGNORECASE
            )
            # Handle other @@error properties that don't exist in BigQuery
            content = re.sub(
                r'@@error\.stack_trace',
                "'N/A' /* BigQuery: use EXCEPTION block for stack trace */",
                content,
                flags=re.IGNORECASE
            )
            content = re.sub(
                r'@@error\.formatted_stack_trace',
                "'N/A' /* BigQuery: use EXCEPTION block for formatted stack trace */",
                content,
                flags=re.IGNORECASE
            )
            content = re.sub(
                r'@@error\.statement_text',
                "'N/A' /* BigQuery: statement text not available outside EXCEPTION */",
                content,
                flags=re.IGNORECASE
            )
            # Replace plain @@error with a valid BigQuery expression
            # Use 0 (no error) as the default, with a TODO comment
            content = re.sub(
                r'@@error\b(?!\.)',
                '0 /* TODO: Convert to BEGIN...EXCEPTION pattern */',
                content,
                flags=re.IGNORECASE
            )
            warnings.append("Replaced @@error with BigQuery-compatible syntax (needs EXCEPTION block)")

        # @@transaction_active -> Not directly available in BigQuery
        if '@@transaction_active' in content.lower():
            content = re.sub(
                r'@@transaction_active',
                '/* TODO: BigQuery has implicit transactions */ TRUE',
                content,
                flags=re.IGNORECASE
            )
            warnings.append("Converted @@transaction_active to TRUE (BigQuery has implicit transactions)")

        # Fix ${ref()} inside OPTIONS description strings - they get compiled by Dataform
        # Convert them to plain text references
        def escape_refs_in_options(match):
            options_block = match.group(0)
            # Replace ${ref('name')} with just 'name' inside OPTIONS blocks
            fixed = re.sub(r'\$\{ref\([\'"]([^\'"]+)[\'"]\)\}', r'\1', options_block)
            return fixed

        content = re.sub(
            r'OPTIONS\s*\([^)]*\)',
            escape_refs_in_options,
            content,
            flags=re.IGNORECASE | re.DOTALL
        )

        # @@identity -> BigQuery doesn't have identity
        if '@@identity' in content.lower():
            content = re.sub(
                r'@@identity',
                '/* TODO: Use GENERATE_UUID() or explicit sequence */ GENERATE_UUID()',
                content,
                flags=re.IGNORECASE
            )
            warnings.append("Converted @@identity to GENERATE_UUID()")

        # @@fetch_status -> Should be converted to set-based operations
        if '@@fetch_status' in content.lower():
            content = re.sub(
                r'@@fetch_status',
                '/* TODO: Convert cursor to set-based operation */ 0',
                content,
                flags=re.IGNORECASE
            )
            warnings.append("Found @@fetch_status - cursor should be converted to set-based operation")

        # @@transaction_id -> BigQuery doesn't have transaction IDs;
        # in EXCEPTION blocks, the check is always true (transaction is active)
        if '@@transaction_id' in content.lower():
            content = re.sub(
                r'IF\s+@@transaction_id\s+IS\s+NOT\s+NULL\s+THEN',
                '/* BigQuery: transaction always active in EXCEPTION block */\n  IF TRUE THEN',
                content,
                flags=re.IGNORECASE
            )
            # Catch any remaining standalone usage
            content = re.sub(
                r'@@transaction_id',
                '/* TODO: BigQuery has no @@transaction_id */ NULL',
                content,
                flags=re.IGNORECASE
            )
            warnings.append("Converted @@transaction_id (BigQuery has implicit transactions)")

        # @@session_id -> BigQuery doesn't have @@session_id;
        # use SESSION_USER() for user identity or INFORMATION_SCHEMA for session info
        if '@@session_id' in content.lower():
            content = re.sub(
                r'@@session_id',
                'CAST(NULL AS STRING) /* TODO: Replace with session-specific logic */',
                content,
                flags=re.IGNORECASE
            )
            warnings.append("Converted @@session_id (BigQuery has no direct equivalent)")

        # Transaction control statements
        if re.search(r'\bBEGIN\s+TRANSACTION\b', content, re.IGNORECASE):
            content = re.sub(
                r'\bBEGIN\s+TRANSACTION\b',
                '/* BEGIN TRANSACTION - BigQuery has implicit transactions */',
                content,
                flags=re.IGNORECASE
            )
            warnings.append("Removed BEGIN TRANSACTION (BigQuery has implicit transactions)")

        if re.search(r'\bCOMMIT\s+TRANSACTION\b', content, re.IGNORECASE):
            content = re.sub(
                r'\bCOMMIT\s+TRANSACTION\b',
                '/* COMMIT - BigQuery auto-commits */',
                content,
                flags=re.IGNORECASE
            )
            warnings.append("Removed COMMIT TRANSACTION (BigQuery auto-commits)")

        if re.search(r'\bROLLBACK\s+TRANSACTION\b', content, re.IGNORECASE):
            content = re.sub(
                r'\bROLLBACK\s+TRANSACTION\b',
                'RAISE USING MESSAGE = "Rollback requested" /* TODO: Use EXCEPTION handling */',
                content,
                flags=re.IGNORECASE
            )
            warnings.append("Converted ROLLBACK TRANSACTION to RAISE")

        # ROWNUM -> ROW_NUMBER() OVER ()
        # Sybase/Oracle ROWNUM is a pseudo-column for row numbering
        if re.search(r'\bROWNUM\b', content, re.IGNORECASE):
            # Simple ROWNUM in WHERE clause: WHERE ROWNUM <= 10 -> use LIMIT
            content = re.sub(
                r'\bWHERE\s+ROWNUM\s*<=?\s*(\d+)',
                r'LIMIT \1',
                content,
                flags=re.IGNORECASE
            )
            # ROWNUM as a column -> ROW_NUMBER() OVER ()
            content = re.sub(
                r'\bROWNUM\b(?!\s*<=?\s*\d)',
                'ROW_NUMBER() OVER ()',
                content,
                flags=re.IGNORECASE
            )
            warnings.append("Converted ROWNUM to ROW_NUMBER() OVER () or LIMIT")

        # CONVERT(type, value) -> CAST(value AS type)
        # Sybase: CONVERT(VARCHAR(10), column)
        # BigQuery: CAST(column AS STRING)
        # Note: Need to handle nested parentheses like CONVERT(VARCHAR(10), col)
        if re.search(r'\bCONVERT\s*\(', content, re.IGNORECASE):
            # Map Sybase types to BigQuery types
            type_map = {
                'VARCHAR': 'STRING', 'CHAR': 'STRING', 'NVARCHAR': 'STRING', 'TEXT': 'STRING',
                'INT': 'INT64', 'INTEGER': 'INT64', 'SMALLINT': 'INT64', 'BIGINT': 'INT64', 'TINYINT': 'INT64',
                'DECIMAL': 'NUMERIC', 'NUMERIC': 'NUMERIC', 'FLOAT': 'FLOAT64', 'REAL': 'FLOAT64', 'DOUBLE': 'FLOAT64', 'MONEY': 'NUMERIC',
                'DATE': 'DATE', 'DATETIME': 'TIMESTAMP', 'TIMESTAMP': 'TIMESTAMP', 'SMALLDATETIME': 'TIMESTAMP',
                'BIT': 'BOOL', 'BOOLEAN': 'BOOL',
                'BINARY': 'BYTES', 'VARBINARY': 'BYTES', 'IMAGE': 'BYTES',
            }

            def find_convert_and_replace(text):
                """Find and replace CONVERT() handling nested parentheses."""
                result = []
                i = 0
                converted_count = 0
                while i < len(text):
                    # Look for CONVERT keyword (case-insensitive)
                    if text[i:i+7].upper() == 'CONVERT' and (i == 0 or not text[i-1].isalnum()):
                        # Skip whitespace to find opening paren
                        j = i + 7
                        while j < len(text) and text[j].isspace():
                            j += 1
                        if j < len(text) and text[j] == '(':
                            # Found CONVERT( - now find matching closing paren
                            paren_count = 1
                            start_args = j + 1
                            k = start_args
                            while k < len(text) and paren_count > 0:
                                if text[k] == '(':
                                    paren_count += 1
                                elif text[k] == ')':
                                    paren_count -= 1
                                k += 1
                            if paren_count == 0:
                                # Found complete CONVERT(...) - extract args
                                args_str = text[start_args:k-1]
                                # Split by first comma at depth 0
                                depth = 0
                                comma_pos = -1
                                for idx, c in enumerate(args_str):
                                    if c == '(':
                                        depth += 1
                                    elif c == ')':
                                        depth -= 1
                                    elif c == ',' and depth == 0:
                                        comma_pos = idx
                                        break
                                if comma_pos > 0:
                                    datatype = args_str[:comma_pos].strip()
                                    expression = args_str[comma_pos+1:].strip()
                                    # Extract base type (ignore size like VARCHAR(10))
                                    base_type = re.sub(r'\([^)]*\)', '', datatype).upper().strip()
                                    bq_type = type_map.get(base_type, base_type)
                                    result.append(f'CAST({expression} AS {bq_type})')
                                    converted_count += 1
                                    i = k
                                    continue
                    result.append(text[i])
                    i += 1
                return ''.join(result), converted_count

            content, convert_count = find_convert_and_replace(content)
            if convert_count > 0:
                warnings.append(f"Converted {convert_count} CONVERT() to CAST()")

        # DATEADD(interval, number, date) -> DATE_ADD(date, INTERVAL number interval)
        # Sybase: DATEADD("dd", 90, @EndDt) or DATEADD(day, 90, @EndDt)
        # BigQuery: DATE_ADD(EndDt, INTERVAL 90 DAY)
        if re.search(r'\bDATEADD\s*\(', content, re.IGNORECASE):
            interval_map = {
                'dd': 'DAY', 'day': 'DAY', 'd': 'DAY',
                'mm': 'MONTH', 'month': 'MONTH', 'm': 'MONTH',
                'yy': 'YEAR', 'year': 'YEAR', 'yyyy': 'YEAR', 'y': 'YEAR',
                'hh': 'HOUR', 'hour': 'HOUR', 'h': 'HOUR',
                'mi': 'MINUTE', 'minute': 'MINUTE', 'n': 'MINUTE',
                'ss': 'SECOND', 'second': 'SECOND', 's': 'SECOND',
                'wk': 'WEEK', 'week': 'WEEK', 'ww': 'WEEK',
                'qq': 'QUARTER', 'quarter': 'QUARTER', 'q': 'QUARTER',
            }

            def convert_dateadd(match):
                full_match = match.group(0)
                inner = match.group(1)
                # Split by comma - DATEADD(interval, number, date)
                parts = [p.strip() for p in inner.split(',')]
                if len(parts) == 3:
                    interval_str = parts[0].strip('\'"').lower()
                    number = parts[1]
                    date_expr = parts[2]
                    bq_interval = interval_map.get(interval_str, 'DAY')
                    return f'DATE_ADD({date_expr}, INTERVAL {number} {bq_interval})'
                return full_match

            content = re.sub(
                r'\bDATEADD\s*\(([^)]+)\)',
                convert_dateadd,
                content,
                flags=re.IGNORECASE
            )
            warnings.append("Converted DATEADD() to DATE_ADD()")

        # DATEDIFF(interval, start, end) -> DATE_DIFF(end, start, interval)
        # Sybase: DATEDIFF(day, start_date, end_date)
        # BigQuery: DATE_DIFF(end_date, start_date, DAY)
        if re.search(r'\bDATEDIFF\s*\(', content, re.IGNORECASE):
            interval_map = {
                'dd': 'DAY', 'day': 'DAY', 'd': 'DAY',
                'mm': 'MONTH', 'month': 'MONTH', 'm': 'MONTH',
                'yy': 'YEAR', 'year': 'YEAR', 'yyyy': 'YEAR', 'y': 'YEAR',
                'hh': 'HOUR', 'hour': 'HOUR', 'h': 'HOUR',
                'mi': 'MINUTE', 'minute': 'MINUTE', 'n': 'MINUTE',
                'ss': 'SECOND', 'second': 'SECOND', 's': 'SECOND',
                'wk': 'WEEK', 'week': 'WEEK', 'ww': 'WEEK',
            }

            def convert_datediff(match):
                full_match = match.group(0)
                inner = match.group(1)
                parts = [p.strip() for p in inner.split(',')]
                if len(parts) == 3:
                    interval_str = parts[0].strip('\'"').lower()
                    start_date = parts[1]
                    end_date = parts[2]
                    bq_interval = interval_map.get(interval_str, 'DAY')
                    return f'DATE_DIFF({end_date}, {start_date}, {bq_interval})'
                return full_match

            content = re.sub(
                r'\bDATEDIFF\s*\(([^)]+)\)',
                convert_datediff,
                content,
                flags=re.IGNORECASE
            )
            warnings.append("Converted DATEDIFF() to DATE_DIFF()")

        # IS_DATE(expr) -> (SAFE.PARSE_DATE('%Y%m%d', expr) IS NOT NULL)
        # Informatica function not available in BigQuery
        if re.search(r'\bIS_DATE\s*\(', content, re.IGNORECASE):
            # Handle IS_DATE with optional format parameter
            content = re.sub(
                r'\bIS_DATE\s*\(\s*([^,)]+)\s*(?:,\s*[^\)]+)?\s*\)',
                r"(SAFE.PARSE_DATE('%Y%m%d', \1) IS NOT NULL)",
                content,
                flags=re.IGNORECASE
            )
            warnings.append("Converted IS_DATE() to SAFE.PARSE_DATE() IS NOT NULL")

        # IS_NUMBER(expr) -> (SAFE_CAST(expr AS NUMERIC) IS NOT NULL)
        # Informatica function not available in BigQuery
        if re.search(r'\bIS_NUMBER\s*\(', content, re.IGNORECASE):
            content = re.sub(
                r'\bIS_NUMBER\s*\(\s*([^)]+)\s*\)',
                r'(SAFE_CAST(\1 AS NUMERIC) IS NOT NULL)',
                content,
                flags=re.IGNORECASE
            )
            warnings.append("Converted IS_NUMBER() to SAFE_CAST() IS NOT NULL")

        # IS_NUMERIC (alias for IS_NUMBER)
        if re.search(r'\bIS_NUMERIC\s*\(', content, re.IGNORECASE):
            content = re.sub(
                r'\bIS_NUMERIC\s*\(\s*([^)]+)\s*\)',
                r'(SAFE_CAST(\1 AS NUMERIC) IS NOT NULL)',
                content,
                flags=re.IGNORECASE
            )
            warnings.append("Converted IS_NUMERIC() to SAFE_CAST() IS NOT NULL")

        # CHR(39) / CHAR(39) -> "'" (special case for single quote character)
        if re.search(r'\b(?:CHR|CHAR)\s*\(\s*39\s*\)', content, re.IGNORECASE):
            content = re.sub(
                r"\b(?:CHR|CHAR)\s*\(\s*39\s*\)",
                "\"'\"",
                content,
                flags=re.IGNORECASE
            )
            warnings.append("Converted CHR/CHAR(39) to single-quote string literal")

        # General CHR(n) / CHAR(n) -> CODE_POINTS_TO_STRING([n])
        # (must run after the special-case above)
        if re.search(r'\b(?:CHR|CHAR)\s*\(\s*\d+\s*\)', content, re.IGNORECASE):
            content = re.sub(
                r'\b(?:CHR|CHAR)\s*\(\s*(\d+)\s*\)',
                r'CODE_POINTS_TO_STRING([\1])',
                content,
                flags=re.IGNORECASE
            )
            warnings.append("Converted CHR/CHAR(n) to CODE_POINTS_TO_STRING([n])")

        # SUBSTR(expr, 0, ...) -> SUBSTR(expr, 1, ...)
        # BigQuery SUBSTR is 1-based; a start position of 0 is always wrong.
        # Only targets literal 0 start position. Other positions are left as-is
        # because Informatica SUBSTR is already 1-based.
        if re.search(r'\bSUBSTR\s*\([^,]+,\s*0\s*,', content, re.IGNORECASE):
            content = re.sub(
                r'(\bSUBSTR\s*\([^,]+,\s*)0(\s*,)',
                r'\g<1>1\2',
                content,
                flags=re.IGNORECASE
            )
            warnings.append("Fixed SUBSTR 0-based indexing: SUBSTR(expr, 0, ...) -> SUBSTR(expr, 1, ...)")

        # GETDATE() -> CURRENT_TIMESTAMP
        # Sybase GETDATE() returns current datetime
        if re.search(r'\bGETDATE\s*\(\s*\)', content, re.IGNORECASE):
            content = re.sub(
                r'\bGETDATE\s*\(\s*\)',
                'CURRENT_TIMESTAMP',
                content,
                flags=re.IGNORECASE
            )
            warnings.append("Converted GETDATE() to CURRENT_TIMESTAMP")

        # ISNULL(a, b) -> IFNULL(a, b)
        # Sybase/T-SQL ISNULL is equivalent to BigQuery IFNULL
        # Careful: only match ISNULL as a function call (with opening paren)
        if re.search(r'\bISNULL\s*\(', content, re.IGNORECASE):
            content = re.sub(
                r'\bISNULL\s*\(',
                'IFNULL(',
                content,
                flags=re.IGNORECASE
            )
            warnings.append("Converted ISNULL() to IFNULL()")

        # CHARINDEX(search, string) -> STRPOS(string, search)
        # Sybase argument order is reversed compared to BigQuery
        if re.search(r'\bCHARINDEX\s*\(', content, re.IGNORECASE):
            def convert_charindex(match):
                args = match.group(1)
                # Split by first comma at depth 0
                depth = 0
                comma_pos = -1
                for idx, c in enumerate(args):
                    if c == '(':
                        depth += 1
                    elif c == ')':
                        depth -= 1
                    elif c == ',' and depth == 0:
                        comma_pos = idx
                        break
                if comma_pos > 0:
                    search_str = args[:comma_pos].strip()
                    # Check for optional third arg (start position)
                    rest = args[comma_pos + 1:]
                    depth = 0
                    second_comma = -1
                    for idx, c in enumerate(rest):
                        if c == '(':
                            depth += 1
                        elif c == ')':
                            depth -= 1
                        elif c == ',' and depth == 0:
                            second_comma = idx
                            break
                    if second_comma > 0:
                        # Has start position arg — use STRPOS with SUBSTR
                        string_expr = rest[:second_comma].strip()
                        return f'STRPOS({string_expr}, {search_str})'
                    else:
                        string_expr = rest.strip()
                        return f'STRPOS({string_expr}, {search_str})'
                return match.group(0)

            content = re.sub(
                r'\bCHARINDEX\s*\(([^)]+(?:\([^)]*\))*[^)]*)\)',
                convert_charindex,
                content,
                flags=re.IGNORECASE
            )
            warnings.append("Converted CHARINDEX() to STRPOS() (reversed argument order)")

        # NOLOCK hint removal
        # Sybase (NOLOCK) or WITH (NOLOCK) — not applicable in BigQuery
        if re.search(r'\bNOLOCK\b', content, re.IGNORECASE):
            content = re.sub(r'\s*\(\s*NOLOCK\s*\)', '', content, flags=re.IGNORECASE)
            content = re.sub(r'\s+WITH\s*\(\s*NOLOCK\s*\)', '', content, flags=re.IGNORECASE)
            content = re.sub(r'\bNOLOCK\b', '', content, flags=re.IGNORECASE)
            warnings.append("Removed NOLOCK hints (not applicable in BigQuery)")

        # Fix invalid CAST types (common Sybase/Oracle types that aren't valid in BigQuery)
        cast_type_map = {
            'CHAR': 'STRING',
            'VARCHAR': 'STRING',
            'NCHAR': 'STRING',
            'NVARCHAR': 'STRING',
            'TEXT': 'STRING',
            'NTEXT': 'STRING',
            'MONEY': 'NUMERIC',
            'SMALLMONEY': 'NUMERIC',
            'TINYINT': 'INT64',
            'SMALLINT': 'INT64',
            'INT': 'INT64',
            'INTEGER': 'INT64',
            'BIGINT': 'INT64',
            'REAL': 'FLOAT64',
            'DOUBLE': 'FLOAT64',
            'BIT': 'BOOL',
            'DATETIME2': 'DATETIME',
            'SMALLDATETIME': 'DATETIME',
            'NUMBER': 'NUMERIC',
            # Truncated types (from malformed source or parsing issues)
            'DAT': 'DATE',
            'INT6': 'INT64',
            'INT66': 'INT64',  # LLM typo: INT66 instead of INT64
            'STRIN': 'STRING',
            'FLOA': 'FLOAT64',
            'NUMER': 'NUMERIC',
            'DATET': 'DATETIME',
            'TIMES': 'TIMESTAMP',
            'NUMERI': 'NUMERIC',  # Another truncation variant
        }

        # Fix corrupted CAST patterns where closing paren gets inserted before last char(s)
        # This happens when type-with-precision (e.g., int(4), varchar(36)) is partially
        # converted: the type name is replaced but the precision+paren leaks through.
        # E.g., CAST(x AS int(4)) -> CAST(x AS INT64(4)) -> INT64)4) after bad paren strip
        # Also handles STRING)G) from varchar(36) partial conversion (36 -> hex-like remnant)
        corruption_fixes = [
            (r'\bNUMERI\)C\)', 'NUMERIC)', 'NUMERI)C) -> NUMERIC)'),
            (r'\bdatetim\)e\)', 'datetime)', 'datetim)e) -> datetime)'),
            (r'\bDATETIM\)E\)', 'DATETIME)', 'DATETIM)E) -> DATETIME)'),
            (r'\bTIMESTAM\)P\)', 'TIMESTAMP)', 'TIMESTAM)P) -> TIMESTAMP)'),
            (r'\bINT64\)\w+\)', 'INT64)', 'INT64)X) -> INT64)'),  # INT64)4) or INT64)N)
            (r'\bSTRING\)\w+\)', 'STRING)', 'STRING)X) -> STRING)'),  # STRING)G) from varchar(36)
            (r'\bFLOAT64\)\w+\)', 'FLOAT64)', 'FLOAT64)X) -> FLOAT64)'),
            (r'\bBOOL\)\w+\)', 'BOOL)', 'BOOL)X) -> BOOL)'),
            (r'\bDATE\)\w+\)', 'DATE)', 'DATE)X) -> DATE)'),
            (r'\bDATETIME\)\w+\)', 'DATETIME)', 'DATETIME)X) -> DATETIME)'),
            # Also fix leftover precision parens: INT64(4)) -> INT64)
            (r'\bINT64\s*\(\s*\d+\s*\)', 'INT64', 'INT64(N) -> INT64'),
            (r'\bSTRING\s*\(\s*\d+\s*\)', 'STRING', 'STRING(N) -> STRING'),
            (r'\bFLOAT64\s*\(\s*\d+\s*\)', 'FLOAT64', 'FLOAT64(N) -> FLOAT64'),
        ]
        for pattern, replacement, desc in corruption_fixes:
            if re.search(pattern, content, re.IGNORECASE):
                content = re.sub(pattern, replacement, content, flags=re.IGNORECASE)
                warnings.append(f"Fixed corrupted CAST type: {desc}")

        def fix_cast_type(match):
            expression = match.group(1)
            cast_type = match.group(2).upper().strip()
            # Extract base type (remove size like VARCHAR(10))
            base_type = re.sub(r'\([^)]*\)', '', cast_type).strip()
            bq_type = cast_type_map.get(base_type, cast_type)
            return f'CAST({expression} AS {bq_type})'

        # Pattern: CAST(expression AS type) or SAFE_CAST(expression AS type) where type might be invalid
        for old_type, new_type in cast_type_map.items():
            # Handle both CAST and SAFE_CAST
            pattern = rf'\b(SAFE_)?CAST\s*\(([^)]+)\s+AS\s+{old_type}(?:\([^)]*\))?\s*\)'
            if re.search(pattern, content, re.IGNORECASE):
                def replace_cast(m):
                    safe_prefix = m.group(1) or ''
                    expression = m.group(2)
                    return f'{safe_prefix}CAST({expression} AS {new_type})'
                content = re.sub(
                    pattern,
                    replace_cast,
                    content,
                    flags=re.IGNORECASE
                )
                warnings.append(f"Converted CAST type {old_type} to {new_type}")

        return content, warnings

    def validate_temp_table_refs(self, content: str) -> List[str]:
        """Check for ${ref()} calls to temporary tables that should be CTEs.

        Returns:
            List of warning messages for temp table refs found
        """
        warnings = []
        refs = re.findall(r"\$\{ref\(['\"]([^'\"]+)['\"]\)\}", content)
        temp_prefixes = ['tmp_', 'temp_', '#', '@']

        for ref in refs:
            ref_lower = ref.lower()
            for prefix in temp_prefixes:
                if ref_lower.startswith(prefix):
                    warnings.append(f"Temp table ref: ${{ref('{ref}')}} - should be CTE or CREATE TEMP TABLE")
                    break

        return warnings

    def detect_duplicate_joins(self, content: str) -> List[str]:
        """Detect potential duplicate JOIN clauses.

        Returns:
            List of warning messages for duplicate joins found
        """
        warnings = []
        join_pattern = r'\b(?:INNER|LEFT|RIGHT|FULL|CROSS)?\s*JOIN\s+(?:\$\{ref\([\'"]([^\'"]+)[\'"]\)\}|(\w+))\s+(?:AS\s+)?(\w+)?'
        joins = re.findall(join_pattern, content, re.IGNORECASE)

        table_counts = {}
        for ref_table, plain_table, alias in joins:
            table = ref_table or plain_table
            if table:
                table_lower = table.lower()
                table_counts[table_lower] = table_counts.get(table_lower, 0) + 1

        for table, count in table_counts.items():
            if count > 1:
                warnings.append(f"Table '{table}' joined {count} times - possible duplicate JOIN")

        return warnings

    def fix_duplicate_join_aliases(self, content: str) -> Tuple[str, List[str]]:
        """Fix duplicate table aliases in JOIN clauses by adding numeric suffixes.

        When the same alias is used for multiple JOINs (e.g., 'DS' used 3 times),
        this renames them to DS, DS2, DS3 and updates all column references.

        IMPORTANT: This method is carefully designed to ONLY modify table aliases,
        NOT SQL keywords. It uses precise patterns to:
        1. Find aliases only in JOIN...AS patterns
        2. Update references only when alias is followed by a dot (column access)
        3. Only update ON clause references that belong to the renamed JOIN
        4. Never touch SQL keywords like WHERE, ON, JOIN, etc.

        Returns:
            Tuple of (fixed_content, list_of_changes)
        """
        changes = []

        # SQL keywords that should NEVER be renamed
        SQL_KEYWORDS = {
            'select', 'from', 'where', 'and', 'or', 'on', 'join', 'left', 'right',
            'inner', 'outer', 'full', 'cross', 'as', 'in', 'is', 'not', 'null',
            'case', 'when', 'then', 'else', 'end', 'group', 'order', 'by', 'having',
            'limit', 'offset', 'union', 'except', 'intersect', 'with', 'distinct',
            'all', 'any', 'between', 'like', 'exists', 'true', 'false', 'asc', 'desc',
            'over', 'partition', 'rows', 'range', 'unbounded', 'preceding', 'following',
            'current', 'row', 'cast', 'safe_cast', 'if', 'ifnull', 'coalesce', 'nullif'
        }

        # Find all JOIN clauses with aliases - capture the full JOIN including ON clause
        # Pattern to find JOIN with its ON clause
        join_with_on_pattern = r'''
            \b((?:INNER\s+|LEFT\s+(?:OUTER\s+)?|RIGHT\s+(?:OUTER\s+)?|FULL\s+(?:OUTER\s+)?|CROSS\s+)?JOIN)\s+
            (?:\$\{ref\([\'"]([^\'"]+)[\'"]\)\}|([A-Za-z_][A-Za-z0-9_]*))\s+
            (?:AS\s+)?
            ([A-Za-z_][A-Za-z0-9_]*)
            (\s+ON\s+[^)]*?)?
            (?=\s*(?:LEFT|RIGHT|INNER|FULL|CROSS|JOIN|WHERE|GROUP|ORDER|HAVING|LIMIT|UNION|$|\n\s*\n))
        '''

        # Simpler pattern to just find JOIN aliases
        join_pattern = r'''
            \b((?:INNER\s+|LEFT\s+(?:OUTER\s+)?|RIGHT\s+(?:OUTER\s+)?|FULL\s+(?:OUTER\s+)?|CROSS\s+)?JOIN)\s+
            (?:\$\{ref\([\'"]([^\'"]+)[\'"]\)\}|([A-Za-z_][A-Za-z0-9_]*))\s+
            (?:AS\s+)?
            ([A-Za-z_][A-Za-z0-9_]*)
        '''

        # First pass: collect all aliases and their positions
        alias_occurrences = {}  # alias_upper -> list of occurrences

        for match in re.finditer(join_pattern, content, re.IGNORECASE | re.VERBOSE):
            alias = match.group(4)
            if alias:
                alias_upper = alias.upper()
                if alias.lower() in SQL_KEYWORDS:
                    continue
                if alias_upper not in alias_occurrences:
                    alias_occurrences[alias_upper] = []
                alias_occurrences[alias_upper].append({
                    'start': match.start(),
                    'end': match.end(),
                    'alias': alias,
                    'full_match': match.group(0)
                })

        # Find aliases that appear more than once
        duplicate_aliases = {k: v for k, v in alias_occurrences.items() if len(v) > 1}

        if not duplicate_aliases:
            return content, changes

        # Build a global rename map: (alias_upper, occurrence_index) -> new_alias
        global_rename_map = {}
        for alias_upper, occurrences in duplicate_aliases.items():
            original_alias = occurrences[0]['alias']
            for i, occ in enumerate(occurrences):
                if i == 0:
                    global_rename_map[(alias_upper, i)] = original_alias
                else:
                    global_rename_map[(alias_upper, i)] = f"{original_alias}{i + 1}"

        # Second pass: find each JOIN's ON clause scope
        # For each JOIN at position P, its ON clause extends from the alias to the next major keyword
        join_scopes = []  # list of (alias_upper, occurrence_index, start, end, new_alias)

        for alias_upper, occurrences in duplicate_aliases.items():
            for i, occ in enumerate(occurrences):
                join_end = occ['end']
                new_alias = global_rename_map[(alias_upper, i)]

                # Find where this JOIN's ON clause ends (next JOIN or major clause)
                remaining = content[join_end:]
                # Look for next major clause
                next_clause_match = re.search(
                    r'\b(?:LEFT|RIGHT|INNER|FULL|CROSS)\s+(?:OUTER\s+)?JOIN\b|\bWHERE\b|\bGROUP\b|\bORDER\b|\bHAVING\b|\bLIMIT\b|\bUNION\b',
                    remaining,
                    re.IGNORECASE
                )
                if next_clause_match:
                    scope_end = join_end + next_clause_match.start()
                else:
                    scope_end = len(content)

                join_scopes.append({
                    'alias_upper': alias_upper,
                    'occurrence_index': i,
                    'alias': occ['alias'],
                    'new_alias': new_alias,
                    'join_start': occ['start'],
                    'join_end': join_end,
                    'scope_end': scope_end
                })

        # Sort by position (descending) to process from end to beginning
        join_scopes.sort(key=lambda x: x['join_start'], reverse=True)

        # Process each JOIN scope
        for scope in join_scopes:
            if scope['occurrence_index'] == 0:
                continue  # First occurrence keeps original name

            old_alias = scope['alias']
            new_alias = scope['new_alias']
            join_start = scope['join_start']
            join_end = scope['join_end']
            scope_end = scope['scope_end']

            # 1. Replace the alias in the JOIN clause itself
            join_text = content[join_start:join_end]
            new_join_text = re.sub(
                rf'\b{re.escape(old_alias)}$',
                new_alias,
                join_text,
                flags=re.IGNORECASE
            )

            # 2. Replace alias.column references ONLY within this JOIN's ON clause scope
            on_clause = content[join_end:scope_end]
            updated_on_clause = re.sub(
                rf'\b{re.escape(old_alias)}\.([A-Za-z_][A-Za-z0-9_]*)',
                rf'{new_alias}.\1',
                on_clause,
                flags=re.IGNORECASE
            )

            # Reconstruct content
            content = content[:join_start] + new_join_text + updated_on_clause + content[scope_end:]

            changes.append(f"Renamed duplicate alias '{old_alias}' to '{new_alias}'")

        # Second pass: update alias references beyond the JOIN/ON scopes.
        # After the first pass, JOINs and ON clauses have correct aliases, but
        # WHERE, SELECT, GROUP BY, ORDER BY, HAVING may still reference old names.
        # Find the position after the last JOIN's ON clause, then replace in the rest.
        last_join_end = 0
        for match in re.finditer(
            r'\b(?:(?:INNER|LEFT|RIGHT|FULL|CROSS)\s+(?:OUTER\s+)?)?JOIN\b',
            content, re.IGNORECASE
        ):
            last_join_end = match.end()

        if last_join_end > 0:
            # Find the end of the last ON clause (next major clause after last JOIN)
            remaining_after_last_join = content[last_join_end:]
            post_on_match = re.search(
                r'\b(?:WHERE|GROUP\s+BY|ORDER\s+BY|HAVING|LIMIT|UNION|SELECT)\b',
                remaining_after_last_join,
                re.IGNORECASE
            )
            if post_on_match:
                rest_start = last_join_end + post_on_match.start()
            else:
                rest_start = len(content)

            if rest_start < len(content):
                before_rest = content[:rest_start]
                rest_sql = content[rest_start:]

                for alias_upper, occurrences in duplicate_aliases.items():
                    for i, occ in enumerate(occurrences):
                        if i == 0:
                            continue
                        old_alias = occ['alias']
                        new_alias = global_rename_map[(alias_upper, i)]
                        rest_sql = re.sub(
                            rf'\b{re.escape(old_alias)}\.([A-Za-z_][A-Za-z0-9_]*)',
                            rf'{new_alias}.\1',
                            rest_sql,
                            flags=re.IGNORECASE
                        )

                content = before_rest + rest_sql

        return content, changes

    def normalize_site_ids(self, content: str) -> Tuple[str, List[str]]:
        """Replace hardcoded site IDs with Dataform config variables.

        Returns:
            Tuple of (normalized_content, list_of_changes)
        """
        changes = []

        # Common patterns for hardcoded site IDs (3-digit numbers)
        site_id_patterns = [
            (r"(\bsite_?id\s*=\s*)(['\"]?\d{3}['\"]?)", self.site_id_var),
            (r"(\bsite\s*=\s*)(['\"]?\d{3}['\"]?)", self.site_id_var),
            (r"(\bproperty_?id\s*=\s*)(['\"]?\d{3}['\"]?)", self.property_id_var),
        ]

        for pattern, replacement_var in site_id_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            if matches:
                for match in matches:
                    changes.append(f"Replaced hardcoded {match[1]} with config variable")
                content = re.sub(pattern, r"\1" + replacement_var, content, flags=re.IGNORECASE)

        return content, changes

    def normalize_dataform_variables(self, content: str) -> Tuple[str, List[str]]:
        """Normalize Dataform variable names to match workflow_settings.yaml conventions.

        The workflow_settings.yaml uses snake_case variable names (site_id, etl_job_dtl_id)
        but LLM-generated SQL may use CamelCase (SiteID, SiteId) based on source column names.
        This normalization ensures consistency across all generated files.

        Returns:
            Tuple of (normalized_content, list_of_changes)
        """
        changes = []

        # Variable name mapping: incorrect variations -> correct yaml-defined name
        variable_name_mapping = {
            # Site-related variables - normalize to snake_case
            'SiteID': 'site_id',
            'SiteId': 'site_id',
            'siteid': 'site_id',
            'SITEID': 'site_id',
            'Site_ID': 'site_id',
            'Site_Id': 'site_id',
            # ETL tracking variables
            'ETLJobDtlID': 'etl_job_dtl_id',
            'etljobdtlid': 'etl_job_dtl_id',
            'ETLJobDtlId': 'etl_job_dtl_id',
            'EtlJobDtlId': 'etl_job_dtl_id',
            # ETLJobID casing variants -> correct yaml-defined name
            # workflow_settings.yaml defines "ETLJobID: -1" (case-sensitive lookup!)
            'EtlJobId': 'ETLJobID',
            'EtlJobID': 'ETLJobID',
            'ETLJobId': 'ETLJobID',
            'etljobid': 'ETLJobID',
            'ETLJOBID': 'ETLJobID',
            # Note: These variables are already correct in workflow_settings.yaml
            # and should NOT be changed:
            # - PtyLocNum (property location number)
            # - Property_no (property number)
            # - ETLJobID (ETL job ID)
            # - SctyCde (security code)
        }

        replacements_made = []

        for old_var, new_var in variable_name_mapping.items():
            if old_var == new_var:
                continue  # Skip if already correct

            # Pattern: ${dataform.projectConfig.vars.OldVar} -> ${dataform.projectConfig.vars.new_var}
            # Use case-insensitive matching for flexibility
            pattern = rf'\$\{{dataform\.projectConfig\.vars\.{re.escape(old_var)}\}}'
            matches = re.findall(pattern, content, flags=re.IGNORECASE)

            if matches:
                content = re.sub(
                    pattern,
                    f'${{dataform.projectConfig.vars.{new_var}}}',
                    content,
                    flags=re.IGNORECASE
                )
                replacements_made.append(f"{old_var} -> {new_var}")

        if replacements_made:
            changes.append(f"Normalized Dataform variable names: {', '.join(replacements_made)}")

        return content, changes

    def fix_property_no_substr(self, content: str) -> Tuple[str, List[str]]:
        """Replace SUBSTR($$Property_no, 2, 3) with direct CAST variable reference.

        Legacy Informatica used 4-digit property numbers (e.g., '1400').
        SUBSTR(...,2,3) extracted the meaningful part ('400').
        In BigQuery, Property_no is already the meaningful value (e.g., '400').

        Handles the pattern in ${dataform.projectConfig.vars.Property_no} format.
        """
        warnings = []
        # Match: SUBSTR(${dataform.projectConfig.vars.Property_no}, 2, 3) — with any spacing
        pattern = r'SUBSTR\s*\(\s*\$\{dataform\.projectConfig\.vars\.Property_no\}\s*,\s*2\s*,\s*3\s*\)'
        replacement = 'CAST(${dataform.projectConfig.vars.Property_no} AS INT64)'
        new_content = re.sub(pattern, replacement, content, flags=re.IGNORECASE)
        if new_content != content:
            warnings.append("Replaced SUBSTR($$Property_no,2,3) with CAST(Property_no AS INT64)")
        return new_content, warnings

    def fix_ref_placeholders(self, content: str) -> Tuple[str, List[str]]:
        """Convert [REF: xxx] placeholders to ${ref('xxx')} in SQL code.

        In comments (both -- and /* */), convert [REF: xxx] to just xxx.
        In code, convert [REF: xxx] to ${ref('xxx')}.

        Returns:
            Tuple of (fixed_content, list_of_changes)
        """
        changes = []
        ref_count = 0
        comment_ref_count = 0

        # First, handle block comments /* ... */ - convert [REF: xxx] to just xxx
        def replace_refs_in_block_comment(match):
            nonlocal comment_ref_count
            comment = match.group(0)
            refs = re.findall(r'\[REF:\s*([^\]]+)\]', comment)
            comment_ref_count += len(refs)
            return re.sub(r'\[REF:\s*([^\]]+)\]', r'\1', comment)

        content = re.sub(r'/\*.*?\*/', replace_refs_in_block_comment, content, flags=re.DOTALL)

        # Now process line by line for -- comments and code
        lines = content.split('\n')
        fixed_lines = []

        for line in lines:
            # Check if this line has a -- comment
            comment_pos = line.find('--')
            if comment_pos == -1:
                # No comment - check entire line
                code_part = line
                comment_part = ''
            else:
                # Has comment - process both parts differently
                code_part = line[:comment_pos]
                comment_part = line[comment_pos:]
                # In comments, convert [REF: xxx] to just xxx
                if '[REF:' in comment_part:
                    refs = re.findall(r'\[REF:\s*([^\]]+)\]', comment_part)
                    comment_ref_count += len(refs)
                    comment_part = re.sub(r'\[REF:\s*([^\]]+)\]', r'\1', comment_part)

            # Replace [REF: xxx] with ${ref('xxx')} in code part only
            if '[REF:' in code_part:
                matches = re.findall(r'\[REF:\s*([^\]]+)\]', code_part)
                ref_count += len(matches)
                new_code = re.sub(
                    r'\[REF:\s*([^\]]+)\]',
                    r"${ref('\1')}",
                    code_part
                )
                code_part = new_code

            fixed_lines.append(code_part + comment_part)

        content = '\n'.join(fixed_lines)

        if ref_count > 0:
            changes.append(f"Converted {ref_count} [REF: xxx] placeholder(s) to ${{ref('xxx')}}")
        if comment_ref_count > 0:
            changes.append(f"Cleaned {comment_ref_count} [REF: xxx] placeholder(s) in comments")

        return content, changes

    def fix_trailing_comma_before_from(self, content: str) -> Tuple[str, List[str]]:
        """Fix trailing comma before FROM, including when comment lines intervene.

        The template generator may produce patterns like:
            col_name,
            -- Post-join expressions (depend on lookup results)
            FROM cte_name AS src

        The comma after col_name is invalid because only a comment follows
        before FROM. This fix removes the trailing comma and preserves the comment.

        Also handles the simple case (no comment between comma and FROM).

        Upstream fix: _generate_cte_joined() in sqlx_template_generator.py now
        defers comment insertion until after expressions are resolved. This
        post-processor fix is a safety net.
        """
        warnings = []

        # Pattern: comma, optional whitespace/newlines, one or more comment lines, then FROM
        # Capture groups: (1) the comma to remove, (2) the comment+FROM block to keep
        new_content = re.sub(
            r',(\s*\n(?:\s*--[^\n]*\n)*\s*FROM\b)',
            r'\1',
            content
        )

        # Also handle simple case: comma directly before FROM (no comments)
        new_content = re.sub(r',\s*\n(\s*FROM\b)', r'\n\1', new_content)

        if new_content != content:
            warnings.append("Fixed trailing comma before FROM (with or without intervening comments)")

        return new_content, warnings

    def fix_invalid_sql_patterns(self, content: str) -> Tuple[str, List[str]]:
        """Fix invalid SQL patterns generated by LLM.

        Fixes:
        - CASE True WHEN -> CASE WHEN (invalid SQL)
        - IS NULL LIMIT 1) -> IS NULL (malformed syntax)
        - lkp.27 = 27 (invalid numeric column reference)
        - lkp.'String' = 'String' (invalid string as column)
        - Hardcoded BigQuery table references

        Returns:
            Tuple of (fixed_content, list_of_changes)
        """
        changes = []
        original = content

        # Fix 1: CASE True WHEN -> CASE WHEN
        if re.search(r'\bCASE\s+True\s+WHEN\b', content, re.IGNORECASE):
            content = re.sub(
                r'\bCASE\s+True\s+WHEN\b',
                'CASE WHEN',
                content,
                flags=re.IGNORECASE
            )
            changes.append("Fixed CASE True WHEN -> CASE WHEN")

        # Fix 2: IS NULL LIMIT pattern - handle various forms:
        # - (var IS NULL LIMIT 1)
        # - IF((var IS NULL LIMIT 1), ...)
        # - IF( (var IS NULL LIMIT 1) , ...)
        # - CASE True WHEN (var IS NULL LIMIT 1) THEN ...
        if re.search(r'IS\s+NULL\s+LIMIT\s+\d+', content, re.IGNORECASE):
            # First, fix all (var IS NULL LIMIT n) patterns to just (var IS NULL)
            content = re.sub(
                r'\(\s*(\w+)\s+IS\s+NULL\s+LIMIT\s+\d+\s*\)',
                r'(\1 IS NULL)',
                content,
                flags=re.IGNORECASE
            )
            changes.append("Fixed IS NULL LIMIT syntax")

        # Fix 2b: LIMIT inside function calls - remove misplaced LIMIT from inside functions
        # Pattern: SAFE.PARSE_DATE('format', value LIMIT 1) -> SAFE.PARSE_DATE('format', value)
        # Pattern: SAFE_CAST(value LIMIT 1 AS TYPE) -> SAFE_CAST(value AS TYPE)
        # This happens when LIMIT 1 from a subquery incorrectly gets placed inside a function
        if re.search(r'\b(?:SAFE[._])?(?:PARSE_DATE|PARSE_TIMESTAMP|CAST)\s*\([^)]*\bLIMIT\s+\d+', content, re.IGNORECASE):
            # Fix SAFE.PARSE_DATE/SAFE_PARSE_DATE with LIMIT inside
            content = re.sub(
                r'(SAFE[._]PARSE_(?:DATE|TIMESTAMP)\s*\(\s*[\'"][^\'\"]+[\'\"])\s*,\s*([^)]+?)\s+LIMIT\s+\d+\s*\)',
                r'\1, \2)',
                content,
                flags=re.IGNORECASE
            )
            # Fix SAFE_CAST/SAFE.CAST with LIMIT inside
            content = re.sub(
                r'(SAFE[._]CAST\s*\(\s*)([^)]+?)\s+LIMIT\s+\d+\s+(AS\s+\w+)\s*\)',
                r'\1\2 \3)',
                content,
                flags=re.IGNORECASE
            )
            changes.append("Removed misplaced LIMIT from inside function calls")

        # Fix 2c: LIMIT inside TRUNC/DATE_ADD/EXTRACT function calls
        # Pattern: TRUNC(DATE_ADD(v_CalendarDateTime LIMIT 1)) -> TRUNC(DATE_ADD(v_CalendarDateTime))
        # Pattern: EXTRACT('HH' FROM value LIMIT 1) -> EXTRACT('HH' FROM value)
        if re.search(r'\b(?:TRUNC|DATE_ADD|DATE_SUB|EXTRACT)\s*\([^)]*\bLIMIT\s+\d+', content, re.IGNORECASE):
            # Fix DATE_ADD/DATE_SUB/TRUNC with LIMIT inside
            content = re.sub(
                r'(\b(?:TRUNC|DATE_ADD|DATE_SUB)\s*\()([^)]+?)\s+LIMIT\s+\d+(\s*\))',
                r'\1\2\3',
                content,
                flags=re.IGNORECASE
            )
            # Fix EXTRACT with LIMIT inside - EXTRACT('format' FROM value LIMIT 1)
            content = re.sub(
                r'(EXTRACT\s*\(\s*[\'"][^\'\"]+[\'\"]?\s+FROM\s+)([^)]+?)\s+LIMIT\s+\d+(\s*\))',
                r'\1\2\3',
                content,
                flags=re.IGNORECASE
            )
            changes.append("Removed misplaced LIMIT from TRUNC/DATE_ADD/EXTRACT calls")

        # Fix 3: Invalid lookup column references (numeric)
        if re.search(r'\w+\.\d+\s*=\s*\d+', content):
            content = re.sub(
                r'(\w+)\.(\d+)\s*=\s*\2',
                r"/* FIXME: Invalid lookup \1.\2 */ TRUE",
                content
            )
            changes.append("Fixed invalid numeric lookup reference")

        # Fix 4: Invalid lookup column references (quoted string as column)
        if re.search(r"\w+\.'[^']+'\s*=\s*'[^']+'", content):
            content = re.sub(
                r"(\w+)\.'([^']+)'\s*=\s*'\2'",
                r"/* FIXME: Invalid lookup \1.'\2' */ TRUE",
                content
            )
            changes.append("Fixed invalid string lookup reference")

        # Fix 5: Invalid NULL comparisons (<> NULL, != NULL -> IS NOT NULL)
        # In SQL, "x <> NULL" always returns NULL (unknown), not TRUE/FALSE
        # Must use "x IS NOT NULL" instead
        if re.search(r'<>\s*NULL|!=\s*NULL', content, re.IGNORECASE):
            # Fix: col <> NULL -> col IS NOT NULL
            content = re.sub(
                r'(\w+(?:\.\w+)?)\s*<>\s*NULL',
                r'\1 IS NOT NULL',
                content,
                flags=re.IGNORECASE
            )
            # Fix: col != NULL -> col IS NOT NULL
            content = re.sub(
                r'(\w+(?:\.\w+)?)\s*!=\s*NULL',
                r'\1 IS NOT NULL',
                content,
                flags=re.IGNORECASE
            )
            changes.append("Fixed invalid NULL comparisons (<> NULL -> IS NOT NULL)")

        # Fix 5b: Invalid NULL equality (= NULL -> IS NULL)
        # Pattern: col = NULL (but not col = 'NULL' which is a string)
        if re.search(r'=\s*NULL\b(?!\s*[\'"])', content, re.IGNORECASE):
            content = re.sub(
                r'(\w+(?:\.\w+)?)\s*=\s*NULL\b(?!\s*[\'"])',
                r'\1 IS NULL',
                content,
                flags=re.IGNORECASE
            )
            changes.append("Fixed invalid NULL equality (= NULL -> IS NULL)")

        # Fix 7: Hardcoded BigQuery table references
        def camel_to_snake_local(name):
            """Convert CamelCase to snake_case."""
            s1 = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1_\2', name)
            s2 = re.sub(r'([a-z\d])([A-Z])', r'\1_\2', s1)
            return s2.lower()

        def convert_hardcoded_bq_table(match):
            full_match = match.group(0)
            project = match.group(1)
            dataset = match.group(2)
            table = match.group(3)

            dataset_lower = dataset.lower()
            if 'dacom' in dataset_lower:
                source = 'dacom'
            elif 'igt' in dataset_lower:
                source = 'igt'
            elif 'syco' in dataset_lower:
                source = 'syco'
            elif 'ezpay' in dataset_lower:
                source = 'ezpay'
            elif 'reveal' in dataset_lower:
                source = 'reveal'
            else:
                return full_match

            # Convert table name to snake_case
            snake_table = camel_to_snake_local(table)
            # Use s_ prefix without site suffix
            ref_name = f"s_{source}_{snake_table}"
            return f"${{ref('{ref_name}')}}"

        if re.search(r'`[a-z0-9_-]+\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+`', content, re.IGNORECASE):
            content = re.sub(
                r'`([a-z0-9_-]+)\.([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)`',
                convert_hardcoded_bq_table,
                content,
                flags=re.IGNORECASE
            )
            changes.append("Converted hardcoded BigQuery table references to refs")

        return content, changes

    def fix_raw_layer_references(self, content: str) -> Tuple[str, List[str]]:
        """Fix incorrect raw layer references from vw_dacom_*_mel to s_dacom_*.

        The raw layer tables use s_ prefix without site suffix:
        - ${ref('vw_dacom_location_mel')} -> ${ref('s_dacom_location')}
        - ${ref('vw_dacom_mac_config_mel')} -> ${ref('s_dacom_mac_config')}
        - ${ref('vw_igt_location_syd')} -> ${ref('s_igt_location')}

        Returns:
            Tuple of (fixed_content, list_of_changes)
        """
        changes = []
        count = 0

        # Pattern to match ${ref('vw_SOURCE_TABLE_SITE')}
        # where SOURCE is dacom/igt/syco/ezpay/reveal and SITE is mel/syd/per
        sources = ['dacom', 'igt', 'syco', 'ezpay', 'reveal']
        sites = ['mel', 'syd', 'per']

        def fix_ref(match):
            nonlocal count
            ref_name = match.group(1)

            # Check if this matches the vw_source_*_site pattern
            for source in sources:
                prefix = f'vw_{source}_'
                if ref_name.lower().startswith(prefix):
                    for site in sites:
                        suffix = f'_{site}'
                        if ref_name.lower().endswith(suffix):
                            # Extract table name (between prefix and suffix)
                            table_part = ref_name[len(prefix):-len(suffix)]
                            # Build new ref name: s_{source}_{table}
                            new_ref = f's_{source}_{table_part}'
                            count += 1
                            return f"${{ref('{new_ref}')}}"
            # No match - return unchanged
            return match.group(0)

        content = re.sub(
            r"\$\{ref\(['\"]([^'\"]+)['\"]\)\}",
            fix_ref,
            content
        )

        if count > 0:
            changes.append(f"Fixed {count} raw layer reference(s): vw_*_site -> s_*")

        return content, changes

    def detect_comma_joins(self, content: str) -> bool:
        """Detect old-style comma joins.

        Returns:
            True if comma joins are detected
        """
        from_pattern = r'\bFROM\s+[\w\$\{\}\(\)\'\"]+(?:\s+(?:AS\s+)?\w+)?\s*,\s*[\w\$\{\}\(\)\'\"]+'
        return bool(re.search(from_pattern, content, re.IGNORECASE))

    def escape_refs_in_comments(self, content: str) -> str:
        """Escape ${ref()} calls inside comments to prevent compilation errors.

        Converts ${ref('table')} to [REF: table] inside comments.
        """
        def replace_refs_in_text(text):
            return re.sub(
                r"\$\{ref\(['\"]([^'\"]+)['\"]\)\}",
                r"[REF: \1]",
                text
            )

        # Process block comments /* ... */
        def replace_block_comment(match):
            comment = match.group(0)
            return replace_refs_in_text(comment)

        content = re.sub(r'/\*.*?\*/', replace_block_comment, content, flags=re.DOTALL)

        # Process line comments
        processed_lines = []
        for line in content.split('\n'):
            comment_pos = line.find('--')
            if comment_pos != -1:
                code_part = line[:comment_pos]
                comment_part = line[comment_pos:]
                comment_part = replace_refs_in_text(comment_part)
                line = code_part + comment_part
            processed_lines.append(line)

        return '\n'.join(processed_lines)

    def _is_inside_comment(self, content: str, pos: int) -> bool:
        """Check if a position is inside a SQL comment."""
        # Check for single-line comment (-- comment)
        line_start = content.rfind('\n', 0, pos) + 1
        line_content = content[line_start:pos]
        if '--' in line_content:
            return True

        # Check for multi-line comment (/* comment */)
        # Find the last /* before pos
        last_open = content.rfind('/*', 0, pos)
        if last_open != -1:
            # Check if there's a */ between last_open and pos
            last_close = content.rfind('*/', last_open, pos)
            if last_close == -1:
                return True  # Inside /* ... */

        return False

    def _get_config_block_end(self, content: str) -> int:
        """Find the end position of the config block.

        Returns -1 if no config block found, otherwise returns the position
        after the closing brace.
        """
        config_match = re.search(r'config\s*\{', content)
        if not config_match:
            return -1

        # Find matching closing brace by counting braces
        start = config_match.end()
        brace_count = 1
        pos = start
        while pos < len(content) and brace_count > 0:
            if content[pos] == '{':
                brace_count += 1
            elif content[pos] == '}':
                brace_count -= 1
            pos += 1

        return pos if brace_count == 0 else -1

    def _is_inside_config_block(self, content: str, pos: int) -> bool:
        """Check if a position is inside the config block."""
        config_end = self._get_config_block_end(content)
        if config_end == -1:
            return False
        config_start = content.find('config')
        return config_start != -1 and config_start <= pos < config_end

    def _is_inside_string_literal(self, content: str, pos: int) -> bool:
        """Check if a position is inside a SQL string literal (single or double quoted).

        This handles strings in OPTIONS clauses, description fields, etc.
        """
        # Count quotes before this position to determine if we're inside a string
        # We need to track both single and double quotes, handling escapes
        in_single_quote = False
        in_double_quote = False
        i = 0
        while i < pos:
            char = content[i]
            # Check for escaped quotes
            if i + 1 < len(content):
                next_char = content[i + 1]
                if char == '\\' and next_char in ('"', "'"):
                    i += 2  # Skip escaped quote
                    continue
                # Also handle SQL-style escaping ('' or "")
                if char == "'" and next_char == "'" and in_single_quote:
                    i += 2
                    continue
                if char == '"' and next_char == '"' and in_double_quote:
                    i += 2
                    continue

            if char == '"' and not in_single_quote:
                in_double_quote = not in_double_quote
            elif char == "'" and not in_double_quote:
                in_single_quote = not in_single_quote
            i += 1

        return in_single_quote or in_double_quote

    def remove_schema_prefixes(self, content: str) -> Tuple[str, List[str]]:
        """Remove schema prefixes and convert bare schema.table to ${ref()}.

        Fixes patterns like:
        - sybaseadmin.${ref('table')} -> ${ref('table')}
        - sybaseadmin.TableName -> ${ref('tablename')}

        Skips patterns inside comments.

        Returns:
            Tuple of (cleaned_content, list_of_warnings)
        """
        warnings = []
        schema_prefixes = ['sybaseadmin', 'dbo', 'dwh', 'staging']

        # Pattern 1: schema_name.${ref(...)} or schema_name.${self()} -> ${ref(...)} or ${self()}
        for schema in schema_prefixes:
            pattern = rf'\b{schema}\s*\.\s*(\$\{{(?:ref|self|dataform)[^}}]+\}})'

            def replace_if_not_comment_p1(match):
                if self._is_inside_comment(content, match.start()):
                    return match.group(0)  # Keep original if in comment
                return match.group(1)

            matches_before = len(re.findall(pattern, content, re.IGNORECASE))
            new_content = re.sub(pattern, replace_if_not_comment_p1, content, flags=re.IGNORECASE)
            if new_content != content:
                # Count actual replacements (not in comments)
                matches_after = len(re.findall(pattern, new_content, re.IGNORECASE))
                replaced = matches_before - matches_after
                if replaced > 0:
                    warnings.append(f"Removed '{schema}.' prefix from {replaced} ref(s)")
                content = new_content

        # Pattern 1b: ${ref('schema')}.TableName -> ${ref('tablename')}
        # This handles incorrect LLM output where schema is wrapped in ref() with table name appended
        schema_ref_pattern = r"`?\$\{ref\(['\"](?:sybaseadmin|dbo|dwh|staging)['\"](?:\s*,\s*['\"][^'\"]+['\"])?\)\}\.([A-Za-z_][A-Za-z0-9_]*)`?"

        def fix_schema_ref(match):
            if self._is_inside_comment(content, match.start()):
                return match.group(0)
            table_name = match.group(1).lower()
            return f"${{ref('{table_name}')}}"

        if re.search(schema_ref_pattern, content, re.IGNORECASE):
            new_content = re.sub(schema_ref_pattern, fix_schema_ref, content, flags=re.IGNORECASE)
            if new_content != content:
                warnings.append("Fixed ${ref('schema')}.TableName -> ${ref('tablename')}")
                content = new_content

        # Pattern 2: schema_name.TableName -> ${ref('tablename')}
        # Match schema.table but NOT:
        #   - Inside ${ref()} already
        #   - dataform.projectConfig patterns
        #   - Inside comments
        for schema in schema_prefixes:
            # Find all schema.table patterns
            pattern = rf'\b{schema}\s*\.\s*([A-Za-z_][A-Za-z0-9_]*)\b'

            def make_replace_func(schema_name, cont):
                def replace_schema_table(match):
                    # Skip if inside a comment
                    if self._is_inside_comment(cont, match.start()):
                        return match.group(0)
                    # Skip if inside config block (descriptions, etc.)
                    if self._is_inside_config_block(cont, match.start()):
                        return match.group(0)
                    # Skip if inside a string literal (e.g., OPTIONS description)
                    if self._is_inside_string_literal(cont, match.start()):
                        return match.group(0)
                    table_name = match.group(1)
                    # Skip if it's a Dataform variable or already processed
                    if table_name.lower() in ('projectconfig', 'vars', 'defaultschema'):
                        return match.group(0)
                    # Convert to ${ref('tablename')} — preserve original case
                    # Dataform resolves refs case-sensitively
                    return f"${{ref('{table_name}')}}"
                return replace_schema_table

            # Check if there are matches before replacing
            if re.search(pattern, content, re.IGNORECASE):
                new_content = re.sub(pattern, make_replace_func(schema, content), content, flags=re.IGNORECASE)
                if new_content != content:
                    warnings.append(f"Converted '{schema}.table' patterns to ${{ref()}}")
                    content = new_content

        # Pattern 3: Backticked schema.table references like `sybaseadmin.TableName`
        # Convert to use Dataform dataset reference
        for schema in schema_prefixes:
            # Match `schema.TableName` (with backticks)
            pattern = rf'`{schema}\.([A-Za-z_][A-Za-z0-9_]*)`'

            def make_backtick_replace_func(schema_name):
                def replace_backtick_ref(match):
                    table_name = match.group(1)
                    # Convert to BigQuery format with Dataform variable
                    return f'`${{dataform.projectConfig.defaultSchema}}.{table_name.lower()}`'
                return replace_backtick_ref

            if re.search(pattern, content, re.IGNORECASE):
                new_content = re.sub(pattern, make_backtick_replace_func(schema), content, flags=re.IGNORECASE)
                if new_content != content:
                    warnings.append(f"Converted backticked '{schema}.table' to Dataform dataset reference")
                    content = new_content

        return content, warnings

    def fix_bare_table_references(self, content: str) -> Tuple[str, List[str]]:
        """Wrap known bare table references in ${ref()}.

        Converts: D_Status_Code sc -> ${ref('d_status_code')} sc

        Returns:
            Tuple of (fixed_content, list_of_warnings)
        """
        warnings = []

        # Known dimension and fact tables that are commonly bare-referenced
        # Format: (original_name, ref_name)
        known_tables = [
            # Dimension tables
            ('D_Status_Code', 'd_status_code'),
            ('D_Jackpot_Structure', 'd_jackpot_structure'),
            ('D_Site', 'd_site'),
            ('D_Property', 'd_property'),
            ('D_Casino_Location_Detail', 'd_casino_location_detail'),
            ('D_CasinoLocationDet', 'd_casinolocationdet'),
            ('D_CASINOLOCATIONDET', 'd_casinolocationdet'),
            ('D_Time', 'd_time'),
            ('D_Date', 'd_date'),
            ('D_EGM', 'd_egm'),
            ('D_Employee', 'd_employee'),
            ('D_Player', 'd_player'),
            ('D_Patron', 'd_patron'),
            ('D_Promotion_Configuration', 'd_promotion_configuration'),
            ('D_Promotion_Details', 'd_promotion_details'),
            ('D_ProductDet', 'd_productdet'),
            ('D_GamingMachine', 'd_gamingmachine'),
            ('D_TerminalLocationDet', 'd_terminallocationdet'),
            ('D_Hour', 'd_hour'),
            ('D_Day', 'd_day'),
            ('D_Month', 'd_month'),
            ('D_Membership', 'd_membership'),
            # Work/staging tables
            ('W_EGM_Received_Keys', 'w_egm_received_keys'),
            ('W_EGM_Sent_Keys', 'w_egm_sent_keys'),
            ('W_Jackpot_Config_Keys', 'w_jackpot_config_keys'),
            ('W_DUPLICATEKEYS_F_GAMINGPATRONVISITGAME', 'w_duplicatekeys_f_gamingpatronvisitgame'),
            ('W_DUPLICATEKEYS_F_GAMINGPATRONVISIT', 'w_duplicatekeys_f_gamingpatronvisit'),
            ('W_DUPLICATEKEYS_F_GAMINGPATRONHOURGAMEAREA', 'w_duplicatekeys_f_gamingpatronhourgamearea'),
            # Views
            ('VW_EGM_Received_Keys_Daily', 'vw_egm_received_keys_daily'),
            ('VW_EGM_Received_Keys_Hourly', 'vw_egm_received_keys_hourly'),
            # Fact tables
            ('F_Gaming_Rating', 'f_gaming_rating'),
            ('F_Jackpot_Hit', 'f_jackpot_hit'),
            ('F_Patron_Membership', 'f_patron_membership'),
        ]

        for original_name, ref_name in known_tables:
            # Match bare table reference with optional alias
            # Patterns:
            #   FROM D_Status_Code sc
            #   JOIN D_Status_Code AS sc
            #   , D_Status_Code b
            # Skip if already inside ${ref()}

            # Simple pattern to find the table name with optional alias
            pattern = rf'\b({original_name})(?:\s+(?:AS\s+)?([a-zA-Z_][a-zA-Z0-9_]*))?\s*(?=ON|WHERE|,|$|\n|--|LEFT|RIGHT|INNER|OUTER|FULL|CROSS|JOIN|\))'

            def make_replace_func(rn, cont):
                def replace_bare_ref(match):
                    start = match.start()

                    # Skip if inside config block
                    if self._is_inside_config_block(cont, start):
                        return match.group(0)

                    # Skip if inside comment
                    if self._is_inside_comment(cont, start):
                        return match.group(0)

                    # Check if this match is inside a ${ref()} or is an alias after ${ref()} AS
                    # Lookback must be long enough to cover ${ref('long_table_name')} AS
                    # (e.g., ${ref('d_terminallocationdet')} AS = 38 chars before alias)
                    prefix = cont[max(0, start-60):start]
                    if "${ref('" in prefix or '${ref("' in prefix:
                        return match.group(0)  # Return unchanged
                    # Also check if preceded by ")} AS " — alias position after a ref
                    if re.search(r"\)}\s+AS\s+$", prefix, re.IGNORECASE):
                        return match.group(0)  # Return unchanged

                    alias = match.group(2)
                    if alias:
                        return f"${{ref('{rn}')}} {alias} "
                    return f"${{ref('{rn}')}}"
                return replace_bare_ref

            new_content = re.sub(pattern, make_replace_func(ref_name, content), content, flags=re.IGNORECASE)
            if new_content != content:
                # Count actual replacements (not matches inside refs)
                warnings.append(f"Wrapped bare '{original_name}' reference(s) in ${{ref()}}")
                content = new_content

        # Also catch schema.table patterns that aren't ${ref()} - and FIX them
        schema_table_pattern = r'\b(sybaseadmin|dbo|dwh)\s*\.\s*([a-zA-Z_][a-zA-Z0-9_]*)\b(?!\s*\()'

        def replace_schema_table(match):
            full_match = match.group(0)
            start = match.start()

            # Skip if inside config block or comment
            if self._is_inside_config_block(content, start):
                return full_match
            if self._is_inside_comment(content, start):
                return full_match

            # Skip if inside ${ref()}
            prefix = content[max(0, start-25):start]
            if "${ref('" in prefix or '${ref("' in prefix or '${' in prefix:
                return full_match

            schema = match.group(1)
            table = match.group(2)
            ref_name = table.lower()
            return f"${{ref('{ref_name}')}}"

        new_content = re.sub(schema_table_pattern, replace_schema_table, content, flags=re.IGNORECASE)
        if new_content != content:
            warnings.append("Converted schema.table references to ${ref()}")
            content = new_content

        # ADDITIONAL: Catch any remaining bare table references after FROM/JOIN
        # Pattern: FROM TableName or JOIN TableName (where TableName starts with D_, F_, W_, VW_, etc.)
        # NOTE: We only match FROM/JOIN patterns now - comma joins are too risky as they match
        # column names in GROUP BY, SELECT, etc. that happen to start with D_, F_, W_
        bare_table_patterns = [
            # D_TableName, F_TableName, W_TableName patterns after FROM/JOIN only
            # NOTE: Use \b word boundary in lookahead to prevent matching partial words like 'ON' in 'Transaction'
            (r'(?:FROM|JOIN)\s+([DFW]_[A-Za-z_][A-Za-z0-9_]*)(?:\s+(?:AS\s+)?([a-zA-Z_][a-zA-Z0-9_]*))?\s*(?=\bON\b|\bWHERE\b|\bLEFT\b|\bRIGHT\b|\bINNER\b|\bJOIN\b|,|\n|$|\))', 1),
            # VW_TableName patterns after FROM/JOIN only
            (r'(?:FROM|JOIN)\s+(VW_[A-Za-z_][A-Za-z0-9_]*)(?:\s+(?:AS\s+)?([a-zA-Z_][a-zA-Z0-9_]*))?\s*(?=\bON\b|\bWHERE\b|\bLEFT\b|\bRIGHT\b|\bINNER\b|\bJOIN\b|,|\n|$|\))', 1),
            # REMOVED: Comma join pattern - too many false positives with column names like D_Table_Column
        ]

        for pattern, table_group in bare_table_patterns:
            def make_replacer(pat_str, tbl_grp):
                def replacer(match):
                    full = match.group(0)
                    start = match.start()

                    # Skip if inside config or comment
                    if self._is_inside_config_block(content, start):
                        return full
                    if self._is_inside_comment(content, start):
                        return full

                    # Skip if already inside ${ref()}
                    prefix = content[max(0, start-25):start]
                    if "${ref('" in prefix or '${ref("' in prefix:
                        return full

                    table_name = match.group(tbl_grp)
                    alias = match.group(tbl_grp + 1) if match.lastindex >= tbl_grp + 1 else None

                    # Determine the keyword (FROM, JOIN, or comma)
                    keyword_match = re.match(r'(FROM|JOIN|,)\s*', full, re.IGNORECASE)
                    keyword = keyword_match.group(1) if keyword_match else ''

                    ref_name = table_name.lower()
                    if alias:
                        return f"{keyword} ${{ref('{ref_name}')}} {alias} "
                    else:
                        return f"{keyword} ${{ref('{ref_name}')}} "
                return replacer

            new_content = re.sub(pattern, make_replacer(pattern, table_group), content, flags=re.IGNORECASE)
            if new_content != content:
                warnings.append(f"Wrapped additional bare table references in ${{ref()}}")
                content = new_content

        return content, warnings

    def detect_select_star_joins(self, content: str) -> List[str]:
        """Detect SELECT * used with multiple JOINs (causes duplicate columns).

        Returns:
            List of warning messages
        """
        warnings = []

        # Find SELECT * statements
        select_star_pattern = r'\bSELECT\s+\*\s+FROM'

        # Count JOIN clauses
        join_pattern = r'\b(?:LEFT\s+(?:OUTER\s+)?|RIGHT\s+(?:OUTER\s+)?|INNER\s+|FULL\s+(?:OUTER\s+)?|CROSS\s+)?JOIN\b'

        # Split by SELECT to analyze each query
        parts = re.split(r'\bSELECT\b', content, flags=re.IGNORECASE)

        for i, part in enumerate(parts[1:], 1):  # Skip first part (before any SELECT)
            # Check if this SELECT uses *
            if re.match(r'\s+\*\s+FROM', part, re.IGNORECASE):
                # Count JOINs in this query (up to next SELECT or end)
                next_select = re.search(r'\bSELECT\b', part, re.IGNORECASE)
                query_part = part[:next_select.start()] if next_select else part

                join_count = len(re.findall(join_pattern, query_part, re.IGNORECASE))

                if join_count >= 2:
                    warnings.append(
                        f"SELECT * with {join_count} JOINs detected - will produce duplicate columns. "
                        f"Use explicit column list instead."
                    )

        return warnings

    def _extract_sql_body(self, content: str) -> str:
        """Extract the SQL body (everything after the config block).

        This is used to find refs only in actual SQL, not in description strings.
        """
        # Find the end of the config block
        config_match = re.search(r'config\s*\{', content)
        if not config_match:
            return content

        # Find matching closing brace by counting braces
        start = config_match.end()
        brace_count = 1
        pos = start
        while pos < len(content) and brace_count > 0:
            if content[pos] == '{':
                brace_count += 1
            elif content[pos] == '}':
                brace_count -= 1
            pos += 1

        # Return everything after the config block
        return content[pos:] if pos < len(content) else ""

    def fix_circular_references(self, content: str, source_name: str) -> Tuple[str, List[str]]:
        """Fix circular self-references by replacing with ${self()}.

        In incremental tables, the pattern:
            ${dataform.projectConfig.defaultSchema}.table_name
        should be replaced with:
            ${self()}

        Args:
            content: The SQLX content
            source_name: Name of the source file (e.g., 'f_tito_movement_daily')

        Returns:
            Tuple of (fixed_content, list_of_warnings)
        """
        warnings = []

        # Extract base name without extension and path
        base_name = source_name.lower()
        if '/' in base_name:
            base_name = base_name.split('/')[-1]
        if '\\' in base_name:
            base_name = base_name.split('\\')[-1]
        if '.' in base_name:
            base_name = base_name.rsplit('.', 1)[0]

        # Fix pattern: ${dataform.projectConfig.defaultSchema}.self_name -> ${self()}
        # This is common in incremental tables for checking existing records
        schema_self_pattern = rf'\$\{{dataform\.projectConfig\.defaultSchema\}}\.{re.escape(base_name)}\b'
        if re.search(schema_self_pattern, content, re.IGNORECASE):
            content = re.sub(schema_self_pattern, '${self()}', content, flags=re.IGNORECASE)
            warnings.append(f"Fixed self-reference: replaced schema.{base_name} with ${{self()}}")

        # Check for remaining ${ref('self')} patterns that need manual review
        # Only check in SQL body, not in description strings
        sql_body = self._extract_sql_body(content)
        refs = re.findall(r"\$\{ref\(['\"]([^'\"]+)['\"]\)\}", sql_body)
        for ref in refs:
            ref_lower = ref.lower()
            if ref_lower == base_name:
                # Check if this is a CALL statement in a procedure (infinite loop)
                call_pattern = rf"CALL\s+\$\{{ref\(['\"]" + re.escape(ref) + r"['\"]\)\}"
                if re.search(call_pattern, content, re.IGNORECASE):
                    # Replace the CALL with a placeholder comment
                    content = re.sub(
                        call_pattern + r"\s*\([^)]*\)\s*;?",
                        "-- TODO: Self-referencing CALL removed (was infinite loop)\n  SELECT 'Procedure needs implementation' AS status;",
                        content,
                        flags=re.IGNORECASE
                    )
                    warnings.append(
                        f"FIXED INFINITE LOOP: Removed CALL ${{ref('{ref}')}} that called itself"
                    )
                else:
                    warnings.append(
                        f"CIRCULAR REFERENCE: File references itself via ${{ref('{ref}')}}. "
                        f"This may need manual review - consider using ${{self()}} for incremental tables."
                    )

        return content, warnings

    def detect_circular_references(self, content: str, source_name: str) -> List[str]:
        """Detect if a file references itself (circular dependency).

        Only checks SQL body, not description strings in config block.

        Args:
            content: The SQLX content
            source_name: Name of the source file (e.g., 'm_jackpot_hit_delete')

        Returns:
            List of warning messages
        """
        warnings = []

        # Extract base name without extension and path
        base_name = source_name.lower()
        if '/' in base_name:
            base_name = base_name.split('/')[-1]
        if '\\' in base_name:
            base_name = base_name.split('\\')[-1]
        if '.' in base_name:
            base_name = base_name.rsplit('.', 1)[0]

        # Only check SQL body, not description strings
        sql_body = self._extract_sql_body(content)

        # Check for self-reference in ${ref()} calls
        refs = re.findall(r"\$\{ref\(['\"]([^'\"]+)['\"]\)\}", sql_body)
        for ref in refs:
            ref_lower = ref.lower()
            if ref_lower == base_name:
                warnings.append(
                    f"CIRCULAR REFERENCE: File references itself via ${{ref('{ref}')}}. "
                    f"This will cause infinite dependency loop."
                )

        # Also check for references using dataform.projectConfig.defaultSchema pattern
        schema_refs = re.findall(
            r"\$\{dataform\.projectConfig\.defaultSchema\}\.(\w+)",
            sql_body
        )
        for ref in schema_refs:
            if ref.lower() == base_name:
                warnings.append(
                    f"CIRCULAR REFERENCE: File references itself via schema.{ref}. "
                    f"This will cause infinite dependency loop."
                )

        return warnings

    def fix_cte_column_references(self, content: str) -> Tuple[str, List[str]]:
        """Validate and fix CTE column references.

        Parses CTE definitions to extract column names, then validates that
        references like `cte_name.column_name` use columns that actually exist.
        When mismatches are found, attempts to fix them or adds TODO comments.

        Only validates references OUTSIDE each CTE's own body. Inside a CTE
        body, identifiers like `A.column` refer to table aliases, not CTE
        outputs — validating those causes false positives.

        Returns:
            Tuple of (fixed_content, list_of_warnings)
        """
        warnings = []

        # Step 1: Extract CTE definitions, their columns, and body ranges
        cte_columns, cte_ranges = self._extract_cte_columns_with_ranges(content)

        if not cte_columns:
            return content, []

        # Step 2: Find all invalid column references
        # Only check references OUTSIDE the CTE's own body
        invalid_refs = []
        ref_pattern = r'\b(\w+)\.(\w+)\b'

        for match in re.finditer(ref_pattern, content):
            table_or_cte = match.group(1)
            column = match.group(2)
            table_lower = table_or_cte.lower()
            column_lower = column.lower()

            # Skip if not a known CTE
            if table_lower not in cte_columns:
                continue

            # Skip common non-column patterns
            if column_lower in ('projectconfig', 'vars', 'defaultschema'):
                continue

            # Skip if this reference is inside ANY CTE body
            # Inside CTE bodies, identifiers like X.column refer to table
            # aliases (e.g., FROM table AS X), not CTE outputs.
            # Only validate refs in the final SELECT (outside all CTEs).
            match_pos = match.start()
            inside_cte = False
            for _, (cte_start, cte_end) in cte_ranges.items():
                if cte_start <= match_pos <= cte_end:
                    inside_cte = True
                    break
            if inside_cte:
                continue

            # Check if column exists in CTE
            available = cte_columns[table_lower]
            if column_lower not in available and '*' not in available:
                # Find similar column names for suggestions
                similar = self._find_similar_columns(column_lower, available)
                invalid_refs.append({
                    'match': match,
                    'cte': table_or_cte,
                    'column': column,
                    'available': available,
                    'similar': similar
                })

        # Step 3: Fix or comment invalid references
        if invalid_refs:
            # Process in reverse order to preserve positions
            fixed_content = content
            fixes_applied = 0

            for ref in reversed(invalid_refs):
                match = ref['match']
                cte = ref['cte']
                column = ref['column']
                similar = ref['similar']
                available = ref['available']

                start, end = match.start(), match.end()
                original = f"{cte}.{column}"

                # Try to auto-fix if there's a clear similar match
                if len(similar) == 1:
                    # Single similar column found - auto-fix
                    new_column = similar[0]
                    fixed_ref = f"{cte}.{new_column}"
                    fixed_content = fixed_content[:start] + fixed_ref + fixed_content[end:]
                    warnings.append(f"Fixed CTE column: {original} -> {fixed_ref}")
                    fixes_applied += 1
                else:
                    # No clear fix - add inline TODO comment
                    available_str = ', '.join(sorted(list(available)[:5]))
                    if len(available) > 5:
                        available_str += f" (+{len(available)-5} more)"

                    suggestion = f" (try: {', '.join(similar)})" if similar else ""
                    todo_comment = f"/* TODO: Invalid column '{column}' in CTE '{cte}'. Available: {available_str}{suggestion} */"

                    # Insert TODO comment before the reference
                    fixed_content = fixed_content[:start] + todo_comment + " " + fixed_content[start:]
                    warnings.append(f"INVALID COLUMN: {original} - added TODO comment")

            if fixes_applied > 0:
                logger.info(f"Auto-fixed {fixes_applied} CTE column references")

            return fixed_content, warnings

        return content, warnings

    def _extract_cte_columns(self, content: str) -> Dict[str, set]:
        """Extract column names from all CTEs in the content.

        Returns:
            Dict mapping CTE name (lowercase) to set of column names (lowercase)
        """
        columns, _ = self._extract_cte_columns_with_ranges(content)
        return columns

    def _extract_cte_columns_with_ranges(self, content: str) -> Tuple[Dict[str, set], Dict[str, Tuple[int, int]]]:
        """Extract column names and body ranges from all CTEs.

        Returns:
            Tuple of:
            - Dict mapping CTE name (lowercase) to set of column names (lowercase)
            - Dict mapping CTE name (lowercase) to (start_pos, end_pos) of CTE body
        """
        cte_columns = {}
        cte_ranges = {}

        # Find CTE definitions with their positions: cte_name AS (SELECT ...)
        cte_def_pattern = re.compile(r'(\w+)\s+AS\s*\(', re.IGNORECASE)
        for cte_match in cte_def_pattern.finditer(content):
            cte_name = cte_match.group(1)
            cte_name_lower = cte_name.lower()

            # Skip SQL keywords that look like CTE names
            if cte_name_lower in ('select', 'from', 'where', 'join', 'on', 'and',
                                   'or', 'not', 'in', 'is', 'case', 'when', 'then',
                                   'else', 'end', 'cast', 'numeric', 'string', 'int64',
                                   'float64', 'bool', 'date', 'timestamp', 'datetime'):
                continue

            # Track CTE body range by matching parens from the opening (
            paren_start = cte_match.end() - 1  # position of the (
            depth = 1
            pos = paren_start + 1
            in_single_quote = False
            while pos < len(content) and depth > 0:
                ch = content[pos]
                if in_single_quote:
                    if ch == "'" and (pos + 1 >= len(content) or content[pos+1] != "'"):
                        in_single_quote = False
                elif ch == "'":
                    in_single_quote = True
                elif ch == '(':
                    depth += 1
                elif ch == ')':
                    depth -= 1
                pos += 1

            cte_body_end = pos  # position after the closing )
            cte_ranges[cte_name_lower] = (paren_start, cte_body_end)

            # Extract SELECT columns from the CTE body
            cte_body = content[paren_start+1:cte_body_end-1] if depth == 0 else ""
            select_from_match = re.search(r'\bSELECT\s+(.*?)\s+FROM\b', cte_body, re.IGNORECASE | re.DOTALL)
            if not select_from_match:
                continue

            select_clause = select_from_match.group(1).strip()
            if select_clause.upper().startswith('DISTINCT'):
                select_clause = re.sub(r'^DISTINCT\s+', '', select_clause, flags=re.IGNORECASE)

            columns = set()
            col_parts = self._split_select_columns(select_clause)

            for part in col_parts:
                part = part.strip()
                if not part:
                    continue

                # Check for AS alias
                as_match = re.search(r'\bAS\s+(\w+)\s*$', part, re.IGNORECASE)
                if as_match:
                    columns.add(as_match.group(1).lower())
                else:
                    # No alias - extract column name (last identifier)
                    col_match = re.search(r'(\w+)\s*$', part)
                    if col_match:
                        columns.add(col_match.group(1).lower())

            # Check for * (SELECT *)
            if '*' in select_clause.split(',')[0].strip():
                columns.add('*')

            if columns:
                cte_columns[cte_name_lower] = columns

        return cte_columns, cte_ranges

    def _find_similar_columns(self, column: str, available: set) -> List[str]:
        """Find similar column names from available columns.

        Handles common naming variations like:
        - DayId vs DayID vs dayid
        - HourId vs HourlD vs hourid
        - JkptOpnPollHrID vs LinkJkptOpnPollHrID (Link prefix)
        - TicketHrID vs HourID
        - CardedJkptOpnPollHrID vs JkptOpnPollHrID (Carded prefix)

        Returns:
            List of similar column names, sorted by similarity (best match first)
        """
        column_lower = column.lower()
        similar = []
        scored_matches = []  # (score, column_name) for sorting

        # Common prefixes that may differ between tables
        # F_Jackpot_Schedule uses Link prefix, W_Jackpot_Schedule doesn't
        known_prefixes = ['link', 'carded', 'adj', 'src', 'tgt', 'in_', 'out_']

        for avail in available:
            avail_lower = avail.lower()
            score = 0

            # Exact match (case-insensitive) - highest priority
            if column_lower == avail_lower:
                scored_matches.append((100, avail))
                continue

            # Check if one is a prefixed version of the other
            # e.g., JkptOpnPollHrID vs LinkJkptOpnPollHrID
            for prefix in known_prefixes:
                # Column missing prefix, available has prefix
                if avail_lower == prefix + column_lower:
                    scored_matches.append((90, avail))
                    break
                # Column has prefix, available doesn't
                if column_lower == prefix + avail_lower:
                    scored_matches.append((90, avail))
                    break
                # Column has prefix that should be stripped
                if column_lower.startswith(prefix) and avail_lower == column_lower[len(prefix):]:
                    scored_matches.append((90, avail))
                    break
                # Available has prefix that we need
                if avail_lower.startswith(prefix) and column_lower == avail_lower[len(prefix):]:
                    scored_matches.append((90, avail))
                    break

            # Substring match (column is part of available or vice versa)
            if column_lower in avail_lower:
                scored_matches.append((70, avail))
                continue
            if avail_lower in column_lower:
                scored_matches.append((70, avail))
                continue

            # Common ID column variations
            if column_lower.endswith('id') and avail_lower.endswith('id'):
                # Strip 'id' suffix and compare
                col_base = column_lower[:-2]
                avail_base = avail_lower[:-2]
                if col_base in avail_base or avail_base in col_base:
                    scored_matches.append((60, avail))
                    continue

            # HourId/HrID/TicketHrID variations
            hour_terms = ['hour', 'hr', 'tickethr', 'pollhr']
            col_has_hour = any(term in column_lower for term in hour_terms)
            avail_has_hour = any(term in avail_lower for term in hour_terms)
            if col_has_hour and avail_has_hour:
                scored_matches.append((50, avail))
                continue

            # DayId/DyId variations
            day_terms = ['day', 'dy']
            col_has_day = any(term in column_lower for term in day_terms)
            avail_has_day = any(term in avail_lower for term in day_terms)
            if col_has_day and avail_has_day:
                scored_matches.append((50, avail))
                continue

        # Sort by score (highest first) and return column names
        scored_matches.sort(key=lambda x: -x[0])
        return [col for score, col in scored_matches]

    def validate_cte_column_references(self, content: str) -> List[str]:
        """Validate that column references in JOINs and SELECTs exist in their CTEs.

        Parses CTE definitions to extract column names, then validates that
        references like `cte_name.column_name` use columns that actually exist.

        Returns:
            List of warning messages for invalid column references
        """
        # Use the new fix method but only return warnings
        _, warnings = self.fix_cte_column_references(content)
        return warnings

    def replace_shortcut_references(self, content: str) -> Tuple[str, List[str]]:
        """Replace shortcut_to_* references with their target table names.

        Informatica shortcuts like ${ref('shortcut_to_day')} are resolved to
        their actual target tables like ${ref('d_day')}.

        Args:
            content: The SQLX content

        Returns:
            Tuple of (modified_content, list_of_warnings)
        """
        warnings = []
        replacements_made = []

        # Find all ${ref('shortcut_to_...')} patterns
        ref_pattern = r"\$\{ref\(['\"]([^'\"]+)['\"]\)\}"

        def replace_shortcut(match):
            ref_name = match.group(1)
            ref_lower = ref_name.lower()

            if not ref_lower.startswith('shortcut_to_'):
                return match.group(0)

            # Check manual mappings first
            if ref_lower in SHORTCUT_MAPPINGS:
                target = SHORTCUT_MAPPINGS[ref_lower]
                replacements_made.append(f"{ref_name} -> {target}")
                return f"${{ref('{target}')}}"

            # Check shared transformation library
            if SHARED_LIB_AVAILABLE:
                lib = get_shared_transformations()
                lookup = lib.get_lookup(ref_name)
                if lookup:
                    target = lookup.lookup_table.lower()
                    replacements_made.append(f"{ref_name} -> {target}")
                    return f"${{ref('{target}')}}"

            # Not found - return original
            return match.group(0)

        content = re.sub(ref_pattern, replace_shortcut, content)

        if replacements_made:
            warnings.append(f"Replaced shortcut refs: {', '.join(replacements_made)}")

        return content, warnings

    def strip_cdw_prefix_from_refs(self, content: str) -> Tuple[str, List[str]]:
        """Strip cdw_ prefix from ${ref()} calls.

        The source stored procedures reference tables with cdw_ prefix (e.g., cdw_d_patron)
        but the actual tables in the Dataform project don't have this prefix (e.g., d_patron).

        This converts:
            ${ref('cdw_d_patron')} -> ${ref('d_patron')}
            ${ref('cdw_f_gamingrating')} -> ${ref('f_gamingrating')}
            ${ref('cdw_vw_hour')} -> ${ref('vw_hour')}

        EXCEPTION: When the stripped name matches a target of the same mapping,
        the cdw_ prefix is intentional (cycle prevention) and must be preserved.

        Args:
            content: The SQLX content

        Returns:
            Tuple of (modified_content, list_of_warnings)
        """
        warnings = []
        replacements_made = []

        # Find all ${ref('cdw_...')} patterns and strip the cdw_ prefix.
        # Cycle prevention no longer uses cdw_ prefixed refs — it prevents
        # alias view creation instead, so all cdw_ refs can be stripped.
        ref_pattern = r"\$\{ref\(['\"]cdw_([^'\"]+)['\"]\)\}"

        def replace_cdw_ref(match):
            original_ref = f"cdw_{match.group(1)}"
            stripped_ref = match.group(1)
            replacements_made.append(f"{original_ref} -> {stripped_ref}")
            return f"${{ref('{stripped_ref}')}}"

        content = re.sub(ref_pattern, replace_cdw_ref, content, flags=re.IGNORECASE)

        if replacements_made:
            warnings.append(f"Stripped cdw_ prefix from refs: {', '.join(replacements_made[:5])}" +
                          (f" (and {len(replacements_made) - 5} more)" if len(replacements_made) > 5 else ""))

        return content, warnings

    def fix_sybase_outer_join_operators(self, content: str) -> Tuple[str, List[str]]:
        """Convert Sybase *= and =* outer join operators to standard SQL.

        Sybase uses:
        - *= for LEFT OUTER JOIN (include all from left side)
        - =* for RIGHT OUTER JOIN (include all from right side)

        These appear in ON conditions like:
            ON a.col *= b.col

        Since these are typically embedded in INNER JOIN ON conditions (mixed syntax),
        we convert them to standard equality with a comment for manual review.

        Returns:
            Tuple of (fixed_content, list_of_warnings)
        """
        warnings = []
        left_outer_count = 0
        right_outer_count = 0

        # Pattern for *= (left outer join operator)
        # Match: column *= value or column*=value (with optional spaces)
        # But NOT inside strings or comments
        def replace_left_outer(match):
            nonlocal left_outer_count
            # Check if inside comment
            if self._is_inside_comment(content, match.start()):
                return match.group(0)
            left_outer_count += 1
            left_side = match.group(1)
            right_side = match.group(2)
            return f"{left_side} = {right_side} /* Sybase *= (left outer) - verify logic */"

        # Pattern: identifier *= identifier (with optional spaces and backticks)
        # Handles: col *= val, `col` *= val, table.col *= table.val
        left_outer_pattern = r'([`\w.]+)\s*\*=\s*([`\w.${}\(\)\'\"]+)'
        new_content = re.sub(left_outer_pattern, replace_left_outer, content)

        if left_outer_count > 0:
            warnings.append(f"Converted {left_outer_count} Sybase *= (left outer) operators to standard SQL")

        content = new_content

        # Pattern for =* (right outer join operator)
        def replace_right_outer(match):
            nonlocal right_outer_count
            if self._is_inside_comment(content, match.start()):
                return match.group(0)
            right_outer_count += 1
            left_side = match.group(1)
            right_side = match.group(2)
            return f"{left_side} = {right_side} /* Sybase =* (right outer) - verify logic */"

        right_outer_pattern = r'([`\w.]+)\s*=\*\s*([`\w.${}\(\)\'\"]+)'
        new_content = re.sub(right_outer_pattern, replace_right_outer, content)

        if right_outer_count > 0:
            warnings.append(f"Converted {right_outer_count} Sybase =* (right outer) operators to standard SQL")

        return new_content, warnings

    def fix_hardcoded_bq_tables(self, content: str) -> Tuple[str, List[str]]:
        """Convert hardcoded BigQuery table references to ${ref()} calls.

        Converts DDAS access view references to raw layer table refs:
        - `crown-ddas-prod.DACOM_MEL_Access.MacConfig` -> ${ref('s_dacom_mac_config')}
        - `crown-ddas-prod.IGT_PER_Access.Location` -> ${ref('s_igt_location')}

        The raw layer tables follow the naming convention: s_{source}_{table}
        (without site suffix, as source tables are not site-specific in raw layer)

        Returns:
            Tuple of (fixed_content, list_of_warnings)
        """
        warnings = []
        replacements = []

        # Source system prefix mapping (dataset prefix to source name)
        source_prefix_map = {
            'DACOM': 'dacom',
            'SYCO': 'syco',
            'IGT': 'igt',
            'EZPAY': 'ezpay',
            'REVEAL': 'reveal',
        }

        # Pattern to match backticked fully-qualified table names
        # `project.dataset.table` or `project.dataset.table`.column
        # Dataset format: SOURCESYSTEM_SITE_Access (e.g., DACOM_MEL_Access)
        pattern = r'`crown-ddas-prod\.([A-Z]+)_([A-Z]+)_Access\.([A-Za-z_][A-Za-z0-9_]*)`'

        def camel_to_snake(name):
            """Convert CamelCase/PascalCase to snake_case, handling acronyms.

            Examples:
                MacConfig -> mac_config
                MTETConfig -> mtet_config
                ClassCode -> class_code
                IGTLocation -> igt_location
            """
            # First, handle transitions from uppercase acronym to capitalized word
            # e.g., MTETConfig -> MTET_Config
            s1 = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1_\2', name)
            # Then handle normal CamelCase: MacConfig -> Mac_Config
            s2 = re.sub(r'([a-z\d])([A-Z])', r'\1_\2', s1)
            return s2.lower()

        def replace_hardcoded_table(match):
            source_system = match.group(1)  # e.g., DACOM
            site = match.group(2)           # e.g., MEL (unused - raw layer not site-specific)
            table_name = match.group(3)     # e.g., MacConfig

            # Get source prefix
            source_prefix = source_prefix_map.get(source_system, source_system.lower())

            # Convert CamelCase/PascalCase to snake_case
            snake_name = camel_to_snake(table_name)

            # Generate raw layer table name: s_{source}_{table}
            # Note: No site suffix - raw layer tables are not site-specific
            ref_name = f"s_{source_prefix}_{snake_name}"
            replacements.append(f"`crown-ddas-prod.{source_system}_{site}_Access.{table_name}` -> ${{ref('{ref_name}')}}")
            return f"${{ref('{ref_name}')}}"

        new_content = re.sub(pattern, replace_hardcoded_table, content)

        # Also handle non-backticked references (less common but possible)
        # Pattern: crown-ddas-prod.DATASET.TABLE (without backticks, in column refs)
        pattern_no_backticks = r'crown-ddas-prod\.([A-Z]+)_([A-Z]+)_Access\.([A-Za-z_][A-Za-z0-9_]*)(?=\.|\s|$|,|\))'

        def replace_hardcoded_table_no_backticks(match):
            source_system = match.group(1)
            site = match.group(2)
            table_name = match.group(3)
            source_prefix = source_prefix_map.get(source_system, source_system.lower())
            snake_name = camel_to_snake(table_name)
            ref_name = f"s_{source_prefix}_{snake_name}"
            replacements.append(f"crown-ddas-prod.{source_system}_{site}_Access.{table_name} -> ${{ref('{ref_name}')}}")
            return f"${{ref('{ref_name}')}}"

        new_content = re.sub(pattern_no_backticks, replace_hardcoded_table_no_backticks, new_content)

        if replacements:
            unique_replacements = list(set(replacements))
            warnings.append(f"Converted {len(replacements)} hardcoded BQ table references: {', '.join(unique_replacements[:3])}" +
                          (f" (and {len(unique_replacements) - 3} more)" if len(unique_replacements) > 3 else ""))

        return new_content, warnings

    def fix_case_alias_references(self, content: str) -> Tuple[str, List[str]]:
        """Fix CASE statements that reference aliases defined in the same SELECT.

        BigQuery doesn't allow referencing a column alias in the same SELECT where
        it's defined. This converts such patterns to use a CTE.

        Example:
            SELECT
              CASE WHEN x THEN 'A' END AS Tier,
              CASE Tier WHEN 'A' THEN 1 END AS TierSort  -- ERROR in BigQuery
            FROM table

        Becomes:
            WITH base_cte AS (
              SELECT *, CASE WHEN x THEN 'A' END AS Tier FROM table
            )
            SELECT Tier, CASE Tier WHEN 'A' THEN 1 END AS TierSort FROM base_cte

        Returns:
            Tuple of (fixed_content, list_of_warnings)
        """
        warnings = []

        # Find aliases defined with CASE ... END AS alias
        alias_pattern = r'END\s+AS\s+(\w+)'
        defined_aliases = re.findall(alias_pattern, content, re.IGNORECASE)

        if not defined_aliases:
            return content, warnings

        # Check if any CASE statement references these aliases
        fixes_needed = []
        for alias in defined_aliases:
            # Look for CASE <alias> WHEN pattern (referencing the alias)
            ref_pattern = rf'\bCASE\s+{alias}\s+WHEN\b'
            if re.search(ref_pattern, content, re.IGNORECASE):
                fixes_needed.append(alias)

        if not fixes_needed:
            return content, warnings

        # Extract the config block
        config_match = re.match(r'(config\s*\{[^}]+\})', content, re.DOTALL)
        if not config_match:
            return content, warnings

        config_block = config_match.group(1)
        sql_content = content[config_match.end():].strip()

        # Find comments before SELECT
        comments_match = re.match(r'((?:\s*--[^\n]*\n)*)', sql_content)
        comments = comments_match.group(1) if comments_match else ''
        sql_after_comments = sql_content[len(comments):].strip()

        # Check if it starts with SELECT (not already a CTE)
        if not sql_after_comments.upper().startswith('SELECT'):
            return content, warnings

        # Find the FROM clause to split the SELECT
        # This is simplified - handles basic cases
        from_match = re.search(r'\bFROM\s+', sql_after_comments, re.IGNORECASE)
        if not from_match:
            return content, warnings

        select_part = sql_after_comments[:from_match.start()]
        from_part = sql_after_comments[from_match.start():]

        # For each alias that's referenced, we need to:
        # 1. Find the CASE expression that defines it
        # 2. Move it to the CTE
        # 3. Replace references to it

        for alias in fixes_needed:
            # Find the CASE ... END AS alias expression
            # Use a more robust pattern that handles nested CASE and various formats
            case_expr_pattern = rf'(CASE\s+.*?END)\s+AS\s+{alias}'
            case_match = re.search(case_expr_pattern, select_part, re.IGNORECASE | re.DOTALL)

            if case_match:
                case_expr = case_match.group(1)

                # Build the CTE
                # Extract the table reference from FROM clause
                table_ref_match = re.match(r'FROM\s+(\$\{ref\([\'"][^\'"]+[\'"]\)\}|\w+)', from_part, re.IGNORECASE)
                if table_ref_match:
                    table_ref = table_ref_match.group(1)

                    # Build new content with CTE
                    cte_select = f"SELECT *, {case_expr} AS {alias}"

                    # Remove the original CASE ... AS alias from select_part
                    # Replace with just the alias reference
                    new_select_part = re.sub(
                        rf',?\s*{re.escape(case_expr)}\s+AS\s+{alias}\s*,?',
                        f', {alias},',
                        select_part,
                        flags=re.IGNORECASE | re.DOTALL
                    )
                    # Clean up double commas and leading/trailing commas
                    new_select_part = re.sub(r',\s*,', ',', new_select_part)
                    new_select_part = re.sub(r'SELECT\s*,', 'SELECT ', new_select_part, flags=re.IGNORECASE)
                    new_select_part = re.sub(r',\s*FROM', ' FROM', new_select_part, flags=re.IGNORECASE)

                    # Build the new SQL with CTE
                    new_sql = f"""WITH {alias.lower()}_cte AS (
  {cte_select}
  {from_part.rstrip()}
)

{new_select_part}
FROM {alias.lower()}_cte"""

                    # Reconstruct full content
                    content = f"{config_block}\n\n{comments}{new_sql}"
                    warnings.append(f"Converted CASE {alias} reference to CTE (BigQuery alias restriction)")

        return content, warnings

    def remove_order_by_from_tables(self, content: str) -> Tuple[str, List[str]]:
        """Remove ORDER BY clauses from TABLE type definitions.

        BigQuery ignores ORDER BY in CREATE TABLE AS SELECT statements,
        so we remove them to avoid confusion.

        Only removes ORDER BY at the outermost SQL level (depth 0), not inside
        CTEs or subqueries. The previous regex-only approach with re.DOTALL would
        match ORDER BY inside a CTE and eat everything through the end of file.

        Returns:
            Tuple of (fixed_content, list_of_warnings)
        """
        warnings = []

        # Check if this is a table type
        if 'type: "table"' not in content and "type: 'table'" not in content:
            return content, warnings

        # Split config block from SQL body to only scan the SQL part
        config_end = 0
        config_match = re.search(r'config\s*\{', content)
        if config_match:
            depth = 0
            for i in range(config_match.start(), len(content)):
                if content[i] == '{':
                    depth += 1
                elif content[i] == '}':
                    depth -= 1
                    if depth == 0:
                        config_end = i + 1
                        break

        sql_body = content[config_end:]

        # Find all ORDER BY positions in the SQL body
        order_by_matches = list(re.finditer(r'ORDER\s+BY\b', sql_body, re.IGNORECASE))
        if not order_by_matches:
            return content, warnings

        # For each ORDER BY match, check if it's at parenthesis depth 0
        # (i.e., not inside a CTE or subquery)
        for match in reversed(order_by_matches):
            pos = match.start()
            # Count parenthesis depth up to this position
            depth = 0
            for i in range(pos):
                if sql_body[i] == '(':
                    depth += 1
                elif sql_body[i] == ')':
                    depth -= 1

            if depth == 0:
                # This ORDER BY is at the outermost level - remove it
                # Find the extent: from ORDER BY to end of line(s) until we hit
                # a new SQL keyword, semicolon, or end of content
                order_start = config_end + pos
                # Match ORDER BY ... up to the next top-level keyword, semicolon, or end
                remaining = content[order_start:]
                end_match = re.search(
                    r'\n\s*(?:LIMIT\b|OFFSET\b|;|\Z)',
                    remaining,
                    re.IGNORECASE
                )
                if end_match:
                    order_end = order_start + end_match.start()
                else:
                    order_end = len(content)

                # Remove the ORDER BY clause (preserve any trailing newline)
                new_content = content[:order_start].rstrip() + content[order_end:]
                new_content = new_content.rstrip() + '\n'
                warnings.append("Removed ORDER BY from TABLE definition (ignored by BigQuery)")
                return new_content, warnings

        return content, warnings

    def remove_order_by_from_ctes(self, content: str) -> Tuple[str, List[str]]:
        """Remove ORDER BY clauses from inside CTEs.

        BigQuery rejects ORDER BY inside CTEs/subqueries unless paired with LIMIT.
        Source SQ SQL from Informatica often includes ORDER BY that was valid in Sybase
        but is invalid inside a BigQuery CTE.

        Uses parenthesis-depth tracking to find ORDER BY at depth > 0 (inside CTEs)
        and removes them. Skips ORDER BY that has a LIMIT clause nearby.
        """
        warnings = []

        # Split config from SQL
        config_end = 0
        config_match = re.search(r'config\s*\{', content)
        if config_match:
            depth = 0
            for i in range(config_match.start(), len(content)):
                if content[i] == '{':
                    depth += 1
                elif content[i] == '}':
                    depth -= 1
                    if depth == 0:
                        config_end = i + 1
                        break

        sql_body = content[config_end:]

        # Find ORDER BY matches
        order_by_matches = list(re.finditer(r'\bORDER\s+BY\b', sql_body, re.IGNORECASE))
        if not order_by_matches:
            return content, warnings

        removals = []

        for match in order_by_matches:
            pos = match.start()

            # Count paren depth AND track the position of the immediately
            # enclosing open-paren (skip string literals/templates).
            depth = 0
            in_string = False
            in_template = False
            # Stack of open-paren positions; top = innermost enclosing paren
            paren_stack: list[int] = []
            for i in range(pos):
                c = sql_body[i]
                if in_template:
                    if c == '}':
                        in_template = False
                    continue
                if c == '$' and i + 1 < pos and sql_body[i + 1] == '{':
                    in_template = True
                    continue
                if c == "'" and not in_template:
                    in_string = not in_string
                    continue
                if in_string:
                    continue
                if c == '(':
                    depth += 1
                    paren_stack.append(i)
                elif c == ')':
                    depth -= 1
                    if paren_stack:
                        paren_stack.pop()

            if depth <= 0:
                continue  # Skip depth-0 ORDER BY (handled by remove_order_by_from_tables)

            # Skip ORDER BY inside window functions: OVER (...ORDER BY...).
            # The immediately enclosing open-paren should be preceded by OVER.
            if paren_stack:
                enclosing_paren_pos = paren_stack[-1]
                # Look backward from the open paren, skipping whitespace
                prefix = sql_body[:enclosing_paren_pos].rstrip()
                if prefix.upper().endswith('OVER'):
                    continue  # Window function ORDER BY — must keep

            # Check for LIMIT after ORDER BY (ORDER BY + LIMIT is valid)
            after_order = sql_body[match.end():]
            # Find next clause boundary: closing paren at same depth, or next CTE keyword
            has_limit = False
            scan_depth = 0
            for i, c in enumerate(after_order):
                if c == '(':
                    scan_depth += 1
                elif c == ')':
                    if scan_depth == 0:
                        break  # Hit the CTE closing paren
                    scan_depth -= 1
                elif c == '\n':
                    # Check if LIMIT is on the next line
                    rest_of_line = after_order[i:].lstrip()
                    if rest_of_line.upper().startswith('LIMIT'):
                        has_limit = True
                        break

            if has_limit:
                continue

            # Find the extent of the ORDER BY clause.
            # ORDER BY is always the last clause before the closing ')' of a CTE
            # subquery, so scan forward tracking paren depth until we hit ')' at
            # depth 0.  This correctly handles multi-line column lists like:
            #   ORDER BY
            #     F.linkJkptOpnPollHrID,
            #     F.SiteID
            #   )
            remaining = sql_body[pos:]
            end_offset = len(remaining)
            scan_depth = 0
            for i, c in enumerate(remaining):
                if c == '(':
                    scan_depth += 1
                elif c == ')':
                    if scan_depth == 0:
                        end_offset = i
                        break
                    scan_depth -= 1

            abs_start = config_end + pos
            abs_end = config_end + pos + end_offset
            removals.append((abs_start, abs_end))

        if not removals:
            return content, warnings

        # Apply removals in reverse order
        new_content = content
        for start, end in reversed(removals):
            new_content = new_content[:start].rstrip() + new_content[end:]

        if new_content != content:
            warnings.append(f"Removed ORDER BY from {len(removals)} CTE(s) (BigQuery rejects ORDER BY without LIMIT in CTEs)")

        return new_content, warnings

    def standardize_key_column_types(self, content: str) -> Tuple[str, List[str]]:
        """Standardize types for key columns to ensure JOIN compatibility.

        Based on analysis of the codebase:
        - SiteID: STRING (242 files) vs INT64 (10 files) -> standardize to STRING
        - PatronID: STRING (58 files) vs INT64 (11 files) -> standardize to STRING
        - DayID: INT64 (40 files) vs STRING (3 files) -> standardize to INT64
        - HourID: STRING (majority) -> standardize to STRING
        - ProductID: STRING -> standardize to STRING
        - GamingMachineID: STRING -> standardize to STRING

        Returns:
            Tuple of (fixed_content, list_of_warnings)
        """
        warnings = []
        changes = []

        # Define standard types for key columns
        # Format: (column_pattern, wrong_type, correct_type)
        type_standards = [
            # SiteID should be STRING
            (r'CAST\s*\(\s*(\w*SiteID)\s+AS\s+INT64\s*\)', 'INT64', 'STRING', 'SiteID'),
            (r'CAST\s*\(\s*(\w*Site_ID)\s+AS\s+INT64\s*\)', 'INT64', 'STRING', 'Site_ID'),
            # PatronID should be STRING
            (r'CAST\s*\(\s*(\w*PatronID)\s+AS\s+INT64\s*\)', 'INT64', 'STRING', 'PatronID'),
            (r'CAST\s*\(\s*(\w*Patron_ID)\s+AS\s+INT64\s*\)', 'INT64', 'STRING', 'Patron_ID'),
            # DayID should be INT64
            (r'CAST\s*\(\s*(\w*DayID)\s+AS\s+STRING\s*\)', 'STRING', 'INT64', 'DayID'),
            (r'CAST\s*\(\s*(\w*Day_ID)\s+AS\s+STRING\s*\)', 'STRING', 'INT64', 'Day_ID'),
            (r'CAST\s*\(\s*(\w*DAYID)\s+AS\s+STRING\s*\)', 'STRING', 'INT64', 'DAYID'),
            # HourID should be STRING (for consistency with existing majority)
            (r'CAST\s*\(\s*(\w*HourID)\s+AS\s+INT64\s*\)', 'INT64', 'STRING', 'HourID'),
            (r'CAST\s*\(\s*(\w*HOURID)\s+AS\s+INT64\s*\)', 'INT64', 'STRING', 'HOURID'),
            # MonthID should be INT64
            (r'CAST\s*\(\s*(\w*MonthID)\s+AS\s+STRING\s*\)', 'STRING', 'INT64', 'MonthID'),
            (r'CAST\s*\(\s*(\w*MONTHID)\s+AS\s+STRING\s*\)', 'STRING', 'INT64', 'MONTHID'),
            # WeekID should be INT64
            (r'CAST\s*\(\s*(\w*WeekID)\s+AS\s+STRING\s*\)', 'STRING', 'INT64', 'WeekID'),
            (r'CAST\s*\(\s*(\w*WEEKID)\s+AS\s+STRING\s*\)', 'STRING', 'INT64', 'WEEKID'),

            # ETL Tracking Columns -> INT64 (more efficient for tracking IDs)
            (r'CAST\s*\(\s*(\w*ETLJobDtlID)\s+AS\s+STRING\s*\)', 'STRING', 'INT64', 'ETLJobDtlID'),
            (r'CAST\s*\(\s*(\w*RejectETLJobDtlID)\s+AS\s+STRING\s*\)', 'STRING', 'INT64', 'RejectETLJobDtlID'),
            (r'CAST\s*\(\s*(\w*InitialRejectETLJobDtlID)\s+AS\s+STRING\s*\)', 'STRING', 'INT64', 'InitialRejectETLJobDtlID'),
            (r'CAST\s*\(\s*(\w*ReloadETLJobDtlID)\s+AS\s+STRING\s*\)', 'STRING', 'INT64', 'ReloadETLJobDtlID'),

            # Flag/Boolean Columns -> INT64 (boolean-like)
            (r'CAST\s*\(\s*(ActvFlg)\s+AS\s+STRING\s*\)', 'STRING', 'INT64', 'ActvFlg'),
            (r'CAST\s*\(\s*(CardBlocked)\s+AS\s+STRING\s*\)', 'STRING', 'INT64', 'CardBlocked'),
            (r'CAST\s*\(\s*(ATSActv)\s+AS\s+STRING\s*\)', 'STRING', 'INT64', 'ATSActv'),
            (r'CAST\s*\(\s*(ATSBal)\s+AS\s+STRING\s*\)', 'STRING', 'INT64', 'ATSBal'),
            (r'CAST\s*\(\s*(ATSRated)\s+AS\s+STRING\s*\)', 'STRING', 'INT64', 'ATSRated'),

            # ID Columns -> STRING (BigQuery best practice for large IDs and JOIN safety)
            (r'CAST\s*\(\s*(CardID)\s+AS\s+INT64\s*\)', 'INT64', 'STRING', 'CardID'),
            (r'CAST\s*\(\s*(DeviceID)\s+AS\s+INT64\s*\)', 'INT64', 'STRING', 'DeviceID'),
            (r'CAST\s*\(\s*(CommitRollbackID)\s+AS\s+INT64\s*\)', 'INT64', 'STRING', 'CommitRollbackID'),
            (r'CAST\s*\(\s*(PatronGrpID)\s+AS\s+INT64\s*\)', 'INT64', 'STRING', 'PatronGrpID'),
            (r'CAST\s*\(\s*(LegacyPromoID)\s+AS\s+INT64\s*\)', 'INT64', 'STRING', 'LegacyPromoID'),
            (r'CAST\s*\(\s*(LegacyCheckID)\s+AS\s+INT64\s*\)', 'INT64', 'STRING', 'LegacyCheckID'),
            (r'CAST\s*\(\s*(LegacyAdjRsnNum)\s+AS\s+INT64\s*\)', 'INT64', 'STRING', 'LegacyAdjRsnNum'),
            (r'CAST\s*\(\s*(LegacyAdjID)\s+AS\s+INT64\s*\)', 'INT64', 'STRING', 'LegacyAdjID'),
            (r'CAST\s*\(\s*(StsID)\s+AS\s+INT64\s*\)', 'INT64', 'STRING', 'StsID'),
            (r'CAST\s*\(\s*(ProgNum)\s+AS\s+INT64\s*\)', 'INT64', 'STRING', 'ProgNum'),
            (r'CAST\s*\(\s*(Link_ID)\s+AS\s+INT64\s*\)', 'INT64', 'STRING', 'Link_ID'),
            (r'CAST\s*\(\s*(PatronNumber)\s+AS\s+INT64\s*\)', 'INT64', 'STRING', 'PatronNumber'),
            (r'CAST\s*\(\s*(PtyLocNum)\s+AS\s+INT64\s*\)', 'INT64', 'STRING', 'PtyLocNum'),

            # Legacy Patron ID Columns -> STRING
            (r'CAST\s*\(\s*(Mel_LegacyPatronNum)\s+AS\s+INT64\s*\)', 'INT64', 'STRING', 'Mel_LegacyPatronNum'),
            (r'CAST\s*\(\s*(Per_LegacyPatronNum)\s+AS\s+INT64\s*\)', 'INT64', 'STRING', 'Per_LegacyPatronNum'),
        ]

        for pattern, wrong_type, correct_type, col_name in type_standards:
            if re.search(pattern, content, re.IGNORECASE):
                # Replace wrong type with correct type
                def replace_type(match):
                    col = match.group(1)
                    return f'CAST({col} AS {correct_type})'

                new_content = re.sub(pattern, replace_type, content, flags=re.IGNORECASE)
                if new_content != content:
                    changes.append(f'{col_name}: {wrong_type} -> {correct_type}')
                    content = new_content

        if changes:
            warnings.append(f"Standardized key column types: {', '.join(changes)}")

        return content, warnings

    def fix_datetime_column_types(self, content: str) -> Tuple[str, List[str]]:
        """Convert datetime columns incorrectly cast as STRING to TIMESTAMP or DATE.

        Columns ending in DtTm, DateTime, Timestamp should be TIMESTAMP, not STRING.
        Columns ending in Date should be DATE, not STRING.

        Returns:
            Tuple of (fixed_content, list_of_warnings)
        """
        warnings = []
        timestamp_changes = []
        date_changes = []

        # Patterns for datetime columns cast as STRING -> TIMESTAMP
        datetime_patterns = [
            # CAST(ColDtTm AS STRING) -> CAST(ColDtTm AS TIMESTAMP)
            (r'CAST\s*\(\s*(\w+(?:DtTm|Dttm))\s+AS\s+STRING\s*\)', 'DtTm'),
            # CAST(ColDateTime AS STRING) -> CAST(ColDateTime AS TIMESTAMP)
            (r'CAST\s*\(\s*(\w+DateTime)\s+AS\s+STRING\s*\)', 'DateTime'),
            # CAST(ColTimestamp AS STRING) -> CAST(ColTimestamp AS TIMESTAMP)
            (r'CAST\s*\(\s*(\w+Timestamp)\s+AS\s+STRING\s*\)', 'Timestamp'),
        ]

        for pattern, suffix in datetime_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            if matches:
                def replace_with_timestamp(match):
                    col = match.group(1)
                    return f'CAST({col} AS TIMESTAMP)'

                new_content = re.sub(pattern, replace_with_timestamp, content, flags=re.IGNORECASE)
                if new_content != content:
                    timestamp_changes.extend(matches[:3])  # Only report first 3
                    content = new_content

        # Pattern for date columns cast as STRING -> DATE
        # Matches columns ending in 'Date' but NOT 'DateTime' (already handled above)
        # Uses negative lookbehind to exclude DateTime columns
        date_pattern = r'CAST\s*\(\s*(\w+(?<!DateTime)[Dd]ate)\s+AS\s+STRING\s*\)'
        date_matches = re.findall(date_pattern, content)
        if date_matches:
            # Filter out any that end in DateTime (extra safety)
            date_matches = [m for m in date_matches if not m.lower().endswith('datetime')]
            if date_matches:
                def replace_with_date(match):
                    col = match.group(1)
                    # Skip if this is actually a DateTime column
                    if col.lower().endswith('datetime'):
                        return match.group(0)
                    return f'CAST({col} AS DATE)'

                new_content = re.sub(date_pattern, replace_with_date, content)
                if new_content != content:
                    date_changes.extend(date_matches[:3])  # Only report first 3
                    content = new_content

        if timestamp_changes:
            if len(timestamp_changes) > 3:
                warnings.append(f"Converted {len(timestamp_changes)} datetime columns from STRING to TIMESTAMP: {', '.join(timestamp_changes[:3])}...")
            else:
                warnings.append(f"Converted datetime columns from STRING to TIMESTAMP: {', '.join(timestamp_changes)}")

        if date_changes:
            if len(date_changes) > 3:
                warnings.append(f"Converted {len(date_changes)} date columns from STRING to DATE: {', '.join(date_changes[:3])}...")
            else:
                warnings.append(f"Converted date columns from STRING to DATE: {', '.join(date_changes)}")

        return content, warnings

    def standardize_column_casing(self, content: str) -> Tuple[str, List[str]]:
        """Standardize casing for common columns to ensure consistency.

        Uses the most common casing found in the codebase for each column.

        Returns:
            Tuple of (fixed_content, list_of_warnings)
        """
        warnings = []
        changes = []

        # Define standard casing for common columns
        # Format: (pattern_to_find, standard_casing)
        casing_standards = [
            # SiteID variations -> SiteID
            (r'\bSiteid\b', 'SiteID'),
            (r'\bsiteID\b', 'SiteID'),
            (r'\bsite_id\b', 'SiteID'),  # Keep snake_case as is for Dataform vars
            # PatronID variations -> PatronID
            (r'\bPatronid\b', 'PatronID'),
            (r'\bpatronID\b', 'PatronID'),
            (r'\bPATRONID\b', 'PatronID'),
            # DayID variations -> DayID
            (r'\bDayid\b', 'DayID'),
            (r'\bdayID\b', 'DayID'),
            # Only fix when used as column alias in AS clause (not in all contexts)
            # HourID variations -> HourID
            (r'\bHourid\b', 'HourID'),
            (r'\bhourID\b', 'HourID'),
            # ETLJobDtlID variations -> ETLJobDtlID
            (r'\bEtlJobDtlID\b', 'ETLJobDtlID'),
            (r'\bETLjobDTLID\b', 'ETLJobDtlID'),
            (r'\bEtljobdtlid\b', 'ETLJobDtlID'),
            # SctyCde variations -> SctyCde
            (r'\bSctycde\b', 'SctyCde'),
            (r'\bSCTYCDE\b', 'SctyCde'),
        ]

        for pattern, standard in casing_standards:
            if re.search(pattern, content):
                new_content = re.sub(pattern, standard, content)
                if new_content != content:
                    changes.append(f'{pattern} -> {standard}')
                    content = new_content

        if changes:
            warnings.append(f"Standardized column casing: {len(changes)} fixes")

        return content, warnings

    def fix_staging_table_cycles(self, content: str) -> Tuple[str, List[str]]:
        """Fix dependency cycles caused by ${ref()} in description strings.

        Procedures and staging tables often have ${ref('table_name')} in
        OPTIONS(description=...) or config description which creates a false
        dependency. Replace with plain table name.

        Handles both single-quoted and triple-quoted description strings.

        Returns:
            Tuple of (fixed_content, list_of_warnings)
        """
        warnings = []

        # Only apply to operations type files (procedures)
        if 'type: "operations"' not in content or 'hasOutput: true' not in content:
            return content, warnings

        # Find the OPTIONS block boundaries (OPTIONS( ... ) before BEGIN)
        options_match = re.search(r'OPTIONS\s*\(', content, re.IGNORECASE)
        if not options_match:
            return content, warnings

        # Find matching closing paren for OPTIONS, tracking depth
        start = options_match.end()
        depth = 1
        pos = start
        in_triple_quote = False
        in_single_quote = False
        while pos < len(content) and depth > 0:
            ch = content[pos]
            # Track triple-quoted strings (""")
            if content[pos:pos+3] == '"""':
                in_triple_quote = not in_triple_quote
                pos += 3
                continue
            if not in_triple_quote:
                if ch == "'" and not in_single_quote:
                    in_single_quote = True
                elif ch == "'" and in_single_quote:
                    in_single_quote = False
                elif not in_single_quote:
                    if ch == '(':
                        depth += 1
                    elif ch == ')':
                        depth -= 1
            pos += 1

        if depth != 0:
            return content, warnings

        options_end = pos  # position after the closing )
        options_block = content[options_match.start():options_end]

        # Replace ${ref()} with plain table name within the OPTIONS block only
        ref_pattern = re.compile(r"\$\{ref\(['\"]([^'\"]+)['\"]\)\}")
        new_options = ref_pattern.sub(lambda m: m.group(1), options_block)

        if new_options != options_block:
            fix_count = len(ref_pattern.findall(options_block))
            content = content[:options_match.start()] + new_options + content[options_end:]
            warnings.append(f"Fixed dependency cycle: removed {fix_count} ${{ref()}} from OPTIONS description")

        return content, warnings

    def fix_invalid_partition_columns(self, content: str) -> Tuple[str, List[str]]:
        """Fix invalid partition column types.

        BigQuery requires partitionBy columns to be DATE, DATETIME, TIMESTAMP, or INT64.
        If partitioning on a STRING column, change to _ingestion_timestamp.

        Returns:
            Tuple of (fixed_content, list_of_warnings)
        """
        warnings = []

        # Find partitionBy in config
        partition_match = re.search(r'partitionBy:\s*["\'](\w+)["\']', content)
        if not partition_match:
            return content, warnings

        partition_col = partition_match.group(1)

        # Skip if already using a valid column
        if partition_col in ('_ingestion_timestamp', '_PARTITIONTIME', '_PARTITIONDATE'):
            return content, warnings

        # Check if the column is cast as STRING in the SELECT
        # Pattern: CAST(ColumnName AS STRING) AS ColumnName
        string_pattern = rf'CAST\s*\(\s*{re.escape(partition_col)}\s+AS\s+STRING\s*\)'
        if re.search(string_pattern, content, re.IGNORECASE):
            # Change partition column to _ingestion_timestamp
            old_partition = f'partitionBy: "{partition_col}"'
            new_partition = 'partitionBy: "_ingestion_timestamp"'
            content = content.replace(old_partition, new_partition)

            # Also try with single quotes
            old_partition_sq = f"partitionBy: '{partition_col}'"
            content = content.replace(old_partition_sq, new_partition)

            warnings.append(f"Changed partition column from STRING '{partition_col}' to '_ingestion_timestamp'")

        return content, warnings

    def fix_missing_semicolons(self, content: str) -> Tuple[str, List[str]]:
        """Add missing semicolons to operations type SQL statements.

        BigQuery requires semicolons at the end of SQL statements.
        This fixes common patterns:
        - CREATE TABLE ... ) -> CREATE TABLE ... );
        - END -> END;

        Returns:
            Tuple of (fixed_content, list_of_warnings)
        """
        warnings = []

        # Only apply to operations type files
        if 'type: "operations"' not in content and "type: 'operations'" not in content:
            return content, warnings

        # Find the config block end
        config_match = re.search(r'config\s*\{', content)
        if not config_match:
            return content, warnings

        # Find end of config block
        start = config_match.end()
        brace_count = 1
        pos = start
        while pos < len(content) and brace_count > 0:
            if content[pos] == '{':
                brace_count += 1
            elif content[pos] == '}':
                brace_count -= 1
            pos += 1

        # Get SQL part after config
        sql_part = content[pos:]

        # Remove trailing whitespace and comments to find actual end
        sql_stripped = sql_part.rstrip()
        # Remove trailing line comments
        lines = sql_stripped.split('\n')
        while lines and lines[-1].strip().startswith('--'):
            lines.pop()
        sql_stripped = '\n'.join(lines).rstrip()

        # Check if it needs a semicolon
        if sql_stripped and not sql_stripped.endswith(';'):
            # Find what the last non-whitespace content is
            last_content = sql_stripped[-20:] if len(sql_stripped) > 20 else sql_stripped

            # Add semicolon
            # Find position in original content where we need to add it
            sql_end_pos = pos + len(sql_part.rstrip())

            # Insert semicolon before any trailing whitespace
            new_content = content[:sql_end_pos].rstrip() + ';\n'

            warnings.append("Added missing semicolon at end of SQL statement")
            return new_content, warnings

        return content, warnings

    def fix_procedure_missing_param_paren(self, content: str) -> Tuple[str, List[str]]:
        """Fix missing closing paren in procedure parameter definitions.

        Detects pattern:
            CREATE OR REPLACE PROCEDURE name(
              param1 TYPE,
              param2 TYPE
            BEGIN  <-- Missing ) before BEGIN

        Returns:
            Tuple of (fixed_content, list_of_warnings)
        """
        warnings = []

        # Pattern: CREATE PROCEDURE with ( but BEGIN on a separate line without )
        # Look for: line with "PROCEDURE" and "(", then lines with params, then "BEGIN" without ")"
        lines = content.split('\n')
        fixed_lines = []
        in_proc_params = False
        proc_paren_line = -1

        for i, line in enumerate(lines):
            stripped = line.strip().upper()

            # Detect start of procedure with open paren
            if 'PROCEDURE' in stripped and '(' in line and ')' not in line:
                in_proc_params = True
                proc_paren_line = i

            # Detect BEGIN or OPTIONS without closing the procedure params
            elif in_proc_params and (stripped.startswith('BEGIN') or stripped.startswith('OPTIONS')):
                # Check if previous non-empty line has the closing paren
                prev_line = fixed_lines[-1].strip() if fixed_lines else ''
                if not prev_line.endswith(')'):
                    # Add closing paren to the previous line
                    if fixed_lines:
                        fixed_lines[-1] = fixed_lines[-1].rstrip() + ')'
                        warnings.append(f"Added missing ) to close procedure parameters before {stripped.split()[0]}")
                in_proc_params = False

            # Detect explicit closing of params
            elif in_proc_params and stripped.startswith(')'):
                in_proc_params = False

            fixed_lines.append(line)

        if warnings:
            return '\n'.join(fixed_lines), warnings
        return content, warnings

    def fix_unclosed_options_block(self, content: str) -> Tuple[str, List[str]]:
        """Fix missing closing paren in OPTIONS block before BEGIN.

        LLM sometimes generates:
            OPTIONS(
              description="..."

            BEGIN
        instead of:
            OPTIONS(
              description="..."
            )
            BEGIN

        This tracks paren depth within OPTIONS to detect unclosed blocks.
        """
        warnings = []
        lines = content.split('\n')
        fixed_lines = []
        in_options = False
        options_depth = 0

        for i, line in enumerate(lines):
            stripped = line.strip()
            upper = stripped.upper()

            # Detect start of OPTIONS block
            if re.match(r'OPTIONS\s*\(', stripped, re.IGNORECASE) and not in_options:
                in_options = True
                options_depth = 0
                # Count parens on this line (outside strings)
                clean = re.sub(r'""".*?"""', '', stripped, flags=re.DOTALL)
                clean = re.sub(r'"[^"]*"', '', clean)
                clean = re.sub(r"'[^']*'", '', clean)
                options_depth += clean.count('(') - clean.count(')')
                if options_depth <= 0:
                    in_options = False  # Fully closed on same line

            elif in_options:
                # Count parens (outside strings) to track depth
                clean = re.sub(r'""".*?"""', '', stripped, flags=re.DOTALL)
                clean = re.sub(r'"[^"]*"', '', clean)
                clean = re.sub(r"'[^']*'", '', clean)
                options_depth += clean.count('(') - clean.count(')')

                if options_depth <= 0:
                    in_options = False  # OPTIONS properly closed

                # If we hit BEGIN while OPTIONS is still open
                elif upper == 'BEGIN' or upper.startswith('BEGIN '):
                    # Insert missing ) before BEGIN
                    fixed_lines.append(')')
                    warnings.append("Added missing ) to close OPTIONS block before BEGIN")
                    in_options = False

            fixed_lines.append(line)

        if warnings:
            return '\n'.join(fixed_lines), warnings
        return content, warnings

    def fix_backtick_procedure_names(self, content: str) -> Tuple[str, List[str]]:
        """Fix backtick-quoted procedure names that wrap ${self.schema} templates.

        LLM sometimes generates:
            CREATE OR REPLACE PROCEDURE `${self.schema}.ProcName`(...)
        Should be:
            CREATE OR REPLACE PROCEDURE ${self.schema}.ProcName(...)

        Backticks around template expressions prevent Dataform from resolving them.
        """
        warnings = []

        # Pattern: `${self.schema}.anything`  ->  ${self.schema}.anything
        pattern = r'`(\$\{self\.schema\}\.[^`]+)`'
        if re.search(pattern, content):
            content = re.sub(pattern, r'\1', content)
            warnings.append("Removed backticks from procedure name containing ${self.schema}")

        return content, warnings

    def fix_declaration_missing_database(self, content: str) -> Tuple[str, List[str]]:
        """Add missing database property to declaration configs.

        Declarations need a database property for proper resolution.
        Adds: database: dataform.projectConfig.defaultDatabase

        Returns:
            Tuple of (fixed_content, list_of_warnings)
        """
        warnings = []

        # Only apply to declaration type files
        if 'type: "declaration"' not in content and "type: 'declaration'" not in content:
            return content, warnings

        # Check if database property already exists
        if 'database:' in content:
            return content, warnings

        # Find the type: "declaration" line and add database after it
        # Pattern: type: "declaration", followed by optional whitespace and newline
        pattern = r'(type:\s*["\']declaration["\'],?\s*\n)'
        match = re.search(pattern, content)
        if match:
            # Insert database property after type line
            indent = '  '  # Standard 2-space indent
            database_line = f'{indent}database: dataform.projectConfig.defaultDatabase,\n'
            new_content = content[:match.end()] + database_line + content[match.end():]
            warnings.append("Added missing database property to declaration")
            return new_content, warnings

        return content, warnings

    def fix_unquoted_declaration_values(self, content: str) -> Tuple[str, List[str]]:
        """Quote unquoted database and schema values in declaration configs.

        Fixes: database: crown-ddas-dev -> database: "crown-ddas-dev"
        Fixes: schema: DACOM_MEL_Access -> schema: "DACOM_MEL_Access"

        Without quotes, Dataform interprets these as JavaScript expressions
        and fails with "crown is not defined" or similar errors.

        Returns:
            Tuple of (fixed_content, list_of_warnings)
        """
        warnings = []

        # Only apply to declaration type files
        if 'type: "declaration"' not in content and "type: 'declaration'" not in content:
            return content, warnings

        original = content

        # Fix unquoted database values
        # Pattern: database: followed by value without quotes (not already quoted, not a JS expression)
        # Match: database: crown-ddas-dev, or database: some_db,
        # Don't match: database: "quoted", database: dataform.projectConfig..., database: (expression)
        db_pattern = r'(\bdatabase:\s*)([a-zA-Z][a-zA-Z0-9_-]+)(\s*[,\n}])'
        content = re.sub(db_pattern, r'\1"\2"\3', content)

        # Fix unquoted schema values
        # Pattern: schema: followed by value without quotes
        # Match: schema: DACOM_MEL_Access, or schema: some_schema,
        # Don't match: schema: "quoted", schema: (expression), schema: dataform...
        schema_pattern = r'(\bschema:\s*)([a-zA-Z][a-zA-Z0-9_-]+)(\s*[,\n}])'
        content = re.sub(schema_pattern, r'\1"\2"\3', content)

        if content != original:
            warnings.append("Fixed unquoted database/schema values in declaration config")

        return content, warnings

    def fix_reserved_keyword_columns(self, content: str) -> Tuple[str, List[str]]:
        """Quote column names that are BigQuery reserved keywords.

        BigQuery reserved words used as column identifiers must be backtick-quoted.
        This method handles general column reference patterns, not just CAST.

        Patterns fixed:
        - alias.GROUPS -> alias.`GROUPS`  (qualified column reference)
        - AS GROUPS,   -> AS `GROUPS`,    (alias definition)
        - GROUPS AS x  -> `GROUPS` AS x   (unqualified column in SELECT)
        - CAST(TO AS TYPE) AS TO -> CAST(`TO` AS TYPE) AS `TO`

        Only a curated subset of reserved words that plausibly appear as column
        names are checked.  Structural SQL keywords (SELECT, FROM, WHERE, etc.)
        are skipped because they never appear as column names in practice and
        matching them would corrupt valid SQL.

        Returns:
            Tuple of (fixed_content, list_of_warnings)
        """
        warnings = []

        # Curated subset: reserved words that realistically appear as column names
        # in Informatica/CDW data.  Excludes structural SQL keywords like SELECT,
        # FROM, WHERE, JOIN, etc. that would never be column names.
        COLUMN_RESERVED_WORDS = {
            'all', 'any', 'array', 'at', 'current', 'default', 'end', 'escape',
            'exists', 'extract', 'fetch', 'for', 'full', 'group', 'groups',
            'grouping', 'hash', 'if', 'ignore', 'interval', 'is', 'lateral',
            'limit', 'lookup', 'merge', 'natural', 'new', 'no', 'not', 'null',
            'nulls', 'of', 'over', 'partition', 'preceding', 'proto', 'range',
            'recursive', 'respect', 'rollup', 'rows', 'set', 'some', 'struct',
            'to', 'treat', 'unbounded', 'window', 'within',
        }

        changes = []

        def _escape_col(match):
            """Backtick-quote the reserved word, preserving surrounding context."""
            prefix = match.group(1)
            word = match.group(2)
            suffix = match.group(3)
            return f"{prefix}`{word}`{suffix}"

        for word in COLUMN_RESERVED_WORDS:
            # Pattern 1: qualified reference  alias.groups -> alias.`groups`
            # Negative lookahead (?!`) avoids double-quoting
            pattern = rf'(\b\w+\.)({word})(\b)(?!`)'
            new_content = re.sub(pattern, _escape_col, content, flags=re.IGNORECASE)
            if new_content != content:
                changes.append(word)
                content = new_content

            # Pattern 2: unqualified column followed by comma  \n  groups, -> `groups`,
            pattern = rf'(\s)({word})(,)'
            new_content = re.sub(pattern, _escape_col, content, flags=re.IGNORECASE)
            if new_content != content:
                changes.append(word)
                content = new_content

            # Pattern 3: column followed by newline (last col before FROM etc.)
            pattern = rf'(\s)({word})(\s*\n)'
            new_content = re.sub(pattern, _escape_col, content, flags=re.IGNORECASE)
            if new_content != content:
                changes.append(word)
                content = new_content

            # Pattern 4: AS KEYWORD (alias definition)
            # e.g. "... AS groups" at word boundary
            pattern = rf'(\bAS\s+)({word})(\b)(?!`)'
            new_content = re.sub(pattern, _escape_col, content, flags=re.IGNORECASE)
            if new_content != content:
                changes.append(word)
                content = new_content

            # Pattern 5: CAST(KEYWORD AS TYPE) — keyword used inside CAST
            pattern = rf'(\bCAST\s*\(\s*)({word})(\s+AS\b)'
            new_content = re.sub(pattern, _escape_col, content, flags=re.IGNORECASE)
            if new_content != content:
                changes.append(word)
                content = new_content

        if changes:
            unique_changes = sorted(set(changes))
            warnings.append(f"Quoted reserved keyword column(s): {', '.join(unique_changes[:10])}")

        return content, warnings

    def fix_unbalanced_parentheses(self, content: str) -> Tuple[str, List[str]]:
        """Fix unbalanced parentheses in SQL content.

        Handles common patterns:
        - Stray closing paren at end of line (e.g., "WHERE x = y)")
        - Stray closing paren at end of file

        Does NOT attempt to fix truly truncated files (more opening than closing).

        Returns:
            Tuple of (fixed_content, list_of_warnings)
        """
        warnings = []

        # Find the config block end
        config_match = re.search(r'config\s*\{', content)
        if not config_match:
            return content, warnings

        # Find end of config block
        start = config_match.end()
        brace_count = 1
        pos = start
        while pos < len(content) and brace_count > 0:
            if content[pos] == '{':
                brace_count += 1
            elif content[pos] == '}':
                brace_count -= 1
            pos += 1

        config_end = pos

        # Get SQL part after config
        sql_part = content[config_end:]

        # Remove strings and comments for accurate paren counting
        # CRITICAL: Protect ${...} template expressions FIRST, because they contain
        # single quotes (e.g., ${ref('table')}) that the string literal regex would
        # eat — matching from one ref's quote to the next ref's quote and destroying
        # all parentheses in between.
        sql_clean = re.sub(r'\$\{[^}]+\}', '__TPL__', sql_part)  # Protect templates
        sql_clean = re.sub(r'/\*.*?\*/', '', sql_clean, flags=re.DOTALL)  # Remove block comments
        sql_clean = re.sub(r"'[^']*'", '', sql_clean)  # Remove string literals
        sql_clean = re.sub(r'--[^\n]*', '', sql_clean)  # Remove line comments

        open_count = sql_clean.count('(')
        close_count = sql_clean.count(')')

        if open_count == close_count:
            return content, warnings

        if open_count > close_count:
            # Truncated file - more opening than closing
            # Don't attempt to fix, just warn (file needs regeneration)
            warnings.append(f"Truncated SQL: {open_count} opening vs {close_count} closing parens - needs regeneration")
            return content, warnings

        # More closing than opening - look for stray closing parens
        extra_close = close_count - open_count

        # Strategy: Remove stray ) at end of lines that don't have matching (
        lines = sql_part.split('\n')
        fixed_lines = []
        removed = 0

        for line in lines:
            if removed >= extra_close:
                fixed_lines.append(line)
                continue

            # Check if line ends with ) that might be stray
            stripped = line.rstrip()
            if stripped.endswith(')'):
                # Count parens in this line
                line_clean = re.sub(r"'[^']*'", '', stripped)
                line_clean = re.sub(r'--[^\n]*', '', line_clean)
                line_open = line_clean.count('(')
                line_close = line_clean.count(')')

                if line_close > line_open:
                    # This line has extra closing parens
                    # Remove trailing ) one at a time
                    while stripped.endswith(')') and removed < extra_close:
                        # Recount after potential removal
                        test_line = stripped[:-1]
                        test_clean = re.sub(r"'[^']*'", '', test_line)
                        test_clean = re.sub(r'--[^\n]*', '', test_clean)
                        test_open = test_clean.count('(')
                        test_close = test_clean.count(')')

                        if test_close >= test_open:
                            # Safe to remove the trailing )
                            stripped = test_line.rstrip()
                            removed += 1
                        else:
                            break

                    # Reconstruct line with original trailing whitespace
                    trailing_ws = line[len(line.rstrip()):]
                    line = stripped + trailing_ws

            fixed_lines.append(line)

        if removed > 0:
            new_sql = '\n'.join(fixed_lines)
            new_content = content[:config_end] + new_sql
            warnings.append(f"Removed {removed} stray closing parenthesis(es)")
            return new_content, warnings

        return content, warnings

    def _split_select_columns(self, select_clause: str) -> List[str]:
        """Split a SELECT clause by commas, respecting nested parentheses.

        Args:
            select_clause: The column list portion of a SELECT statement

        Returns:
            List of individual column expressions
        """
        columns = []
        current = []
        depth = 0

        for char in select_clause:
            if char == '(':
                depth += 1
                current.append(char)
            elif char == ')':
                depth -= 1
                current.append(char)
            elif char == ',' and depth == 0:
                columns.append(''.join(current))
                current = []
            else:
                current.append(char)

        if current:
            columns.append(''.join(current))

        return columns

    def fix_is_null_comparison(self, content: str) -> Tuple[str, List[str]]:
        """Fix IS NULL = TRUE/FALSE anti-patterns.

        LLM sometimes generates redundant comparisons against boolean literals:
        - IS NULL = TRUE  → IS NULL
        - IS NULL = FALSE → IS NOT NULL
        - IS NOT NULL = TRUE  → IS NOT NULL
        - IS NOT NULL = FALSE → IS NULL

        These are valid SQL but verbose and confusing. BigQuery doesn't need
        the boolean comparison since IS NULL already returns a boolean.
        """
        warnings = []
        original = content

        # IS NOT NULL = FALSE → IS NULL
        content = re.sub(
            r'\bIS\s+NOT\s+NULL\s*=\s*FALSE\b',
            'IS NULL',
            content,
            flags=re.IGNORECASE
        )

        # IS NOT NULL = TRUE → IS NOT NULL (no-op, just remove = TRUE)
        content = re.sub(
            r'\bIS\s+NOT\s+NULL\s*=\s*TRUE\b',
            'IS NOT NULL',
            content,
            flags=re.IGNORECASE
        )

        # IS NULL = FALSE → IS NOT NULL
        content = re.sub(
            r'\bIS\s+NULL\s*=\s*FALSE\b',
            'IS NOT NULL',
            content,
            flags=re.IGNORECASE
        )

        # IS NULL = TRUE → IS NULL (no-op, just remove = TRUE)
        content = re.sub(
            r'\bIS\s+NULL\s*=\s*TRUE\b',
            'IS NULL',
            content,
            flags=re.IGNORECASE
        )

        if content != original:
            count = sum(1 for a, b in zip(content.split('\n'), original.split('\n')) if a != b)
            warnings.append(f"Fixed {count} IS NULL = TRUE/FALSE anti-patterns")

        return content, warnings

    def fix_date_format_strings(self, content: str) -> Tuple[str, List[str]]:
        """Fix Informatica/Sybase date format strings to BigQuery format.

        Replaces common legacy date format patterns with BigQuery-compatible formats:
        - 'yyyymmdd' -> '%Y%m%d'
        - 'yyyy-mm-dd' -> '%Y-%m-%d'
        - 'dd/mm/yyyy' -> '%d/%m/%Y'
        - 'mm/dd/yyyy' -> '%m/%d/%Y'
        - 'yyyymm' -> '%Y%m'
        - 'yyyy' -> '%Y'

        These patterns typically appear in PARSE_DATE, FORMAT_DATE, or CAST functions.

        Returns:
            Tuple of (fixed_content, list_of_warnings)
        """
        warnings = []
        original = content

        # Map of legacy formats to BigQuery formats
        # Order matters - check longer patterns first to avoid partial matches
        format_replacements = [
            # Date formats with separators
            (r"'yyyy-mm-dd'", "'%Y-%m-%d'"),
            (r"'yyyy/mm/dd'", "'%Y/%m/%d'"),
            (r"'dd-mm-yyyy'", "'%d-%m-%Y'"),
            (r"'dd/mm/yyyy'", "'%d/%m/%Y'"),
            (r"'mm-dd-yyyy'", "'%m-%d-%Y'"),
            (r"'mm/dd/yyyy'", "'%m/%d/%Y'"),
            # Date formats without separators
            (r"'yyyymmdd'", "'%Y%m%d'"),
            (r"'yyyymm'", "'%Y%m'"),
            (r"'mmddyyyy'", "'%m%d%Y'"),
            (r"'ddmmyyyy'", "'%d%m%Y'"),
            # Time formats
            (r"'hh:mi:ss'", "'%H:%M:%S'"),
            (r"'hh:mm:ss'", "'%H:%M:%S'"),
            (r"'hh24:mi:ss'", "'%H:%M:%S'"),
            # Combined datetime formats
            (r"'yyyy-mm-dd hh:mi:ss'", "'%Y-%m-%d %H:%M:%S'"),
            (r"'yyyy-mm-dd hh:mm:ss'", "'%Y-%m-%d %H:%M:%S'"),
            (r"'yyyy-mm-dd hh24:mi:ss'", "'%Y-%m-%d %H:%M:%S'"),
            # Year only
            (r"'yyyy'", "'%Y'"),
            # Month only
            (r"'mm'", "'%m'"),
            # Day only
            (r"'dd'", "'%d'"),
        ]

        changes = []
        for old_format, new_format in format_replacements:
            # Case-insensitive replacement
            pattern = re.compile(old_format, re.IGNORECASE)
            if pattern.search(content):
                content = pattern.sub(new_format, content)
                changes.append(f"{old_format} -> {new_format}")

        if content != original:
            warnings.append(f"Fixed date format strings: {', '.join(changes)}")

        return content, warnings

    def remove_unused_ctes(self, content: str) -> Tuple[str, List[str]]:
        """Remove CTEs that are defined but never referenced in the query.

        Common patterns:
        - cte_source defined but only cte_transformed is used
        - cte_expressions defined but main query doesn't reference it

        Returns:
            Tuple of (fixed_content, list_of_warnings)
        """
        warnings = []

        # Find all CTE definitions
        # Pattern: WITH cte_name AS ( or , cte_name AS (
        cte_pattern = r'(?:WITH|,)\s+(\w+)\s+AS\s*\('
        cte_names = re.findall(cte_pattern, content, re.IGNORECASE)

        if not cte_names:
            return content, warnings

        # Find the main query (after the last CTE)
        # Look for SELECT that's not inside a CTE
        # Find end of WITH clause by matching parentheses
        with_match = re.search(r'\bWITH\b', content, re.IGNORECASE)
        if not with_match:
            return content, warnings

        # Find all CTE references in the content (excluding definitions)
        # A CTE is referenced if it appears after FROM, JOIN, or in a subquery
        unused_ctes = []
        for cte_name in cte_names:
            # Check if CTE is referenced (not just defined)
            # Pattern: FROM cte_name, JOIN cte_name, or ${ref('cte_name')}
            ref_patterns = [
                rf'\bFROM\s+{cte_name}\b',
                rf'\bJOIN\s+{cte_name}\b',
                rf'\$\{{ref\([\'\"]{cte_name}[\'\"]',
                rf',\s*{cte_name}\b(?!\s+AS)',  # Comma-separated table list (but not AS definition)
            ]

            is_referenced = False
            for ref_pattern in ref_patterns:
                if re.search(ref_pattern, content, re.IGNORECASE):
                    is_referenced = True
                    break

            # Also check if referenced by another CTE
            for other_cte in cte_names:
                if other_cte != cte_name:
                    # Check if this CTE references the target CTE
                    cte_def_pattern = rf'\b{other_cte}\s+AS\s*\([^)]*\b{cte_name}\b'
                    if re.search(cte_def_pattern, content, re.IGNORECASE | re.DOTALL):
                        is_referenced = True
                        break

            if not is_referenced:
                unused_ctes.append(cte_name)

        if unused_ctes:
            # Remove unused CTEs
            for cte_name in unused_ctes:
                # Pattern to match the full CTE definition including its body
                # This is tricky because we need to match balanced parentheses

                # First, try to remove ", cte_name AS (...)"
                # Then try "WITH cte_name AS (...) ,"
                # Then try "WITH cte_name AS (...)" (only CTE)

                # For safety, just add a warning rather than auto-removing
                # Auto-removal of CTEs is risky without proper parenthesis matching
                pass

            warnings.append(f"Unused CTEs detected (consider removing): {', '.join(unused_ctes)}")

        return content, warnings

    def detect_scalar_subquery_lookups(self, content: str) -> List[str]:
        """Detect scalar subqueries that could be converted to JOINs.

        Pattern: (SELECT col FROM table WHERE condition) in SELECT clause
        These are inefficient and should be LEFT JOINs.

        Returns:
            List of warnings about detected scalar subqueries
        """
        warnings = []

        # Look for scalar subqueries in SELECT clause
        # Pattern: opening paren followed by SELECT, then FROM and WHERE in same subquery
        # We use a simpler approach: find all (SELECT and count those that look like scalar lookups

        # Find positions of all (SELECT patterns
        scalar_starts = list(re.finditer(r'\(\s*SELECT\b', content, re.IGNORECASE))

        scalar_count = 0
        for match in scalar_starts:
            start_pos = match.start()

            # Find the matching closing paren by counting
            depth = 0
            pos = start_pos
            end_pos = None
            while pos < len(content):
                if content[pos] == '(':
                    depth += 1
                elif content[pos] == ')':
                    depth -= 1
                    if depth == 0:
                        end_pos = pos
                        break
                pos += 1

            if end_pos:
                subquery = content[start_pos:end_pos + 1]
                # Check if this looks like a scalar lookup (has WHERE with correlation)
                # and is in SELECT clause (not in FROM or WHERE of outer query)
                if re.search(r'\bWHERE\b', subquery, re.IGNORECASE):
                    # Check if it's selecting a single value (aggregate or single column)
                    if re.search(r'SELECT\s+(MAX|MIN|COUNT|SUM|AVG|FIRST|LAST)\s*\(', subquery, re.IGNORECASE):
                        scalar_count += 1
                    elif re.search(r'SELECT\s+\w+\s+FROM', subquery, re.IGNORECASE):
                        # Single column select - likely scalar subquery
                        scalar_count += 1

        if scalar_count > 0:
            warnings.append(
                f"Found {scalar_count} scalar subquery lookup(s) - consider converting to LEFT JOIN for better performance"
            )

        return warnings

    def fix_bare_division(self, content: str) -> Tuple[str, List[str]]:
        """Wrap bare column/column division with SAFE_DIVIDE.

        Replaces `alias.col / alias.col` with `SAFE_DIVIDE(alias.col, alias.col)`
        to prevent division-by-zero errors in BigQuery.

        Only targets dot-qualified column references (alias.col / alias.col)
        to avoid false positives with comments, string literals, or number constants.
        Skips if already inside SAFE_DIVIDE().
        """
        warnings = []
        fix_count = 0

        lines = content.split('\n')
        new_lines = []

        for line in lines:
            stripped = line.strip()
            # Skip comments and lines already using SAFE_DIVIDE
            if stripped.startswith('--') or 'SAFE_DIVIDE' in line:
                new_lines.append(line)
                continue

            # Match: alias.col / alias.col (both dot-qualified)
            # The denominator column must start with a letter (not a number like 1.1)
            def _replace_div(m):
                nonlocal fix_count
                num = m.group(1)
                den = m.group(2)
                # Check denominator's column part starts with letter
                den_col = den.split('.')[1] if '.' in den else den
                if re.match(r'^\d', den_col):
                    return m.group(0)
                fix_count += 1
                return f"SAFE_DIVIDE({num}, {den})"

            new_line = re.sub(
                r'(\w+\.\w+)\s*/\s*(\w+\.\w+)',
                _replace_div,
                line
            )
            new_lines.append(new_line)

        if fix_count > 0:
            warnings.append(f"Wrapped {fix_count} bare division(s) with SAFE_DIVIDE")

        return '\n'.join(new_lines), warnings

    def fix_truncated_sql(self, content: str) -> Tuple[str, List[str]]:
        """Attempt to fix truncated SQL by adding missing closing elements.

        Handles:
        - Missing closing parentheses
        - Missing END statements for BEGIN blocks
        - Missing semicolons at end of statements

        Returns:
            Tuple of (fixed_content, list_of_warnings)
        """
        warnings = []

        # Find the config block end first
        config_match = re.search(r'config\s*\{', content)
        if not config_match:
            return content, warnings

        # Find end of config block
        start = config_match.end()
        brace_count = 1
        pos = start
        while pos < len(content) and brace_count > 0:
            if content[pos] == '{':
                brace_count += 1
            elif content[pos] == '}':
                brace_count -= 1
            pos += 1

        config_end = pos
        sql_part = content[config_end:]

        # Remove strings and comments for accurate counting
        sql_clean = re.sub(r'/\*.*?\*/', '', sql_part, flags=re.DOTALL)
        sql_clean = re.sub(r"'[^']*'", '', sql_clean)
        sql_clean = re.sub(r'--[^\n]*', '', sql_clean)

        # Check parentheses balance
        open_parens = sql_clean.count('(')
        close_parens = sql_clean.count(')')

        if open_parens > close_parens:
            missing = open_parens - close_parens
            # Add missing closing parentheses before the final semicolon or at end
            if content.rstrip().endswith(';'):
                content = content.rstrip()[:-1] + ')' * missing + ';'
            else:
                content = content.rstrip() + ')' * missing
            warnings.append(f"Added {missing} missing closing parenthesis(es)")

        # Check BEGIN/END balance
        begin_count = len(re.findall(r'\bBEGIN\b', sql_clean, re.IGNORECASE))
        end_count = len(re.findall(r'\bEND\b', sql_clean, re.IGNORECASE))

        if begin_count > end_count:
            missing = begin_count - end_count
            # Add missing END statements
            if not content.rstrip().endswith(';'):
                content = content.rstrip() + ';\n'
            for _ in range(missing):
                content = content.rstrip() + '\nEND;'
            warnings.append(f"Added {missing} missing END statement(s)")

        return content, warnings

    def rename_final_select_columns_to_pascal_case(self, content: str) -> Tuple[str, List[str]]:
        """Rename output columns in the final SELECT to PascalCase.

        Only affects the final SELECT (after all CTEs). Internal CTEs are unchanged.
        Handles UNION ALL by processing each SELECT...FROM block independently.
        Skips * and * EXCEPT(...) expansions.

        Returns:
            Tuple of (modified_content, list_of_warnings)
        """
        warnings = []

        # Split off the config block if present
        config_end = 0
        if content.lstrip().startswith('config'):
            # Find the closing brace of config block
            brace_depth = 0
            in_config = False
            for i, ch in enumerate(content):
                if ch == '{':
                    brace_depth += 1
                    in_config = True
                elif ch == '}':
                    brace_depth -= 1
                    if in_config and brace_depth == 0:
                        config_end = i + 1
                        break

        sql_part = content[config_end:]

        # Find the end of the WITH block (all CTEs) to locate the final SELECT
        final_select_start = self._find_final_select_start(sql_part)
        if final_select_start is None:
            return content, warnings

        before_final = sql_part[:final_select_start]
        final_sql = sql_part[final_select_start:]

        # Process each SELECT...FROM block (handles UNION ALL)
        new_final, rename_count = self._process_select_blocks_pascal(final_sql)

        if rename_count > 0:
            warnings.append(f"Renamed {rename_count} output column(s) to PascalCase")
            content = content[:config_end] + before_final + new_final

        return content, warnings

    def _find_final_select_start(self, sql: str) -> int:
        """Find the start index of the final SELECT (after all CTEs).

        Returns None if no final SELECT found.
        """
        # Look for WITH ... AS (...) pattern, then find the SELECT after all CTEs end
        upper = sql.upper()
        stripped = sql.lstrip()
        upper_stripped = stripped.upper()

        # If there's no WITH clause, the whole thing is the final SELECT
        if not upper_stripped.startswith('WITH'):
            # Find first SELECT
            m = re.search(r'\bSELECT\b', sql, re.IGNORECASE)
            return m.start() if m else None

        # Walk through CTEs tracking paren depth
        # After WITH, each CTE is: name AS ( ... ), name AS ( ... ), ... final_select
        i = 0
        length = len(sql)

        # Skip to after WITH keyword
        with_match = re.search(r'\bWITH\b', sql, re.IGNORECASE)
        if not with_match:
            return None
        i = with_match.end()

        while i < length:
            # Skip whitespace and comments
            while i < length and sql[i] in ' \t\n\r':
                i += 1
            if i >= length:
                break

            # Skip line comments
            if sql[i:i+2] == '--':
                nl = sql.find('\n', i)
                i = nl + 1 if nl != -1 else length
                continue

            # Look for AS ( to start a CTE body
            as_match = re.search(r'\bAS\s*\(', sql[i:], re.IGNORECASE)
            select_match = re.search(r'\bSELECT\b', sql[i:], re.IGNORECASE)

            if as_match is None:
                # No more CTEs — the next SELECT is the final one
                if select_match:
                    return i + select_match.start()
                return None

            if select_match and select_match.start() < as_match.start():
                # SELECT comes before AS ( — this is the final SELECT
                return i + select_match.start()

            # Found AS ( — skip the CTE body by matching parens
            paren_start = i + as_match.end() - 1  # position of the (
            depth = 1
            j = paren_start + 1
            in_single_quote = False
            in_double_quote = False
            while j < length and depth > 0:
                ch = sql[j]
                if in_single_quote:
                    if ch == "'" and (j + 1 >= length or sql[j+1] != "'"):
                        in_single_quote = False
                elif in_double_quote:
                    if ch == '"':
                        in_double_quote = False
                elif ch == "'":
                    in_single_quote = True
                elif ch == '"':
                    in_double_quote = True
                elif ch == '-' and j + 1 < length and sql[j+1] == '-':
                    # Line comment — skip to end of line
                    nl = sql.find('\n', j)
                    j = nl if nl != -1 else length
                elif ch == '(':
                    depth += 1
                elif ch == ')':
                    depth -= 1
                j += 1

            # j is now past the closing ) of this CTE
            i = j

            # Skip whitespace
            while i < length and sql[i] in ' \t\n\r':
                i += 1

            # After a CTE: either comma (more CTEs) or SELECT (final)
            if i < length and sql[i] == ',':
                i += 1  # skip comma, continue to next CTE
            # else: next token should be SELECT (final)

        return None

    def _process_select_blocks_pascal(self, sql: str) -> Tuple[str, int]:
        """Process final SELECT blocks, renaming columns to PascalCase.

        Handles UNION ALL by processing each SELECT...FROM independently.
        Returns (modified_sql, count_of_renamed_columns).
        """
        # Split by UNION ALL at depth 0
        blocks = self._split_union_all(sql)
        if not blocks:
            return sql, 0

        total_renames = 0
        result_parts = []

        for block_text, separator in blocks:
            new_block, renames = self._rename_columns_in_select_block(block_text)
            total_renames += renames
            result_parts.append(new_block)
            if separator:
                result_parts.append(separator)

        return ''.join(result_parts), total_renames

    def _split_union_all(self, sql: str) -> list:
        """Split SQL by top-level UNION ALL.

        Returns list of (block_text, separator_text) tuples.
        separator_text is the 'UNION ALL' text (with surrounding whitespace) or '' for last.
        """
        parts = []
        depth = 0
        i = 0
        block_start = 0
        length = len(sql)

        while i < length:
            ch = sql[i]
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
            elif ch == "'" and depth >= 0:
                # Skip string literal
                j = i + 1
                while j < length:
                    if sql[j] == "'" and (j + 1 >= length or sql[j+1] != "'"):
                        break
                    if sql[j] == "'" and j + 1 < length and sql[j+1] == "'":
                        j += 1  # skip escaped quote
                    j += 1
                i = j
            elif depth == 0:
                # Check for UNION ALL
                m = re.match(r'(\s+UNION\s+ALL\s+)', sql[i:], re.IGNORECASE)
                if m:
                    parts.append((sql[block_start:i], m.group(0)))
                    i += m.end()
                    block_start = i
                    continue
            i += 1

        # Last block
        parts.append((sql[block_start:], ''))
        return parts

    def _rename_columns_in_select_block(self, block: str) -> Tuple[str, int]:
        """Rename columns in a single SELECT...FROM block to PascalCase.

        Returns (modified_block, rename_count).
        """
        # Find SELECT and FROM at depth 0
        select_match = re.search(r'\bSELECT\b(\s+DISTINCT\b)?', block, re.IGNORECASE)
        if not select_match:
            return block, 0

        select_end = select_match.end()

        # Find FROM at paren depth 0
        from_pos = self._find_top_level_from(block, select_end)
        if from_pos is None:
            return block, 0

        col_text = block[select_end:from_pos]

        # Split into individual column expressions at top-level commas
        col_entries = self._split_top_level_commas(col_text)
        if not col_entries:
            return block, 0

        rename_count = 0
        new_entries = []

        for entry in col_entries:
            stripped = entry.strip()

            # Skip empty, comments, * expansions
            if not stripped or stripped.startswith('--'):
                new_entries.append(entry)
                continue
            if re.match(r'^(\w+\.)?\*', stripped):
                new_entries.append(entry)
                continue

            new_entry, renamed = self._pascal_rename_column_entry(entry)
            if renamed:
                rename_count += 1
            new_entries.append(new_entry)

        if rename_count == 0:
            return block, 0

        new_col_text = ','.join(new_entries)
        new_block = block[:select_end] + new_col_text + block[from_pos:]
        return new_block, rename_count

    def _find_top_level_from(self, sql: str, start: int) -> int:
        """Find FROM keyword at paren depth 0, starting from 'start' index."""
        depth = 0
        i = start
        length = len(sql)
        in_single_quote = False

        while i < length:
            ch = sql[i]
            if in_single_quote:
                if ch == "'" and (i + 1 >= length or sql[i+1] != "'"):
                    in_single_quote = False
                elif ch == "'" and i + 1 < length and sql[i+1] == "'":
                    i += 1  # skip escaped
            elif ch == "'":
                in_single_quote = True
            elif ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
            elif depth == 0 and ch in ('F', 'f'):
                # Check for FROM keyword
                m = re.match(r'\bFROM\b', sql[i:], re.IGNORECASE)
                if m:
                    return i
            i += 1
        return None

    def _split_top_level_commas(self, text: str) -> list:
        """Split text by commas at paren depth 0, preserving whitespace."""
        parts = []
        depth = 0
        start = 0
        i = 0
        length = len(text)
        in_single_quote = False

        while i < length:
            ch = text[i]
            if in_single_quote:
                if ch == "'" and (i + 1 >= length or text[i+1] != "'"):
                    in_single_quote = False
                elif ch == "'" and i + 1 < length and text[i+1] == "'":
                    i += 1
            elif ch == "'":
                in_single_quote = True
            elif ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
            elif ch == ',' and depth == 0:
                parts.append(text[start:i])
                start = i + 1
            i += 1

        parts.append(text[start:])
        return parts

    def _pascal_rename_column_entry(self, entry: str) -> Tuple[str, bool]:
        """Rename a single column entry's output name to PascalCase.

        Returns (new_entry, was_renamed).
        """
        stripped = entry.strip()

        # Check if it already has an AS alias
        # Match: ... AS alias_name (case-insensitive AS, alias at end)
        as_match = re.search(r'\bAS\s+(\w+)\s*$', stripped, re.IGNORECASE)

        if as_match:
            old_name = as_match.group(1)
            if '_' not in old_name:
                return entry, False
            new_name = _to_pascal_case(old_name)
            if new_name == old_name:
                return entry, False
            # Replace the alias name, preserving leading AND trailing whitespace
            leading_ws = entry[:len(entry) - len(entry.lstrip())]
            trailing_ws = entry[len(entry.rstrip()):]
            new_stripped = stripped[:as_match.start(1)] + new_name + stripped[as_match.end(1):]
            return leading_ws + new_stripped + trailing_ws, True
        else:
            # No AS alias — extract the last identifier
            # Pattern: optional_prefix.column_name at end
            ident_match = re.search(r'(?:(\w+)\.)?(\w+)\s*$', stripped)
            if not ident_match:
                return entry, False

            col_name = ident_match.group(2)
            if '_' not in col_name:
                return entry, False

            new_name = _to_pascal_case(col_name)
            if new_name == col_name:
                return entry, False

            # Add AS alias
            leading_ws = entry[:len(entry) - len(entry.lstrip())]
            trailing_ws = entry[len(entry.rstrip()):]
            return leading_ws + stripped.rstrip() + ' AS ' + new_name + trailing_ws, True

    def fix_config_column_references(self, content: str) -> Tuple[str, List[str]]:
        """Align clusterBy/partitionBy/uniqueKey values with actual SELECT aliases.

        When the translator generates clusterBy from DDL column names (e.g.,
        "AccountState_LuID") but the SELECT aliases use PascalCase
        (e.g., "AccountstateLuid"), BigQuery table creation fails because
        the cluster column doesn't exist in the output schema.

        This fix runs AFTER PascalCase renaming (step 32) so SELECT aliases
        are finalized. It updates config values to match the actual aliases.
        """
        warnings = []

        # Only process files with config blocks
        # Strip leading comments to find config keyword
        stripped = content.lstrip()
        while stripped.startswith('--'):
            nl = stripped.find('\n')
            if nl == -1:
                break
            stripped = stripped[nl + 1:].lstrip()
        if not stripped.startswith('config'):
            return content, warnings

        # Find config block by matching braces from the first '{'
        config_start = content.find('config')
        if config_start == -1:
            return content, warnings

        config_end = 0
        brace_depth = 0
        in_config = False
        for i in range(config_start, len(content)):
            ch = content[i]
            if ch == '{':
                brace_depth += 1
                in_config = True
            elif ch == '}':
                brace_depth -= 1
                if in_config and brace_depth == 0:
                    config_end = i + 1
                    break

        if config_end == 0:
            return content, warnings

        config_block = content[:config_end]
        sql_part = content[config_end:]

        # Extract SELECT aliases from the SQL body
        select_aliases = self._extract_all_select_aliases(sql_part)
        if not select_aliases:
            return content, warnings

        # Build case-insensitive lookup: lowered_name -> actual_alias
        # Include both "as-is" lowercase and "no underscore" lowercase variants
        # because PascalCase conversion removes underscores (e.g., AccountState_LuID -> AccountstateLuid)
        alias_lookup = {}
        for alias in select_aliases:
            alias_lookup[alias.lower()] = alias
            # Also index by no-underscore form for matching snake_case config values
            no_underscore = alias.lower().replace('_', '')
            if no_underscore not in alias_lookup:
                alias_lookup[no_underscore] = alias

        # Fix clusterBy values
        config_block, cluster_fixes = self._fix_config_array(
            config_block, 'clusterBy', alias_lookup
        )
        for old, new in cluster_fixes:
            warnings.append(f"Fixed clusterBy: '{old}' -> '{new}' (matching SELECT alias)")

        # Fix partitionBy value
        config_block, partition_fixes = self._fix_config_scalar(
            config_block, 'partitionBy', alias_lookup
        )
        for old, new in partition_fixes:
            warnings.append(f"Fixed partitionBy: '{old}' -> '{new}' (matching SELECT alias)")

        # Fix uniqueKey values
        config_block, key_fixes = self._fix_config_array(
            config_block, 'uniqueKey', alias_lookup
        )
        for old, new in key_fixes:
            warnings.append(f"Fixed uniqueKey: '{old}' -> '{new}' (matching SELECT alias)")

        if cluster_fixes or partition_fixes or key_fixes:
            content = config_block + sql_part

        return content, warnings

    def _extract_all_select_aliases(self, sql: str) -> set:
        """Extract all column aliases from SELECT statements in SQL.

        Returns a set of alias names found in AS clauses and bare column names.
        """
        aliases = set()

        # Find all SELECT...FROM blocks
        select_pattern = re.compile(r'\bSELECT\b\s+(.*?)\s+\bFROM\b', re.IGNORECASE | re.DOTALL)
        for match in select_pattern.finditer(sql):
            select_clause = match.group(1)
            if select_clause.strip().upper().startswith('DISTINCT'):
                select_clause = re.sub(r'^DISTINCT\s+', '', select_clause.strip(), flags=re.IGNORECASE)

            # Split by top-level commas (not inside parens)
            cols = self._split_top_level_commas(select_clause)
            for col in cols:
                col = col.strip()
                if not col or col.startswith('--'):
                    continue
                # Skip * expansions
                if re.match(r'^(\w+\.)?\*', col):
                    continue

                # Extract alias from "expr AS alias" pattern
                as_match = re.search(r'\bAS\s+`?(\w+)`?\s*$', col, re.IGNORECASE)
                if as_match:
                    aliases.add(as_match.group(1))
                else:
                    # Bare column: "table.col" or "col"
                    bare_match = re.match(r'^`?(\w+)`?$', col.strip())
                    if bare_match:
                        aliases.add(bare_match.group(1))
                    else:
                        # "table.col" pattern
                        dot_match = re.match(r'^\w+\.`?(\w+)`?$', col.strip())
                        if dot_match:
                            aliases.add(dot_match.group(1))

        return aliases

    def _fix_config_array(self, config: str, key: str, alias_lookup: dict) -> Tuple[str, List[tuple]]:
        """Fix an array config value like clusterBy: ["Col1", "Col2"].

        Returns (modified_config, list_of_(old, new)_pairs).
        """
        fixes = []
        # Match pattern: key: ["val1", "val2", ...]
        pattern = re.compile(
            rf'({key}\s*:\s*\[)([^\]]*?)(\])',
            re.IGNORECASE
        )
        match = pattern.search(config)
        if not match:
            return config, fixes

        prefix = match.group(1)
        values_str = match.group(2)
        suffix = match.group(3)

        # Parse individual quoted values
        value_pattern = re.compile(r'"([^"]*)"')
        values = value_pattern.findall(values_str)

        new_values = []
        for val in values:
            val_lower = val.lower()
            val_no_underscore = val_lower.replace('_', '')
            # Try exact lowercase first, then no-underscore variant
            if val_lower in alias_lookup and alias_lookup[val_lower] != val:
                new_val = alias_lookup[val_lower]
                fixes.append((val, new_val))
                new_values.append(new_val)
            elif val_no_underscore in alias_lookup and alias_lookup[val_no_underscore] != val:
                new_val = alias_lookup[val_no_underscore]
                fixes.append((val, new_val))
                new_values.append(new_val)
            else:
                new_values.append(val)

        if fixes:
            new_values_str = ', '.join(f'"{v}"' for v in new_values)
            new_match = prefix + new_values_str + suffix
            config = config[:match.start()] + new_match + config[match.end():]

        return config, fixes

    def _fix_config_scalar(self, config: str, key: str, alias_lookup: dict) -> Tuple[str, List[tuple]]:
        """Fix a scalar config value like partitionBy: "ColName".

        Returns (modified_config, list_of_(old, new)_pairs).
        """
        fixes = []
        pattern = re.compile(
            rf'({key}\s*:\s*)"([^"]*)"',
            re.IGNORECASE
        )
        match = pattern.search(config)
        if not match:
            return config, fixes

        val = match.group(2)
        val_lower = val.lower()
        val_no_underscore = val_lower.replace('_', '')
        # Try exact lowercase first, then no-underscore variant
        matched_val = None
        if val_lower in alias_lookup and alias_lookup[val_lower] != val:
            matched_val = alias_lookup[val_lower]
        elif val_no_underscore in alias_lookup and alias_lookup[val_no_underscore] != val:
            matched_val = alias_lookup[val_no_underscore]

        if matched_val:
            fixes.append((val, matched_val))
            replacement = match.group(1) + f'"{matched_val}"'
            config = config[:match.start()] + replacement + config[match.end():]

        return config, fixes


# Singleton instance for convenience
_default_processor = None


def get_post_processor() -> SQLXPostProcessor:
    """Get the default post-processor instance."""
    global _default_processor
    if _default_processor is None:
        _default_processor = SQLXPostProcessor()
    return _default_processor


def post_process_sqlx(content: str, source_name: str = "unknown") -> str:
    """Convenience function to post-process SQLX content.

    Args:
        content: The SQLX content to process
        source_name: Name of the source for logging

    Returns:
        Processed content
    """
    processor = get_post_processor()
    processed_content, _ = processor.process(content, source_name)
    return processed_content


def generate_missing_external_stubs(output_dir: str, sources_subdir: str = "missing", cleanup: bool = True) -> List[str]:
    """Scan output directory for missing refs and create stub declarations.

    This function should be called after all conversions are complete to
    create placeholder declarations for external tables that are referenced
    but don't exist in the output.

    Also cleans up unused declarations in the missing folder.

    Args:
        output_dir: Path to the output definitions directory
        sources_subdir: Subdirectory to create stubs in (default: "missing")
        cleanup: If True, remove unused declarations (default: True)

    Returns:
        List of created stub file paths
    """
    from pathlib import Path

    output_path = Path(output_dir)
    sources_path = output_path / sources_subdir

    # Folders to skip when scanning for existing files and refs
    skip_folders = {'sources', 'missing'}

    # Ensure target directory exists
    sources_path.mkdir(parents=True, exist_ok=True)

    # Build index of all existing files (by stem name), excluding sources and missing folders
    existing_files = {}
    for f in output_path.rglob('*.sqlx'):
        if not skip_folders.intersection(f.parts):
            existing_files[f.stem.lower()] = f

    # Extract all refs from all files (excluding sources and missing folders)
    all_refs = set()
    all_refs_lower = set()
    ref_pattern = re.compile(r"\$\{ref\(['\"]([^'\"]+)['\"]\)\}")

    for sqlx_file in output_path.rglob('*.sqlx'):
        if skip_folders.intersection(sqlx_file.parts):
            continue
        content = sqlx_file.read_text(encoding='utf-8')
        refs = ref_pattern.findall(content)
        all_refs.update(refs)
        all_refs_lower.update(r.lower() for r in refs)

    # Also index files in sources/ and missing/ so we don't create duplicates
    for folder in skip_folders:
        folder_path = output_path / folder
        if folder_path.exists():
            for f in folder_path.glob('*.sqlx'):
                existing_files[f.stem.lower()] = f

    # Find missing refs (referenced but no file exists)
    missing_refs = set()
    for ref_name in all_refs:
        ref_lower = ref_name.lower()
        if ref_lower not in existing_files:
            missing_refs.add(ref_name)

    # Create stub declarations for missing refs
    created_files = []
    stub_template = '''config {{
  type: "declaration",
  database: dataform.projectConfig.defaultDatabase,
  schema: "external",
  name: "{name}",
  description: "MISSING: No source found for '{name}' - add to CDW Files in DDAS.csv"
}}
'''

    for ref_name in sorted(missing_refs):
        stub_file = sources_path / f"{ref_name}.sqlx"

        # Skip if stub already exists
        if stub_file.exists():
            continue

        stub_content = stub_template.format(name=ref_name)
        stub_file.write_text(stub_content, encoding='utf-8')
        created_files.append(str(stub_file))
        logger.info(f"Created stub declaration in {sources_subdir}/: {ref_name}")

    if created_files:
        logger.info(f"Created {len(created_files)} stub declarations in {sources_subdir}/")
    else:
        logger.info("No missing external references found - no stubs needed")

    # Cleanup unused declarations
    if cleanup and sources_path.exists():
        source_files = list(sources_path.glob('*.sqlx'))
        removed_count = 0
        for source_file in source_files:
            source_name = source_file.stem.lower()
            if source_name not in all_refs_lower:
                source_file.unlink()
                removed_count += 1
                logger.info(f"Removed unused declaration: {source_file.name}")
        if removed_count > 0:
            logger.info(f"Cleaned up {removed_count} unused declarations")

    # Fix existing declarations that are missing the 'name' property
    if sources_path.exists():
        fixed_count = 0
        for source_file in sources_path.glob('*.sqlx'):
            content = source_file.read_text(encoding='utf-8')
            # Check if it's a declaration missing name property
            if 'type: "declaration"' in content and 'name:' not in content:
                name = source_file.stem
                # Add name property after type line
                new_content = content.replace(
                    'type: "declaration",',
                    f'type: "declaration",\n  name: "{name}",'
                )
                if new_content != content:
                    source_file.write_text(new_content, encoding='utf-8')
                    fixed_count += 1
                    logger.info(f"Added missing 'name' property to declaration: {source_file.name}")
        if fixed_count > 0:
            logger.info(f"Fixed {fixed_count} declarations with missing 'name' property")

    return created_files
