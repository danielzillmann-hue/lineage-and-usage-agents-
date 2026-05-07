"""
Centralized SQLX Validation Module

Validates and repairs SQLX files before they're written to disk.
This catches issues at generation time rather than during Dataform compile.

Two validator classes:
1. SQLXValidator - Original structural validation (config blocks, ref syntax, bad patterns)
2. SQLXRepairValidator - New repair layer that fixes hardcoded refs, duplicate aliases,
   stray braces, and other LLM-generated defects

Used by: informatica_converter.py, sp_converter.py, translator.py, dataform_fixer.py
"""

import re
import logging
from typing import Tuple, List, Dict, Optional
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Result of SQLX validation."""
    is_valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    auto_fixed: bool = False
    fixed_content: Optional[str] = None

    def add_error(self, msg: str):
        self.errors.append(msg)
        self.is_valid = False

    def add_warning(self, msg: str):
        self.warnings.append(msg)


class SQLXValidator:
    """
    Validates and optionally fixes SQLX content before writing to disk.

    This is the single source of truth for SQLX validation rules.
    All converters should use this before writing files.
    """

    # Patterns that indicate LLM confusion/hallucination
    BAD_PATTERNS = [
        (r'\$\d+(?!\d)', "Regex capture group placeholder ($1, $2, etc.)"),
        (r'\{\{error\.group', "Template placeholder {{error.group}}"),
        (r'\{\{match\.group', "Template placeholder {{match.group}}"),
        (r'INSERT_TABLE_NAME_HERE', "Placeholder INSERT_TABLE_NAME_HERE"),
        (r'UNKNOWN_TABLE', "Placeholder UNKNOWN_TABLE"),
        (r'unknown_table', "Placeholder unknown_table"),
        (r'\\\$\{ref', "Escaped ref pattern \\${ref}"),
        (r'\$\\\{ref', "Escaped ref pattern $\\{ref}"),
        (r'\$\{ref\([\'"]?\$\d+', "Ref with regex placeholder ${ref('$1')}"),
    ]

    # Patterns that can be auto-fixed
    FIXABLE_PATTERNS = [
        # Escaped refs: \${ref(...)} -> ${ref(...)}
        (r'\\\$\{ref\(', '${ref(', "Unescape ref pattern"),
        # Double braces: ${{...}} -> ${...}
        (r'\$\{\{', '${', "Fix double opening braces"),
        (r'\}\}(?!\})', '}', "Fix double closing braces"),
        # Type operation -> operations
        (r'type:\s*["\']operation["\']', 'type: "operations"', "Fix type: operation -> operations"),
    ]

    def __init__(self, auto_fix: bool = True):
        """
        Initialize validator.

        Args:
            auto_fix: If True, attempt to fix issues automatically
        """
        self.auto_fix = auto_fix

    def validate(self, content: str, filename: str = None) -> ValidationResult:
        """
        Validate SQLX content.

        Args:
            content: The SQLX content to validate
            filename: Optional filename for better error messages

        Returns:
            ValidationResult with errors, warnings, and optionally fixed content
        """
        result = ValidationResult(is_valid=True)
        working_content = content

        if not content or len(content.strip()) < 10:
            result.add_error("Empty or too short content")
            return result

        # Check for bad patterns (strip SQL strings/comments to avoid
        # false positives from literal dollar amounts like '$1' or
        # commented-out code like --CBET$7.50FB)
        stripped_for_check = re.sub(r"'[^']*'", '', working_content)
        stripped_for_check = re.sub(r'--[^\n]*', '', stripped_for_check)
        for pattern, description in self.BAD_PATTERNS:
            if re.search(pattern, stripped_for_check):
                result.add_error(f"Contains {description}")

        # Check config block structure
        config_errors = self._validate_config_block(working_content)
        for err in config_errors:
            result.add_error(err)

        # Check ref syntax
        ref_errors = self._validate_refs(working_content)
        for err in ref_errors:
            result.add_error(err)

        # Check balanced parentheses
        paren_error = self._check_balanced_parens(working_content)
        if paren_error:
            result.add_warning(paren_error)  # Warning, not error - might be intentional

        # Attempt auto-fix if enabled and there are errors
        if self.auto_fix and not result.is_valid:
            fixed_content = self._attempt_auto_fix(working_content)
            if fixed_content != working_content:
                # Re-validate the fixed content
                revalidate = self.validate(fixed_content, filename)
                if revalidate.is_valid or len(revalidate.errors) < len(result.errors):
                    result.auto_fixed = True
                    result.fixed_content = fixed_content
                    result.is_valid = revalidate.is_valid
                    result.errors = revalidate.errors
                    result.warnings = revalidate.warnings

        return result

    def _validate_config_block(self, content: str) -> List[str]:
        """Validate config block structure."""
        errors = []

        # Check for config block presence
        config_match = re.search(r'config\s*\{', content)
        if not config_match:
            # Not all SQLX files need config blocks (e.g., includes)
            return errors

        # Check for balanced braces in config block
        start = config_match.start()
        brace_count = 0
        in_config = False
        config_end = -1

        for i, char in enumerate(content[start:], start):
            if char == '{':
                brace_count += 1
                in_config = True
            elif char == '}':
                brace_count -= 1
                if brace_count == 0 and in_config:
                    config_end = i
                    break

        if config_end == -1:
            errors.append("Config block has unbalanced braces")
            return errors

        config_content = content[start:config_end + 1]

        # Check for required fields
        if 'type:' not in config_content:
            errors.append("Config block missing 'type' field")

        # Check for dots in name field
        name_match = re.search(r'name:\s*"([^"]+)"', config_content)
        if name_match and '.' in name_match.group(1):
            errors.append(f"Config name contains dot: {name_match.group(1)}")

        # Check for multiple config blocks
        config_count = len(re.findall(r'config\s*\{', content))
        if config_count > 1:
            errors.append(f"Multiple config blocks found ({config_count})")

        return errors

    def _validate_refs(self, content: str) -> List[str]:
        """Validate ${ref()} syntax."""
        errors = []

        # Find all ref patterns
        ref_pattern = r'\$\{ref\([^)]*\)\}'
        refs = re.findall(ref_pattern, content)

        # Check for malformed refs
        if '${ref(' in content:
            # Count opening and closing
            open_count = content.count('${ref(')
            close_count = len(refs)
            if open_count != close_count:
                errors.append(f"Malformed ref syntax: {open_count} openings, {close_count} complete refs")

        # Check each ref for issues
        for ref in refs:
            # Check for empty ref
            if re.match(r'\$\{ref\(\s*\)\}', ref):
                errors.append("Empty ref: ${ref()}")

            # Check for ref with regex placeholder
            if re.search(r'\$\d+', ref):
                errors.append(f"Ref contains regex placeholder: {ref}")

        return errors

    def _check_balanced_parens(self, content: str) -> Optional[str]:
        """Check for balanced parentheses."""
        # Skip config block for this check
        config_match = re.search(r'config\s*\{', content)
        if config_match:
            # Find end of config block
            start = config_match.start()
            brace_count = 0
            config_end = start
            for i, char in enumerate(content[start:], start):
                if char == '{':
                    brace_count += 1
                elif char == '}':
                    brace_count -= 1
                    if brace_count == 0:
                        config_end = i + 1
                        break
            sql_content = content[config_end:]
        else:
            sql_content = content

        open_parens = sql_content.count('(')
        close_parens = sql_content.count(')')

        if open_parens != close_parens:
            return f"Unbalanced parentheses: {open_parens} opening, {close_parens} closing"

        return None

    def _attempt_auto_fix(self, content: str) -> str:
        """Attempt to auto-fix common issues."""
        fixed = content

        # Apply fixable patterns
        for pattern, replacement, description in self.FIXABLE_PATTERNS:
            new_fixed = re.sub(pattern, replacement, fixed, flags=re.IGNORECASE)
            if new_fixed != fixed:
                logger.debug(f"Auto-fixed: {description}")
                fixed = new_fixed

        # Fix refs with regex placeholders
        fixed = re.sub(
            r'\$\{ref\([\'"]?\$\d+[\'"]?,?\s*[\'"]?\$?\d*[\'"]?\)\}',
            "${ref('unknown_source')}",
            fixed
        )

        # Fix dots in name field
        def fix_name_dots(match):
            name = match.group(1).replace('.', '_').lower()
            return f'name: "{name}"'
        fixed = re.sub(r'name:\s*"([^"]+)"', fix_name_dots, fixed)

        # Remove malformed config lines with regex placeholders
        fixed = re.sub(r'\$\d+\s+name:\s*"[^"]*"[,]?\s*\n?', '', fixed)
        fixed = re.sub(r'\{\{(?:error|match)\.group\[\d+\]\}\}', '', fixed)

        return fixed


# ==============================================================================
# SQLXRepairValidator - New repair layer for hardcoded refs, aliases, braces
# ==============================================================================

class SQLXRepairValidator:
    """Deterministic repair layer that fixes known LLM-generated defect patterns.

    Runs after post-processing but before file write. Fixes:
    1. Hardcoded BigQuery table refs -> ${ref()} conversion
    2. Duplicate table alias detection and renaming in JOINs
    3. Stray brace/bracket detection and removal
    4. Balanced parentheses validation and repair
    5. Incomplete statement detection
    """

    def __init__(self, ddas_mappings: Optional[Dict] = None):
        """Initialize repair validator.

        Args:
            ddas_mappings: Dict from sybase.yaml ddas_mappings section.
        """
        self.ddas_mappings = ddas_mappings or {}
        self._build_ref_lookup()

    def _build_ref_lookup(self):
        """Build lookup from hardcoded BigQuery table refs to ${ref()} names."""
        self.ref_lookup = {}
        database = self.ddas_mappings.get('database', '')

        for schema, tables in self.ddas_mappings.items():
            if schema == 'database':
                continue
            if isinstance(tables, list):
                for table in tables:
                    full_ref = f"{database}.{schema}.{table}".lower()
                    ref_name = table.lower()
                    self.ref_lookup[full_ref] = f"${{ref('{ref_name}')}}"
            elif isinstance(tables, dict):
                for lookup_key, bq_name in tables.items():
                    full_ref = f"{database}.{schema}.{bq_name}".lower()
                    ref_name = f"s_{lookup_key.lower()}"
                    self.ref_lookup[full_ref] = f"${{ref('{ref_name}')}}"

    def validate_and_repair(self, content: str, source_name: str = "unknown") -> Tuple[str, List[str]]:
        """Run all validation and repair steps.

        Args:
            content: SQLX file content
            source_name: Name for logging

        Returns:
            Tuple of (repaired_content, list_of_warnings)
        """
        warnings = []

        # 1. Convert hardcoded BigQuery table references to ${ref()}
        content, w = self.fix_hardcoded_refs(content)
        warnings.extend(w)

        # 2. Detect and fix duplicate table aliases in JOINs
        content, w = self.fix_duplicate_aliases(content)
        warnings.extend(w)

        # 3. Fix stray braces/brackets
        content, w = self.fix_stray_braces(content)
        warnings.extend(w)

        # 4. Validate balanced parentheses in SQL
        content, w = self.fix_unbalanced_sql_parens(content)
        warnings.extend(w)

        # 5. Validate statement completeness
        content, w = self.fix_incomplete_statements(content)
        warnings.extend(w)

        if warnings:
            logger.info(f"SQLX Repair [{source_name}]: {len(warnings)} fixes applied")
            for ww in warnings:
                logger.debug(f"  - {ww}")

        return content, warnings

    def _split_config_and_sql(self, content: str) -> Tuple[str, str]:
        """Split content into config/pre_operations block and SQL portion."""
        # Find end of config block
        config_match = re.search(r'config\s*\{', content)
        if not config_match:
            return "", content

        depth = 0
        config_end = config_match.start()
        for i in range(config_match.start(), len(content)):
            if content[i] == '{':
                depth += 1
            elif content[i] == '}':
                depth -= 1
                if depth == 0:
                    config_end = i + 1
                    while config_end < len(content) and content[config_end] in ('\n', '\r'):
                        config_end += 1
                    break

        # Also skip pre_operations block if present
        pre_ops_match = re.search(r'pre_operations\s*\{', content[config_end:])
        if pre_ops_match:
            depth = 0
            pre_start = config_end + pre_ops_match.start()
            for i in range(pre_start, len(content)):
                if content[i] == '{':
                    depth += 1
                elif content[i] == '}':
                    depth -= 1
                    if depth == 0:
                        config_end = i + 1
                        while config_end < len(content) and content[config_end] in ('\n', '\r'):
                            config_end += 1
                        break

        return content[:config_end], content[config_end:]

    def fix_hardcoded_refs(self, content: str) -> Tuple[str, List[str]]:
        """Convert hardcoded backtick BigQuery table references to ${ref()}.

        Converts patterns like:
            `crown-ddas-dev.CDWH_Access.D_Day`  ->  ${ref('d_day')}
            `crown-cdw-dev.cdw.S_DACOM_Device`  ->  ${ref('s_dacom_device')}
        """
        warnings = []
        config_block, sql_part = self._split_config_and_sql(content)

        # Pattern: `project-id.dataset.table` with backticks
        pattern = r'`([a-zA-Z0-9_-]+\.[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+)`'

        def replace_ref(match):
            full_ref = match.group(1)
            full_ref_lower = full_ref.lower()

            # Check our DDAS lookup first
            if full_ref_lower in self.ref_lookup:
                ref_replacement = self.ref_lookup[full_ref_lower]
                warnings.append(f"Converted hardcoded ref `{full_ref}` -> {ref_replacement}")
                return ref_replacement

            # Infer from table name — extract last part
            parts = full_ref.split('.')
            if len(parts) == 3:
                table_name = parts[2].lower()
                ref_str = f"${{ref('{table_name}')}}"
                warnings.append(f"Converted hardcoded ref `{full_ref}` -> {ref_str} (inferred)")
                return ref_str

            return match.group(0)

        # Only fix refs in SQL part, skip comments
        lines = sql_part.split('\n')
        fixed_lines = []
        for line in lines:
            stripped = line.strip()
            if stripped.startswith('--') or stripped.startswith('//'):
                fixed_lines.append(line)
            else:
                fixed_lines.append(re.sub(pattern, replace_ref, line))

        return config_block + '\n'.join(fixed_lines), warnings

    def fix_duplicate_aliases(self, content: str) -> Tuple[str, List[str]]:
        """Detect and rename duplicate table aliases in JOIN clauses.

        Catches:
            FROM table1 AS DS
            LEFT JOIN table2 AS DS    -- duplicate!
            LEFT JOIN table3 AS DS    -- duplicate!

        Renames to DS, DS2, DS3 and updates references in ON/WHERE/SELECT.

        CTE-aware: aliases in different CTE scopes are NOT considered duplicates.
        Each CTE (parenthesized block after `name AS (`) has its own alias scope.
        """
        warnings = []
        config_block, sql_part = self._split_config_and_sql(content)

        # Find all alias definitions in FROM/JOIN clauses
        alias_pattern = (
            r'(?:FROM|(?:LEFT|RIGHT|INNER|OUTER|CROSS|FULL)\s+JOIN|JOIN)\s+'
            r'(?:\$\{ref\([^\)]+\)\}|`[^`]+`|\S+)\s+'
            r'(?:AS\s+)?([A-Za-z]\w*)'
        )

        matches = list(re.finditer(alias_pattern, sql_part, re.IGNORECASE))
        if not matches:
            return content, warnings

        # SQL keywords to skip
        sql_keywords = {
            'ON', 'WHERE', 'SET', 'AND', 'OR', 'NOT', 'AS', 'SELECT', 'FROM',
            'JOIN', 'LEFT', 'RIGHT', 'INNER', 'OUTER', 'CROSS', 'FULL',
            'GROUP', 'ORDER', 'HAVING', 'LIMIT', 'UNION', 'EXCEPT',
            'INTERSECT', 'INTO', 'VALUES', 'INSERT', 'UPDATE', 'DELETE',
            'WHEN', 'THEN', 'ELSE', 'END', 'CASE', 'BETWEEN', 'IN', 'LIKE',
            'EXISTS', 'ALL', 'ANY', 'SOME', 'WITH', 'RECURSIVE', 'BY'
        }

        # Build a parenthesis-depth map so we can determine CTE scope for each position.
        # Each CTE body is enclosed in parens: `cte_name AS ( ... )`.
        # Aliases at different paren depths (or different CTE blocks at the same depth)
        # are in separate scopes and must NOT be treated as duplicates.
        # We assign a "scope ID" to each position: (depth, scope_counter_at_that_depth).
        scope_at_pos = {}  # position -> scope_id
        depth = 0
        scope_counter = [0] * 100  # counter per depth level
        for i, ch in enumerate(sql_part):
            if ch == '(':
                depth += 1
                scope_counter[depth] += 1
            elif ch == ')':
                depth -= 1
            scope_at_pos[i] = (depth, scope_counter[depth] if depth < 100 else 0)

        def get_scope(pos):
            """Get the CTE scope ID for a position in sql_part."""
            return scope_at_pos.get(pos, (0, 0))

        # Count alias occurrences — grouped by (alias, scope)
        alias_positions = {}  # (alias_upper, scope_id) -> list of positions
        for m in matches:
            alias = m.group(1)
            if alias.upper() in sql_keywords:
                continue
            alias_upper = alias.upper()
            scope_id = get_scope(m.start(1))
            key = (alias_upper, scope_id)
            if key not in alias_positions:
                alias_positions[key] = []
            alias_positions[key].append((m.start(1), m.end(1), alias))

        # Fix duplicates — rename 2nd, 3rd, etc. occurrences
        # Only aliases that appear multiple times within the SAME scope are duplicates
        renames = {}  # old_alias -> new_alias (for non-first occurrences)
        for (alias_upper, scope_id), positions in alias_positions.items():
            if len(positions) <= 1:
                continue

            original_alias = positions[0][2]
            warnings.append(f"Fixed {len(positions)} duplicate '{original_alias}' aliases")

            for i, (start, end, alias_text) in enumerate(positions[1:], start=2):
                new_alias = f"{alias_text}{i}"
                renames[(start, end)] = (alias_text, new_alias)

        if not renames:
            return content, warnings

        # Apply alias renames in reverse order to preserve positions
        sorted_renames = sorted(renames.items(), key=lambda x: x[0][0], reverse=True)
        for (start, end), (old_alias, new_alias) in sorted_renames:
            # Replace the alias definition
            sql_part = sql_part[:start] + new_alias + sql_part[end:]

            # Find the scope where this alias is used (its ON clause + subsequent clauses)
            adjusted_end = start + len(new_alias)

            # Find the next JOIN or end-of-query clause
            next_clause = re.search(
                r'\b(?:(?:LEFT|RIGHT|INNER|OUTER|CROSS|FULL)\s+)?JOIN\b|\bWHERE\b|\bGROUP\b|\bORDER\b|\bHAVING\b|\bLIMIT\b|\bUNION\b',
                sql_part[adjusted_end:], re.IGNORECASE
            )
            scope_end = adjusted_end + next_clause.start() if next_clause else len(sql_part)
            scope = sql_part[adjusted_end:scope_end]

            # Replace alias.column references within scope
            fixed_scope = re.sub(
                rf'\b{re.escape(old_alias)}\b\.',
                f'{new_alias}.',
                scope
            )
            sql_part = sql_part[:adjusted_end] + fixed_scope + sql_part[scope_end:]

        # Second pass: update alias references in WHERE/SELECT/GROUP BY/ORDER BY
        # that are beyond the JOIN clause scope
        for (_, _), (old_alias, new_alias) in sorted_renames:
            # Find all remaining references to the old alias that should be the new one
            # This handles cases where the LLM referenced the duplicate alias in WHERE etc.
            # We only update references that come AFTER all JOIN definitions
            last_join_pos = 0
            for m in re.finditer(r'\b(?:(?:LEFT|RIGHT|INNER|OUTER|CROSS|FULL)\s+)?JOIN\b', sql_part, re.IGNORECASE):
                last_join_pos = max(last_join_pos, m.end())

            if last_join_pos > 0:
                # Find where the last ON clause ends
                rest_match = re.search(r'\b(?:WHERE|GROUP|ORDER|HAVING|LIMIT|UNION)\b', sql_part[last_join_pos:], re.IGNORECASE)
                if rest_match:
                    rest_start = last_join_pos + rest_match.start()
                    rest = sql_part[rest_start:]
                    # Check if old_alias is used but not defined in the final alias set
                    if re.search(rf'\b{re.escape(old_alias)}\b\.', rest):
                        # Only replace if the old_alias is the one we renamed (not the first occurrence)
                        fixed_rest = re.sub(
                            rf'\b{re.escape(old_alias)}\b\.',
                            f'{new_alias}.',
                            rest
                        )
                        sql_part = sql_part[:rest_start] + fixed_rest

        return config_block + sql_part, warnings

    def fix_stray_braces(self, content: str) -> Tuple[str, List[str]]:
        """Fix stray braces that aren't part of ${ref()} or config blocks.

        Catches:
            SiteID}  ->  SiteID
            column{  ->  column
        """
        warnings = []
        config_block, sql_part = self._split_config_and_sql(content)

        # Walk through SQL character by character, tracking ${...} blocks
        result = []
        i = 0
        while i < len(sql_part):
            # Check for ${...} pattern — preserve these
            if sql_part[i:i+2] == '${':
                depth = 0
                j = i + 2
                while j < len(sql_part):
                    if sql_part[j] == '{':
                        depth += 1
                    elif sql_part[j] == '}':
                        if depth == 0:
                            result.append(sql_part[i:j+1])
                            i = j + 1
                            break
                        depth -= 1
                    j += 1
                else:
                    result.append(sql_part[i])
                    i += 1
                continue

            # Stray } after word character (not part of ${...})
            if sql_part[i] == '}' and i > 0 and re.match(r'\w', sql_part[i-1]):
                preceding = sql_part[max(0, i-100):i]
                dollar_brace = preceding.rfind('${')
                close_brace = preceding.rfind('}')
                if dollar_brace == -1 or close_brace > dollar_brace:
                    word_match = re.search(r'(\w+)$', sql_part[:i])
                    word = word_match.group(1) if word_match else '?'
                    warnings.append(f"Removed stray '}}' after '{word}'")
                    i += 1
                    continue

            # Stray { after word (not part of ${)
            if sql_part[i] == '{' and i > 0 and re.match(r'\w', sql_part[i-1]):
                if i < 1 or sql_part[i-1] != '$':
                    word_match = re.search(r'(\w+)$', sql_part[:i])
                    word = word_match.group(1) if word_match else '?'
                    warnings.append(f"Removed stray '{{' after '{word}'")
                    i += 1
                    continue

            result.append(sql_part[i])
            i += 1

        return config_block + ''.join(result), warnings

    def fix_unbalanced_sql_parens(self, content: str) -> Tuple[str, List[str]]:
        """Check for unbalanced parentheses in SQL portion and attempt repair."""
        warnings = []
        config_block, sql_part = self._split_config_and_sql(content)

        # Count parens (ignoring strings, line comments, and block comments).
        # Block comments /* ... */ may contain unbalanced parens in TODO
        # placeholders (e.g., /* TODO: DECODE(TRUE, ...) */) — these must
        # be skipped to avoid false positive "unbalanced" detection.
        open_count = 0
        close_count = 0
        in_string = False
        string_char = None
        in_line_comment = False
        in_block_comment = False
        i = 0
        n = len(sql_part)

        while i < n:
            ch = sql_part[i]
            if in_block_comment:
                if ch == '*' and i + 1 < n and sql_part[i + 1] == '/':
                    in_block_comment = False
                    i += 2
                    continue
                i += 1
                continue
            if in_line_comment:
                if ch == '\n':
                    in_line_comment = False
                i += 1
                continue
            if in_string:
                if ch == string_char and (i + 1 >= n or sql_part[i+1] != string_char):
                    in_string = False
                i += 1
                continue
            if ch == '-' and i + 1 < n and sql_part[i+1] == '-':
                in_line_comment = True
                i += 2
                continue
            if ch == '/' and i + 1 < n and sql_part[i+1] == '*':
                in_block_comment = True
                i += 2
                continue
            if ch in ("'", '"'):
                in_string = True
                string_char = ch
                i += 1
                continue
            if ch == '(':
                open_count += 1
            elif ch == ')':
                close_count += 1
            i += 1

        if open_count > close_count:
            diff = open_count - close_count
            sql_part = sql_part.rstrip() + ')' * diff + '\n'
            warnings.append(f"Added {diff} missing closing parenthesis(es)")
        elif close_count > open_count:
            diff = close_count - open_count
            stripped = sql_part.rstrip()
            removed = 0
            while removed < diff and stripped.endswith(')'):
                stripped = stripped[:-1]
                removed += 1
            if removed > 0:
                sql_part = stripped + '\n'
                warnings.append(f"Removed {removed} extra closing parenthesis(es)")

        return config_block + sql_part, warnings

    def fix_incomplete_statements(self, content: str) -> Tuple[str, List[str]]:
        """Check for incomplete SQL statements and add warnings."""
        warnings = []
        _, sql_part = self._split_config_and_sql(content)
        sql_upper = sql_part.upper()

        has_select = 'SELECT' in sql_upper
        has_from = 'FROM' in sql_upper

        if has_select and not has_from and 'DECLARATION' not in content.upper():
            warnings.append("SQL has SELECT but no FROM clause - may be incomplete")

        # Check for truncated SQL
        lines = sql_part.strip().split('\n')
        if lines:
            last_line = lines[-1].strip()
            if last_line.endswith(',') or last_line.upper().endswith(' AND') or last_line.upper().endswith(' OR'):
                warnings.append(f"SQL appears truncated - last line ends with '{last_line[-5:]}'")

            # Check for unclosed CASE
            case_count = len(re.findall(r'\bCASE\b', sql_part, re.IGNORECASE))
            end_count = len(re.findall(r'\bEND\b', sql_part, re.IGNORECASE))
            if case_count > end_count:
                warnings.append(f"Unclosed CASE statement(s): {case_count} CASE vs {end_count} END")

        return content, warnings


# ==============================================================================
# Convenience functions
# ==============================================================================

def validate_sqlx(content: str, filename: str = None, auto_fix: bool = True) -> ValidationResult:
    """
    Convenience function to validate SQLX content.

    Args:
        content: The SQLX content to validate
        filename: Optional filename for better error messages
        auto_fix: If True, attempt to fix issues automatically

    Returns:
        ValidationResult
    """
    validator = SQLXValidator(auto_fix=auto_fix)
    return validator.validate(content, filename)


def validate_and_fix(content: str, filename: str = None) -> Tuple[str, List[str]]:
    """
    Validate and fix SQLX content, returning the best possible content.

    Args:
        content: The SQLX content to validate
        filename: Optional filename for better error messages

    Returns:
        Tuple of (fixed_content, remaining_errors)
    """
    result = validate_sqlx(content, filename, auto_fix=True)

    if result.auto_fixed and result.fixed_content:
        return result.fixed_content, result.errors

    return content, result.errors


def validate_and_repair(content: str, source_name: str = "unknown",
                        ddas_mappings: Optional[Dict] = None) -> Tuple[str, List[str]]:
    """Run both structural validation and repair layer.

    This is the main entry point for the full validation pipeline.
    Called from informatica_converter._create_transformation_sqlx() before file write.

    Args:
        content: SQLX file content
        source_name: Name for logging
        ddas_mappings: DDAS mappings dict from sybase.yaml

    Returns:
        Tuple of (repaired_content, list_of_all_warnings)
    """
    all_warnings = []

    # Step 1: Structural validation and auto-fix
    result = validate_sqlx(content, source_name, auto_fix=True)
    if result.auto_fixed and result.fixed_content:
        content = result.fixed_content
        all_warnings.extend([f"[structural] {e}" for e in result.warnings])
    all_warnings.extend([f"[structural] {e}" for e in result.errors])

    # Step 2: Repair layer (hardcoded refs, aliases, braces)
    repair = SQLXRepairValidator(ddas_mappings=ddas_mappings)
    content, repair_warnings = repair.validate_and_repair(content, source_name)
    all_warnings.extend([f"[repair] {w}" for w in repair_warnings])

    return content, all_warnings


def create_repair_validator(ddas_mappings: Optional[Dict] = None) -> SQLXRepairValidator:
    """Create a repair validator instance.

    Args:
        ddas_mappings: DDAS mappings dict (from adapter.get_ddas_mappings()).
                      If None, creates validator without hardcoded ref conversion.

    Returns:
        Configured SQLXRepairValidator instance
    """
    return SQLXRepairValidator(ddas_mappings=ddas_mappings)
