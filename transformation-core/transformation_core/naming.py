"""
Centralized Naming Conventions Module

Enforces consistent naming across all generated SQLX files.
This is the single source of truth for naming rules.

Naming convention:
  - Prefixes (d_, f_, w_, s_, vw_, stg_, cdw_) kept with underscore
  - Everything after the prefix is PascalCase with no underscores
  - Examples: d_Site, f_JackpotHit, w_CarparkCard, vw_SDacomClassCodeG

Used by: informatica_converter.py, sp_converter.py, translator.py, dataform_fixer.py
"""

import re
import logging
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

# Acronyms that should stay uppercase in PascalCase conversion
KNOWN_ACRONYMS = {'ID', 'ATS', 'EGM', 'IGT', 'ETL', 'SCD', 'BU', 'DT', 'FK', 'PK', 'BQ', 'UDS', 'CDW'}

# Recognized prefixes that keep their underscore separator
# Order matters: longer prefixes first to avoid partial matches (e.g., stg_ before s_)
_KNOWN_PREFIXES = ('vw_', 'stg_', 'cdw_', 's_', 'd_', 'f_', 'w_')


def _pascal_word(word: str) -> str:
    """Capitalize a single word, preserving known acronyms."""
    if not word:
        return word
    if word.upper() in KNOWN_ACRONYMS:
        return word.upper()
    return word[0].upper() + word[1:].lower() if len(word) > 1 else word.upper()


def to_pascal_name(name: str) -> str:
    """Convert a snake_case name to prefix_PascalCase format.

    Keeps the first recognized prefix (s_, vw_, stg_, cdw_) with its
    underscore, then PascalCases the remainder with no underscores.

    Examples:
        d_site              -> d_Site
        f_jackpot_hit       -> f_JackpotHit
        w_carpark_card      -> w_CarparkCard
        vw_s_dacom_class_code_g -> vw_SDacomClassCodeG
        s_dacom_bank        -> s_DacomBank
        stg_sybaseadmin_s_dacom_bank -> stg_SybaseadminSDacomBank
    """
    if not name:
        return name

    # Find the prefix
    prefix = ''
    body = name
    for p in _KNOWN_PREFIXES:
        if name.startswith(p):
            prefix = p
            body = name[len(p):]
            break

    if not body:
        return name

    # Split body on underscores, PascalCase each part, join without separator
    parts = [p for p in body.split('_') if p]
    pascal_body = ''.join(_pascal_word(part) for part in parts)

    return prefix + pascal_body


# Blocklist of placeholder/generic names that should never be used
PLACEHOLDER_BLOCKLIST = {
    # Generic placeholders
    'my_table', 'your_table', 'the_table', 'a_table',
    'source_table', 'target_table', 'temp_table',
    'table1', 'table2', 'table3',
    'unknown_table', 'unknown_source', 'unknown',
    'insert_table_name_here', 'table_name_here',
    'placeholder', 'example', 'sample', 'test',
    
    # Generic dimension/fact names
    'dim_table', 'fact_table', 'stg_table',
    
    # Too generic
    'users', 'data', 'records', 'items', 'values',
    'source', 'target', 'input', 'output',
}

# Patterns that indicate placeholder names
PLACEHOLDER_PATTERNS = [
    r'^source_for_',
    r'^target_for_',
    r'^my_',
    r'^your_',
    r'^some_',
    r'_src$',
    r'_tgt$',
    r'^temp_',
    r'^tmp_',
    r'_temp$',
    r'_tmp$',
    r'^xxx',
    r'xxx$',
    r'^todo_',
    r'_todo$',
]


def sanitize_name(name: str) -> str:
    """
    Sanitize a name for use in Dataform action targets.
    
    Rules:
    - Lowercase only
    - Replace dots with underscores
    - Replace other invalid characters with underscores
    - Collapse multiple underscores
    - Remove leading/trailing underscores
    
    Args:
        name: The name to sanitize
        
    Returns:
        Sanitized name
    """
    if not name:
        return name
    
    # Lowercase
    sanitized = name.lower()
    
    # Replace dots with underscores (Dataform doesn't allow dots in action names)
    sanitized = sanitized.replace('.', '_')
    
    # Replace other invalid characters with underscores
    sanitized = re.sub(r'[^a-z0-9_]', '_', sanitized)
    
    # Collapse multiple underscores
    sanitized = re.sub(r'_+', '_', sanitized)
    
    # Remove leading/trailing underscores
    sanitized = sanitized.strip('_')
    
    return sanitized


def sanitize_filename(name: str, extension: str = '.sqlx') -> str:
    """
    Sanitize a name for use as a filename.
    
    Args:
        name: The name to sanitize
        extension: File extension to append
        
    Returns:
        Sanitized filename
    """
    sanitized = sanitize_name(name)
    
    # Limit length
    max_len = 100 - len(extension)
    if len(sanitized) > max_len:
        sanitized = sanitized[:max_len]
    
    return sanitized + extension


def is_placeholder_name(name: str) -> bool:
    """
    Check if a name appears to be a placeholder.
    
    Args:
        name: The name to check
        
    Returns:
        True if the name appears to be a placeholder
    """
    if not name:
        return True
    
    name_lower = name.lower()
    
    # Check blocklist
    if name_lower in PLACEHOLDER_BLOCKLIST:
        return True
    
    # Check patterns
    for pattern in PLACEHOLDER_PATTERNS:
        if re.search(pattern, name_lower):
            return True
    
    return False


def validate_action_name(name: str) -> Tuple[bool, Optional[str]]:
    """
    Validate a name for use as a Dataform action name.
    
    Args:
        name: The name to validate
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    if not name:
        return False, "Name is empty"
    
    # Check for dots
    if '.' in name:
        return False, f"Name contains dot: {name}"
    
    # Check for invalid characters (allow PascalCase with prefix_Body format)
    if not re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', name):
        return False, f"Name contains invalid characters: {name}"
    
    # Check for placeholder names
    if is_placeholder_name(name):
        return False, f"Name appears to be a placeholder: {name}"
    
    # Check length
    if len(name) > 100:
        return False, f"Name too long ({len(name)} chars): {name[:50]}..."
    
    return True, None


def normalize_table_name(name: str) -> str:
    """
    Normalize a table name for consistent referencing.
    
    This handles variations like:
    - FocalResearch -> focal_research
    - FOCAL_RESEARCH -> focal_research
    - focal.research -> focal_research
    
    Args:
        name: The table name to normalize
        
    Returns:
        Normalized table name
    """
    if not name:
        return name
    
    # Handle camelCase and PascalCase
    # Insert underscore before uppercase letters (except at start)
    normalized = re.sub(r'(?<!^)(?<!_)([A-Z])', r'_\1', name)
    
    # Apply standard sanitization
    normalized = sanitize_name(normalized)
    
    return normalized


def get_ref_name(table_name: str) -> str:
    """
    Get the name to use in ${ref('...')} calls.
    
    This ensures refs are consistent with declaration names.
    
    Args:
        table_name: The original table name
        
    Returns:
        Name to use in ref() calls
    """
    return sanitize_name(table_name)


def get_declaration_name(source_name: str) -> str:
    """
    Get the name to use in source declaration config blocks.
    
    This ensures declaration names match what refs expect.
    
    Args:
        source_name: The original source name
        
    Returns:
        Name to use in declaration config
    """
    return sanitize_name(source_name)


class NamingEnforcer:
    """
    Enforces naming conventions across SQLX content.
    
    Can be used to fix naming issues in existing content.
    """
    
    def __init__(self):
        self.fixes_applied = []
    
    def fix_names_in_content(self, content: str) -> str:
        """
        Fix naming issues in SQLX content.

        Applies PascalCase convention to config names and ref calls.
        Does NOT force lowercase — uses to_pascal_name() instead.

        Args:
            content: The SQLX content to fix

        Returns:
            Fixed content
        """
        self.fixes_applied = []
        fixed = content

        # Fix name field in config block — apply PascalCase, not lowercase
        def fix_config_name(match):
            original = match.group(1)
            # First sanitize (clean invalid chars), then PascalCase
            sanitized = sanitize_name(original)
            fixed_name = to_pascal_name(sanitized)
            if fixed_name != original:
                self.fixes_applied.append(f"Config name: {original} -> {fixed_name}")
            return f'name: "{fixed_name}"'

        fixed = re.sub(r'name:\s*"([^"]+)"', fix_config_name, fixed)

        # Fix ref calls — apply PascalCase, not lowercase
        def fix_ref(match):
            original = match.group(1)
            sanitized = sanitize_name(original)
            fixed_name = to_pascal_name(sanitized)
            if fixed_name != original:
                self.fixes_applied.append(f"Ref: {original} -> {fixed_name}")
            return f"${{ref('{fixed_name}')}}"

        fixed = re.sub(r"\$\{ref\('([^']+)'\)\}", fix_ref, fixed)
        fixed = re.sub(r'\$\{ref\("([^"]+)"\)\}', fix_ref, fixed)

        return fixed
    
    def get_fixes_applied(self) -> list:
        """Get list of fixes that were applied."""
        return self.fixes_applied


def enforce_naming(content: str) -> Tuple[str, list]:
    """
    Convenience function to enforce naming conventions.
    
    Args:
        content: The SQLX content to fix
        
    Returns:
        Tuple of (fixed_content, fixes_applied)
    """
    enforcer = NamingEnforcer()
    fixed = enforcer.fix_names_in_content(content)
    return fixed, enforcer.get_fixes_applied()
