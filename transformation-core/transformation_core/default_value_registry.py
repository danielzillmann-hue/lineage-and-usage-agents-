"""
Default Value Registry

Provides standard default values for common fields across mappings.
This eliminates "business logic needed" TODOs for standard operational fields.
"""

from typing import Dict, Optional, List


DEFAULT_VALUES = {
    # =======================
    # ETL Control Fields
    # =======================

    'ETLJobDtlID': {
        'default': '-1',
        'dataform_var': '${dataform.projectConfig.vars.etl_job_dtl_id}',
        'description': 'ETL Job Detail ID - Use Dataform variable or placeholder',
        'alternatives': [
            '-1',  # Placeholder for unknown job
            '${dataform.projectConfig.vars.etl_job_dtl_id}',  # From Dataform config
            'CAST(-1 AS INT64)',  # Explicit type cast
        ],
        'informatica_pattern': ':LKP.SHORTCUT_TO_LKP_JOBDETID($$ETLJobID, $$SiteID)',
        'bigquery_equivalent': '${dataform.projectConfig.vars.etl_job_dtl_id}'
    },

    'ETLJobID': {
        'default': '-1',
        'dataform_var': '${dataform.projectConfig.vars.etl_job_id}',
        'description': 'ETL Job ID - Use Dataform variable or placeholder',
        'alternatives': [
            '-1',
            '${dataform.projectConfig.vars.etl_job_id}',
            'CAST(-1 AS INT64)'
        ]
    },

    # =======================
    # Site/Location Fields
    # =======================

    'SiteID': {
        'default': '${dataform.projectConfig.vars.site_id}',
        'description': 'Site ID - Typically from Dataform variable or source data',
        'alternatives': [
            '${dataform.projectConfig.vars.site_id}',  # From Dataform config
            'source.PtyLocNum',  # From source party location number
            'source.SiteID',  # From source if available
            '1',  # Default site
        ],
        'source_column_candidates': ['PtyLocNum', 'SiteID', 'Site_ID', 'LocationID'],
        'lookup_table': None  # Usually not looked up, comes from config or source
    },

    'SctyCde': {
        'default': '0',
        'description': 'Security Code - Typically 0 for internal ETL processes',
        'alternatives': [
            '0',  # Default for internal processes
            'source.SctyCde',  # From source if exists
            '${dataform.projectConfig.vars.security_code}'  # From config
        ],
        'source_column_candidates': ['SctyCde', 'SecurityCode', 'SecCde']
    },

    'PtyLocNum': {
        'default': '${dataform.projectConfig.vars.site_id}',
        'description': 'Party Location Number - Usually same as SiteID',
        'alternatives': [
            '${dataform.projectConfig.vars.site_id}',
            'source.PtyLocNum',
            '1'
        ]
    },

    # =======================
    # SCD Type 2 Fields
    # =======================

    'CurrentFlg': {
        'default': "'Y'",
        'description': 'Current record flag for SCD Type 2 tables',
        'alternatives': [
            "'Y'",  # New records are current
            "CASE WHEN EffEndDate IS NULL THEN 'Y' ELSE 'N' END",  # Derived from EffEndDate
            "IF(EffEndDate IS NULL, 'Y', 'N')"  # Alternative syntax
        ],
        'pattern_for_insert': "'Y'",
        'pattern_for_update': "CASE WHEN EffEndDate IS NULL THEN 'Y' ELSE 'N' END"
    },

    'EffStartDate': {
        'default': 'CURRENT_DATE()',
        'description': 'Effective start date for SCD Type 2',
        'alternatives': [
            'CURRENT_DATE()',
            'source.TransactionDate',  # From source transaction
            'source.EffectiveDate',
            '${dataform.projectConfig.vars.processing_date}'
        ],
        'source_column_candidates': ['TransactionDate', 'EffectiveDate', 'StartDate', 'TxnDate']
    },

    'EffEndDate': {
        'default': 'NULL',
        'description': 'Effective end date for SCD Type 2 - NULL for current records',
        'alternatives': [
            'NULL',  # Current records
            'DATE(\'9999-12-31\')',  # Far future date
            'source.EndDate'  # If updating
        ]
    },

    # =======================
    # Audit Fields
    # =======================

    'InsertDtTm': {
        'default': 'CURRENT_TIMESTAMP()',
        'description': 'Insert timestamp - when record was created',
        'alternatives': [
            'CURRENT_TIMESTAMP()',
            'CURRENT_DATETIME()',
            '${dataform.projectConfig.vars.processing_timestamp}'
        ]
    },

    'UpdateDtTm': {
        'default': 'CURRENT_TIMESTAMP()',
        'description': 'Update timestamp - when record was last modified',
        'alternatives': [
            'CURRENT_TIMESTAMP()',
            'CURRENT_DATETIME()',
            '${dataform.projectConfig.vars.processing_timestamp}'
        ]
    },

    'InsertUserId': {
        'default': "'ETL_PROCESS'",
        'description': 'User who inserted the record',
        'alternatives': [
            "'ETL_PROCESS'",
            "'DATAFORM'",
            "'SYSTEM'",
            '${dataform.projectConfig.vars.etl_user}'
        ]
    },

    'UpdateUserId': {
        'default': "'ETL_PROCESS'",
        'description': 'User who last updated the record',
        'alternatives': [
            "'ETL_PROCESS'",
            "'DATAFORM'",
            "'SYSTEM'",
            '${dataform.projectConfig.vars.etl_user}'
        ]
    },

    # =======================
    # Status/Flag Fields
    # =======================

    'ActiveFlg': {
        'default': "'Y'",
        'description': 'Active record flag',
        'alternatives': [
            "'Y'",
            "CASE WHEN DeletedFlg = 'Y' THEN 'N' ELSE 'Y' END"
        ]
    },

    'DeletedFlg': {
        'default': "'N'",
        'description': 'Deleted record flag',
        'alternatives': ["'N'", "'0'", 'FALSE']
    },

    'ProcessedFlg': {
        'default': "'N'",
        'description': 'Processed flag - Y after processing',
        'alternatives': ["'N'", "'0'", 'FALSE']
    },

    # =======================
    # Default Numeric Values
    # =======================

    'RowVersion': {
        'default': '1',
        'description': 'Row version for optimistic locking',
        'alternatives': ['1', 'CAST(1 AS INT64)']
    },

    'RecordCount': {
        'default': '1',
        'description': 'Record count - usually 1 per row',
        'alternatives': ['1', 'CAST(1 AS INT64)']
    },

    # =======================
    # Reject/Alert Flags
    # =======================

    'RejectFlg': {
        'default': '0',
        'description': 'Reject flag - 1 if record should be rejected',
        'alternatives': ['0', "'N'", 'FALSE']
    },

    'AlertFlg': {
        'default': '0',
        'description': 'Alert flag - 1 if record should trigger alert',
        'alternatives': ['0', "'N'", 'FALSE']
    },

    'ValidCode': {
        'default': '0',
        'description': 'Validation code - 0 for valid records',
        'alternatives': [
            '0',
            'CASE WHEN {validation_condition} THEN 0 ELSE {error_code} END'
        ]
    },

    'ValidCodeDesc': {
        'default': "'Valid'",
        'description': 'Validation code description',
        'alternatives': [
            "'Valid'",
            "CASE WHEN {validation_condition} THEN 'Valid' ELSE {error_desc} END"
        ]
    }
}


def get_default_value(field_name: str, context: Optional[Dict] = None) -> Optional[str]:
    """
    Get default value for a common field.

    Args:
        field_name: Name of the field (case-insensitive)
        context: Optional context dict with:
            - source_columns: List of available source columns
            - is_insert: Boolean indicating if this is an insert operation
            - is_update: Boolean indicating if this is an update operation

    Returns:
        Default value expression or None if not found
    """
    field_lower = field_name.lower()

    # Try exact match first
    for key, config in DEFAULT_VALUES.items():
        if key.lower() == field_lower:
            return _get_best_value(config, context)

    # Try partial match
    for key, config in DEFAULT_VALUES.items():
        if key.lower() in field_lower:
            return _get_best_value(config, context)

    return None


def _get_best_value(config: Dict, context: Optional[Dict]) -> str:
    """
    Get the best value from config based on context.

    Args:
        config: Configuration dictionary for the field
        context: Context information

    Returns:
        Best default value expression
    """
    if not context:
        return config['default']

    # Check if source column exists
    if 'source_column_candidates' in config and 'source_columns' in context:
        source_cols = context['source_columns']
        for candidate in config['source_column_candidates']:
            if any(candidate.lower() in col.lower() for col in source_cols):
                return f"source.{candidate}"

    # Check for operation-specific patterns
    if context.get('is_insert') and 'pattern_for_insert' in config:
        return config['pattern_for_insert']

    if context.get('is_update') and 'pattern_for_update' in config:
        return config['pattern_for_update']

    # Use Dataform variable if available
    if 'dataform_var' in config:
        return config['dataform_var']

    return config['default']


def get_field_description(field_name: str) -> Optional[str]:
    """Get description for a common field."""
    field_lower = field_name.lower()

    for key, config in DEFAULT_VALUES.items():
        if key.lower() == field_lower or key.lower() in field_lower:
            return config.get('description')

    return None


def format_default_values_for_prompt() -> str:
    """
    Format default values for inclusion in LLM prompt.

    Returns:
        Formatted string describing default values
    """
    output = ["**🔧 STANDARD DEFAULT VALUES** (Use these for common fields - DO NOT add TODOs):"]
    output.append("")
    output.append("For these standard fields, use the specified default values instead of creating TODOs:")
    output.append("")

    # Group by category
    categories = {
        'ETL Control Fields': ['ETLJobDtlID', 'ETLJobID'],
        'Site/Location Fields': ['SiteID', 'SctyCde', 'PtyLocNum'],
        'SCD Type 2 Fields': ['CurrentFlg', 'EffStartDate', 'EffEndDate'],
        'Audit Fields': ['InsertDtTm', 'UpdateDtTm', 'InsertUserId', 'UpdateUserId'],
        'Status/Flag Fields': ['ActiveFlg', 'DeletedFlg', 'ProcessedFlg', 'RejectFlg', 'AlertFlg', 'ValidCode', 'ValidCodeDesc']
    }

    for category, fields in categories.items():
        output.append(f"### {category}")
        output.append("")

        for field_name in fields:
            if field_name in DEFAULT_VALUES:
                config = DEFAULT_VALUES[field_name]
                default = config['default']
                desc = config['description']

                # Show primary default
                output.append(f"**{field_name}:**")
                output.append(f"  - Default: `{default}`")
                output.append(f"  - Description: {desc}")

                # Show Dataform variable alternative if exists
                if 'dataform_var' in config:
                    output.append(f"  - Dataform Variable: `{config['dataform_var']}`")

                # Show source column candidates if exists
                if 'source_column_candidates' in config:
                    candidates = ', '.join(config['source_column_candidates'])
                    output.append(f"  - Check Source Columns: {candidates}")

                output.append("")

        output.append("")

    output.append("💡 **IMPORTANT RULES:**")
    output.append("1. Use Dataform variables for ETL control fields (ETLJobDtlID, SiteID)")
    output.append("2. Check if source data provides these values before using defaults")
    output.append("3. For SCD Type 2: CurrentFlg='Y' for new records, EffEndDate=NULL for current")
    output.append("4. For audit fields: Use CURRENT_TIMESTAMP() for timestamps")
    output.append("5. DO NOT create TODOs for these standard fields - use the defaults shown")
    output.append("")

    return "\n".join(output)


def is_known_default_field(field_name: str) -> bool:
    """
    Check if a field has a known default value.

    Args:
        field_name: Name of the field (case-insensitive)

    Returns:
        True if field has a known default, False otherwise
    """
    return get_default_value(field_name) is not None


# Export all functions and constants
__all__ = [
    'DEFAULT_VALUES',
    'get_default_value',
    'get_field_description',
    'format_default_values_for_prompt',
    'is_known_default_field'
]
