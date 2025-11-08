import enum
from dataclasses import dataclass


class OptionAutoUpdateFields(enum.Enum):
    NO = "no"
    AUTO = "auto"
    ALWAYS = "always"


class OptionKeepFields(enum.Enum):
    NONE = "none"
    SOME = "some"
    ALL = "all"


@dataclass
class MailMergeOptions:
    """This is the Options class for MailMerge.

    You can use this class to change the way the MailMerge behaves, how the tables are handled, auto updates on fields, etc.

    Args:
        remove_empty_tables (bool): Default(False) - When True the empty tables are removed from the document
        auto_update_fields_on_open (OptionAutoUpdateFields): Default(NO) - When "AUTO" it sets the auto_update Flag in the output DOCX if necessary. When "ALWAYS" the flag will always be set.
        keep_fields (OptionKeepFields): Default(NONE) - When NONE all fields are replaced, even those not updated. When SOME the fields that were not updated are kept. When ALL then all fields are kept.
        enable_experimental (bool): Default(False) - Deprecated field to enable updating IF fields. Use merge_if_fields instead.
        merge_if_fields (bool): Default(False) - When True the if fields will be updated.
        table_rows_replace_mode (bool): Default(False) - When True, the table rows will replace existing table rows, raising an Exception if it overflows.
    """

    remove_empty_tables: bool = False
    auto_update_fields_on_open: OptionAutoUpdateFields = OptionAutoUpdateFields.NO
    keep_fields: OptionKeepFields = OptionKeepFields.NONE
    enable_experimental: bool = False
    merge_if_fields: bool = False
    table_rows_replace_mode: bool = False
