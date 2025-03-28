# -*- coding: utf-8 -*-

from imio_luigi.core.target_rest import (
    DeleteRESTTarget,
    PatchRESTTarget,
    PostRESTTarget,
    PutRESTTarget,
)
from imio_luigi.core.task_access import GetFromAccessJSONTask
from imio_luigi.core.task_basic import InMemoryTask
from imio_luigi.core.task_cleanup import (
    ConvertDateInMultiFormatMemoryTask,
    ConvertDateTaskMultiFormat,
    ConvertDateInMemoryTask,
    ConvertDateTask,
    DropColumnInMemoryTask,
    DropColumnTask,
    StringToListInMemoryTask,
    StringToListRegexpInMemoryTask,
    StringToListRegexpTask,
    StringToListTask,
    ValueFixerInMemoryTask,
    ValueFixerTask,
)
from imio_luigi.core.task_database import (
    GetFromMySQLTask,
    JoinFromMySQLInMemoryTask,
    JoinFromMySQLTask,
)
from imio_luigi.core.task_csv import GetFromCSVFile
from imio_luigi.core.task_xml import (GetFromXMLFile, GetFromListXMLFile)
from imio_luigi.core.task_filesystem import WalkFS, WriteToJSONTask
from imio_luigi.core.task_mapping import (
    MappingKeysInMemoryTask,
    MappingKeysTask,
    MappingValueInMemoryTask,
    MappingValueTask,
    MappingValueWithFileInMemoryTask,
    MappingValueWithFileTask,
)
from imio_luigi.core.task_rest import (
    DeleteRESTTask,
    GetFromRESTServiceTask,
    GetFromRESTServiceInMemoryTask,
    PatchRESTTask,
    PostRESTTask,
)
from imio_luigi.core.task_transform import (
    AddDataTask,
    AddDataInMemoryTask,
    CreateSubElementInMemoryTask,
    CreateSubElementsFromSubElementsInMemoryTask,
    CreateSubElementsFromSubElementsTask,
    CreateSubElementTask,
    UpdateReferenceTask,
    UpdateReferenceInMemoryTask,
)
from imio_luigi.core.task_validation import JSONSchemaValidationTask
from imio_luigi.core.utils import frozendict_to_dict
from luigi.mock import MockTarget as InMemoryTarget


__all__ = (
    "AddDataInMemoryTask",
    "AddDataTask",
    "ConvertDateInMemoryTask",
    "ConvertDateTask",
    "CreateSubElementInMemoryTask",
    "CreateSubElementTask",
    "CreateSubElementsFromSubElementsInMemoryTask",
    "CreateSubElementsFromSubElementsTask",
    "DeleteRESTTarget",
    "DeleteRESTTask",
    "DropColumnInMemoryTask",
    "DropColumnTask",
    "GetFromAccessJSONTask",
    "GetFromMySQLTask",
    "GetFromRESTServiceInMemoryTask",
    "GetFromRESTServiceTask",
    "GetFromCSVFile",
    "GetFromXMLFile",
    "GetFromListXMLFile",
    "InMemoryTarget",
    "InMemoryTask",
    "JSONSchemaValidationTask",
    "JoinFromMySQLInMemoryTask",
    "JoinFromMySQLTask",
    "MappingKeysInMemoryTask",
    "MappingKeysTask",
    "MappingValueInMemoryTask",
    "MappingValueTask",
    "MappingValueWithFileInMemoryTask",
    "MappingValueWithFileTask",
    "PatchRESTTarget",
    "PatchRESTTask",
    "PostRESTTarget",
    "PostRESTTask",
    "PutRESTTarget",
    "StringToListInMemoryTask",
    "StringToListRegexpInMemoryTask",
    "StringToListRegexpTask",
    "StringToListTask",
    "UpdateReferenceInMemoryTask",
    "UpdateReferenceTask",
    "ValueFixerInMemoryTask",
    "ValueFixerTask",
    "WalkFS",
    "WriteToJSONTask",
    "frozendict_to_dict",
    "ConvertDateTaskMultiFormat",
    "ConvertDateInMultiFormatMemoryTask"
)
