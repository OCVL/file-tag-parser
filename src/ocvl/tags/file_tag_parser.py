import json
from pathlib import Path

import pandas as pd
import parse
from parse_type import TypeBuilder

from ocvl.tags.json_format_constants import DataTags, DataFormat, AcquisiTags, MetaTags


class FileTagParser():

    json_dict = dict()
    format_parsers = dict()

    def __init__(self, format_dict=None):
        # An optional parser for strings.
        self.optional_parse = TypeBuilder.with_optional(lambda opt_str: str(opt_str))

        for dataformat, form in format_dict.items():
            self.format_parsers[dataformat] = parse.compile(form, {"s?":self.optional_parse})


    @classmethod
    def from_json(cls, json_file, root_group=None):
        with open(json_file, 'r') as config_json_path:
            json_dict = json.load(config_json_path)

            return cls.from_dataformat_dict(json_dict, json_file, root_group=root_group)

    @classmethod
    def from_dataformat_dict(cls, json_dict_base=None, parse_path=None, root_group=None):

        allFilesColumns = [AcquisiTags.DATASET, AcquisiTags.DATA_PATH, DataFormat.FORMAT_TYPE]
        allFilesColumns.extend([d.value for d in DataTags])

        cls.json_dict = json_dict_base

        if root_group is not None:
            sub_dict = json_dict_base.get(root_group)
        else:
            sub_dict = json_dict_base

        cls.parser_extensions = ()

        if sub_dict is not None and parse_path is not None:
            form_dict = dict()

            for format in DataFormat:
                form = sub_dict.get(format)

                if form is not None and isinstance(form, str):
                    form_dict[format] = form

                    cls.parser_extensions = cls.parser_extensions + (form[form.rfind(".", -5, -1):],) if form and form[form.rfind(".", -5,
                                                                                                          -1):] not in cls.parser_extensions else cls.parser_extensions

            metadata_form = None
            metadata_params = None
            if sub_dict.get(MetaTags.METATAG) is not None:
                metadata_params = sub_dict.get(MetaTags.METATAG)
                metadata_form = metadata_params.get(DataFormat.METADATA)

            cls.parser_extensions = cls.parser_extensions + (metadata_form[metadata_form.rfind(".", -5, -1):],) if metadata_form and metadata_form[ metadata_form.rfind(".", -5, -1):] not in cls.parser_extensions else cls.parser_extensions

            # Construct the parser we'll use for each of these forms
            return cls(form_dict)
        else:
            return None

    def parse_filename(self, file_string):

        filename_tags = dict()
        parsed_str = None
        parser_used = None

        for dataformat, parser in self.format_parsers.items():
            parsed_str = parser.parse(file_string)

            if parsed_str is not None:
                parser_used = dataformat
                break

        if parsed_str is None:
            return None, filename_tags

        for formatstr in DataTags:
            if formatstr in parsed_str.named:
                if parsed_str[formatstr] is not None:
                    filename_tags[formatstr.value] = parsed_str[formatstr]
                else:
                    filename_tags[formatstr.value] = ""

        return parser_used, filename_tags

    def parse_path(self, parse_path, recurse_me=False):
        # Parse out the locations and filenames, store them in a hash table by location.
        searchpath = Path(parse_path)
        allFiles = list()
        
        if recurse_me:
            for ext in self.parser_extensions:
                for path in searchpath.rglob("*" + ext):
                    format_type, file_info = self.parse_filename(path.name)
                    if format_type is not None:
                        file_info[DataFormat.FORMAT_TYPE] = format_type
                        file_info[AcquisiTags.DATA_PATH] = path
                        file_info[AcquisiTags.BASE_PATH] = path.parent
                        file_info[AcquisiTags.DATASET] = None
                        entry = pd.DataFrame.from_dict([file_info])

                        allFiles.append(entry)
        else:
            for ext in self.parser_extensions:
                for path in searchpath.glob("*" + ext):
                    format_type, file_info = self.parse_filename(path.name)
                    if format_type is not None:
                        file_info[DataFormat.FORMAT_TYPE] = format_type
                        file_info[AcquisiTags.DATA_PATH] = path
                        file_info[AcquisiTags.BASE_PATH] = path.parent
                        file_info[AcquisiTags.DATASET] = None
                        entry = pd.DataFrame.from_dict([file_info])

                        allFiles.append(entry)

        if allFiles:
            return pd.concat(allFiles, ignore_index=True)
        else:
            return pd.DataFrame()

    def get_dict(self):
        return self.json_dict