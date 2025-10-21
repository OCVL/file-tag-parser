import json
from pathlib import Path

import pandas as pd
import parse
from parse_type import TypeBuilder

from ocvl.tags.json_format_constants import DataTags, DataFormatType, AcquisiTags, MetaTags


class FileTagParser():
    def __init__(self, video_format=None, mask_format=None, image_format=None, metadata_format=None, queryloc_format=None):
        self.formatlocs = dict()
        self.json_dict = dict()

        # An optional parser for strings.
        self.optional_parse = TypeBuilder.with_optional(lambda opt_str: str(opt_str))

        if video_format is not None:
            self.vid_parser = parse.compile(video_format, {"s?":self.optional_parse})
        else:
            self.vid_parser = None

        if mask_format is not None:
            self.mask_parser = parse.compile(mask_format, {"s?":self.optional_parse})
        else:
            self.mask_parser = None

        if image_format is not None:
            self.im_parser = parse.compile(image_format, {"s?":self.optional_parse})
        else:
            self.im_parser = None

        if queryloc_format is not None:
            self.queryloc_parser = parse.compile(queryloc_format, {"s?":self.optional_parse})
        else:
            self.queryloc_parser = None

        if metadata_format is not None:
            self.metadata_parser = parse.compile(metadata_format, {"s?":self.optional_parse})
        else:
            self.metadata_parser = None

    @classmethod
    def from_json(cls, json_file, root_group=None):
        with open(json_file, 'r') as config_json_path:
            json_dict = json.load(config_json_path)

            return cls.from_dict(json_dict, json_file, root_group=root_group)

    @classmethod
    def from_dict(cls, json_dict_base=None, parse_path=None, root_group=None):

        allFilesColumns = [AcquisiTags.DATASET, AcquisiTags.DATA_PATH, DataFormatType.FORMAT_TYPE]
        allFilesColumns.extend([d.value for d in DataTags])

        if root_group is not None:
            cls.json_dict = json_dict_base.get(root_group)
        else:
            cls.json_dict = json_dict_base

        if cls.json_dict is not None and parse_path is not None:
            im_form = cls.json_dict.get(DataFormatType.IMAGE)
            vid_form = cls.json_dict.get(DataFormatType.VIDEO)
            mask_form = cls.json_dict.get(DataFormatType.MASK)
            query_form = cls.json_dict.get(DataFormatType.QUERYLOC)

            metadata_form = None
            metadata_params = None
            if cls.json_dict.get(MetaTags.METATAG) is not None:
                metadata_params = cls.json_dict.get(MetaTags.METATAG)
                metadata_form = metadata_params.get(DataFormatType.METADATA)

            cls.parser_extensions = ()
            # Grab our extensions, make sure to check them all.
            cls.parser_extensions = cls.parser_extensions + (vid_form[vid_form.rfind(".", -5, -1):],) if vid_form else cls.parser_extensions
            cls.parser_extensions = cls.parser_extensions + (mask_form[mask_form.rfind(".", -5, -1):],) if mask_form and mask_form[
                                                                                             mask_form.rfind(".", -5,
                                                                                                             -1):] not in cls.parser_extensions else cls.parser_extensions
            cls.parser_extensions = cls.parser_extensions + (im_form[im_form.rfind(".", -5, -1):],) if im_form and im_form[im_form.rfind(".", -5,
                                                                                                             -1):] not in cls.parser_extensions else cls.parser_extensions
            cls.parser_extensions = cls.parser_extensions + (query_form[query_form.rfind(".", -5, -1):],) if query_form and query_form[
                                                                                                query_form.rfind(".",
                                                                                                                 -5,
                                                                                                                 -1):] not in cls.parser_extensions else cls.parser_extensions
            cls.parser_extensions = cls.parser_extensions + (metadata_form[metadata_form.rfind(".", -5, -1):],) if metadata_form and metadata_form[
                                                                                                         metadata_form.rfind(
                                                                                                             ".", -5,
                                                                                                             -1):] not in cls.parser_extensions else cls.parser_extensions

            # Construct the parser we'll use for each of these forms
            return cls(vid_form, mask_form, im_form, metadata_form, query_form)
        else:
            return None

    def parse_filename(self, file_string):

        filename_metadata = dict()

        parsed_str = self.vid_parser.parse(file_string)
        parser_used = DataFormatType.VIDEO
        if parsed_str is None and self.mask_parser is not None:
            parsed_str = self.mask_parser.parse(file_string)
            parser_used = DataFormatType.MASK
        if parsed_str is None and self.im_parser is not None:
            parsed_str = self.im_parser.parse(file_string)
            parser_used = DataFormatType.IMAGE
        if parsed_str is None and self.queryloc_parser is not None:
            parsed_str = self.queryloc_parser.parse(file_string)
            parser_used = DataFormatType.QUERYLOC
        if parsed_str is None and self.metadata_parser is not None:
            parsed_str = self.metadata_parser.parse(file_string)
            parser_used = DataFormatType.METADATA
        if parsed_str is None:
            return None, filename_metadata


        for formatstr in DataTags:
            if formatstr in parsed_str.named:
                if parsed_str[formatstr] is not None:
                    filename_metadata[formatstr.value] = parsed_str[formatstr]
                else:
                    filename_metadata[formatstr.value] = ""


        return parser_used, filename_metadata

    def parse_path(self, parse_path, recurse_me=False):
        # Parse out the locations and filenames, store them in a hash table by location.
        searchpath = Path(parse_path)
        allFiles = list()
        
        if recurse_me:
            for ext in self.parser_extensions:
                for path in searchpath.rglob("*" + ext):
                    format_type, file_info = self.parse_filename(path.name)
                    if format_type is not None:
                        file_info[DataFormatType.FORMAT_TYPE] = format_type
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
                        file_info[DataFormatType.FORMAT_TYPE] = format_type
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