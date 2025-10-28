import os
import unittest
from pathlib import Path

from file_tag_parser.tags.file_tag_parser import FileTagParser
from file_tag_parser.tags.json_format_constants import DataFormat


class FileTagTestCase(unittest.TestCase):
    def setUp(self):
        self.test_json_fname = Path(os.path.dirname(__file__)).joinpath("json_ex/test_json.json")

        self.test_preanalysis_nomatch_vid = "00-36828_20230324_OD_(4,0)_1x1_6385_760nm1_extract_reg_cropped_piped.avi"
        self.test_preanalysis_match_vid = "00-36828_20230324_OD_(4,0)_1x1_6385_760nm1_vid.avi"
        self.test_preanalysis_nomatch_mask = "00-36828_20230324_OD_(4,0)_1x1_6385_760nm1_piped.avi"
        self.test_preanalysis_match_mask = "00-36828_20230324_OD_(4,0)_1x1_6385_760nm1_mask.avi"


    def test_json_parser_init(self):
        parse_me = FileTagParser.from_json(self.test_json_fname)

        self.assertEqual(len(parse_me.format_parsers), 0, "Parser should be empty, since we aren't supplying a base json group.")

        parse_me = FileTagParser.from_json(self.test_json_fname, root_group="preanalysis")
        self.assertGreater(len(parse_me.format_parsers), 0,"Parser have more than 0 parsers available, since we are supplying a base json group.")

        self.assertNotEqual(len(parse_me.json_dict), 0, "JSON should not be empty.")


    def test_json_file_parsing(self):

        parse_me = FileTagParser.from_json(self.test_json_fname, root_group="preanalysis")
        self.assertGreater(len(parse_me.format_parsers), 0,"Parser have more than 0 parsers available, since we are supplying a base json group.")

        used_parser, file_tags = parse_me.parse_filename(self.test_preanalysis_nomatch_vid)
        self.assertIsNone(used_parser, f"This filename ({self.test_preanalysis_nomatch_vid}) should not match any parsers in this object.")
        self.assertEqual(len(file_tags), 0, f"This filename ({self.test_preanalysis_nomatch_vid}) should not produce any matches with the parser")

        used_parser, file_tags = parse_me.parse_filename(self.test_preanalysis_match_vid)
        self.assertEqual(used_parser, DataFormat.VIDEO,f"This filename ({self.test_preanalysis_match_vid}) match the video format parser.")
        self.assertEqual(len(file_tags), 11, f"This filename ({self.test_preanalysis_match_vid}) should produce 11 matches with the parser.")
        print(f"SUCCESS: Used parser: {used_parser}")
        print(f"Found tags: {file_tags}")

        used_parser, file_tags = parse_me.parse_filename(self.test_preanalysis_nomatch_mask)
        self.assertIsNone(used_parser, f"This filename ({self.test_preanalysis_nomatch_mask}) should not match any parsers in this object.")
        self.assertEqual(len(file_tags), 0, f"This filename ({self.test_preanalysis_nomatch_mask}) should not produce any matches with the parser")

        used_parser, file_tags = parse_me.parse_filename(self.test_preanalysis_match_mask)
        self.assertEqual(used_parser, DataFormat.MASK,f"This filename ({self.test_preanalysis_match_mask}) should match the mask format parser.")
        self.assertEqual(len(file_tags), 11, f"This filename ({self.test_preanalysis_match_mask}) produce 11 matches with the parser.")
        print(f"SUCCESS: Used parser: {used_parser}")
        print(f"Found tags: {file_tags}")


if __name__ == '__main__':
    unittest.main()
