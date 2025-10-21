from enum import StrEnum

class DataFormatType(StrEnum):
    FORMAT_TYPE = "Data_Type"
    IMAGE = "image_format"
    VIDEO = "video_format"
    MASK = "mask_format"
    METADATA = "metadata_format"
    QUERYLOC = "queryloc_format"
    RECURSIVE = "recursive_search"

# Will probably want to make a list of these corresponding to the version of the json files.
# This verson assumes that we don't have any naming collisions; e.g. these constants aren't used in the filenames.
class DataTags(StrEnum):
    DATA_ID = "IDnum"
    VIDEO_ID = "VidNum"
    YEAR = "Year" # YYYY
    MONTH = "Month" # MM
    DAY = "Day" # DD
    HOUR = "Hour"
    MINUTE = "Minute"
    SECOND = "Second"
    EYE = "Eye"
    RET_LOC_X = "LocX"
    RET_LOC_Y = "LocY"
    RET_LOC_Z = "LocZ"
    FOV_WIDTH = "FOV_Width"
    FOV_HEIGHT = "FOV_Height"
    FOV_DEPTH = "FOV_Depth"
    MODALITY = "Modality"
    QUERYLOC = "QueryLoc"

class MetaTags(StrEnum):
    METATAG = "metadata"

class AcquisiTags(StrEnum):
    DATASET = "Dataset"
    DATA_PATH = "Data_Path"
    OUTPUT_PATH = "Output_Path"
    VIDEO_PATH = "Video_Path"
    IMAGE_PATH = "Image_Path"
    QUERYLOC_PATH = "QueryLocation_Path"
    BASE_PATH = "Base_Path"
    MASK_PATH = "Mask_Path"
    META_PATH = "Metadata_Path"


