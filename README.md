# file-tag-parser

| | |
| --- | --- |
|Package| [![PyPI Latest Release](https://img.shields.io/pypi/v/file-tag-parser.svg)](https://pypi.org/project/file-tag-parser/)|

A tag-based filename parser for imaging data.

This library is designed to make parsing of arbitrary filenames attainable and searchable via a pandas-based database. 
Each filename can then be interpreted and/or manipulated for whatever processing and analysis is desired.

At the moment, the library is solely dedicated to the interpretation of complex filenames using
a "tagging system", to streamline their grouping and loading. This will likely be expanded in the future as needed.

Below are the details for how the file tag parser works. This allows the program to easily handle filename structures from different devices and groups, by extracting "tags" from the filename that have meaning in the context of data analysis.

## Filename strings and tags
Filename tags are highly useful for grouping datasets for processing and analysis. Each of the tags below will be extracted from the filename if it is contained within braces. 

| Tag | Description |
| :---: | :--- |
| `IDnum` | The ID of the participant that was imaged. |
| `VidNum` | The acquisition number that the data corresponds to. **This tag MUST be present for F(Cell) to work!** All data may share modalities but **not** video numbers. |
| `Year` | The year the data was obtained, in YYYY format. |
| `Month` | The month the data was obtained, in MM format. |
| `Day` | The day the data was obtained, in DD format. |
| `Hour` | The hour the data was obtained, in HH format. Either 12 hour or 24 clocks are allowed. |
| `Minute` | The minute the data was obtained, in MM format. |
| `Second` | The second the data was obtained, in SS format. |
| `Eye` | The eye the data was obtained from. There are no restrictions on use of OS/OD or left/right. |
| `LocX` | The azimuthal (X) location that the data was obtained from. Usually corresponds to temporal/nasal eccentricity relative to the locus of fixation, but can be in any type of coordinate. **Example:** 4T |
| `LocY` | The polar (Y) location that the data was obtained from. Usually corresponds to superior/inferior eccentricity relative to the locus of fixation, but can be in any type of coordinate. **Example:** 9S |
| `LocZ` | The radial (Z) location that the data was obtained from. Usually corresponds to the focus of the device, but can be in any type of coordinate (especially if using OCT). **Example:** 0.15D |
| `FOV_Width` | The field of view width. Usually in degrees, but can be anything. |
| `FOV_Height` | The field of view height. Usually in degrees, but can be anything. |
| `FOV_Depth` | The depth of field. |
| `Modality` | The modality that the data was obtained from, in devices that have multiple modes/channels. **Example:** 760nm |
| `QueryLoc` | The name of the query locations. Very useful for datasets with multiple possible coordinate sets. Can be an empty field, as well; to make this field optional, add `:s?` to the end. **Example:** {QueryLoc:s?} |

## Example filenames and tags

### Example 1:
For an image filename with the following format:

`8675309_OD_1776_850nm_AVG.tif`

and tag string:

`{IDnum}_{Eye}_{VidNum}_{Modality}_AVG.tif`

The program would store the following tags in the pandas database:
```python
IDNum = "8675309"
Eye = "OD"
VidNum = "1776"
Modality = "850nm"
```

### Example 2:
For an video filename with the following format:

`hellothere_OD_1861_split_det_favorite.avi`

and tag string:

`{IDnum}_OD_{VidNum}_{Modality}_favorite.avi`

The program would store the following tags in the pandas database:
```python
IDNum = "hellothere"
VidNum = "1861"
Modality = "split_det"
```

### Example 3: Ignoring tags.
If your filenames have changing numbers that should be grouped together but file-tag-parser doesn't have a tag for them, they can be blank (ignored) fields. For example:

Video 1: `hellothere_OD_1861_split_det_vanilla.avi`
Video 2: `hellothere_OD_1861_confocal_chocolate.avi`

and tag string:

`{IDnum}_OD_{VidNum}_{Modality}_{}.avi`

file-tag-parser would store the following tags for use in other analysis steps:

Video 1:
```python
IDNum = "hellothere"
VidNum = "1861"
Modality = "split_det"
```

Video 2:
```python
IDNum = "hellothere"
VidNum = "1861"
Modality = "confocal"
```

### Example 4: Optional fields

In some cases, for example in query location files, files may take on a form that sometimes includes modifiers and other times does not. Optional fields automatically strip out common "separator" characters, (e.g. `-`, `_`, ` `, etc.) Optional fields are delineated in a tag string as follows:

`{Fieldname:s?}`

So for query location files: 

Query Locations 1: `hellothere_OD_1861_split_det_vanilla_coords.csv`

Query Locations 2: `hellothere_OD_1861_split_det_vanilla_subset_coords.csv`

and tag string:

`{IDnum}_OD_{VidNum}_{Modality}_{}_{QueryLoc:s?}coords.csv`

file-tag-parser would store the following tags in the database:

Query Locations 1:
```python
IDNum = "hellothere"
VidNum = "1861"
Modality = "split_det"
QueryLoc = ""
```

Query Locations 2:
```python
IDNum = "hellothere"
VidNum = "1861"
Modality = "split_det"
QueryLoc = "subset"
```
