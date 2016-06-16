# csvutils

CSV files are popular file format in enterprises for data exchange, and Excel is also one of most popular software used to edit CSV files. while for some reasons,
EXcel is not friendly when dealing with CSV.

By default,when a CSV file gets opened in Excel, CSV fields get automatically converted and formated, which is normally not expected, since it modifies the 
file content when saving back.

e.g.  '0011' will be treated as a number and conveted to 11.
      '2014-02-18' is converted to a date.

Here is the CSV plugin come to play.

It is a Excel plugin that streamlines the CSV editing in Excel.

Features:
1. All data are imported as text fields, no automatic conversion;
2. Fields widh get automatically adjusted;
3. Header line is automatically frozen;
4. When editing the file, all the formating : fonts, cells color etc. get saved.
