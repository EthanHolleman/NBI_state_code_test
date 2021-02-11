from pathlib import Path
import zipfile
import urllib.request
import csv

here = Path(__file__).parent.absolute()
table_path = here.joinpath('STATE_CODE_001.csv')


csv_dir = here.joinpath('csv')
undim_dir = here.joinpath('undim')

delim_files = list(csv_dir.iterdir())
undim_files = list(undim_dir.iterdir())


def is_matched(string, code_table):
    if string.strip() in code_table:
        return True
    return False


def extract_field_from_row(row, index=None, start=None, end=None):
    # extract an item from a row either represented as a list or
    # as a string (delim vs undelim)
    if type(row) == list:
        if index == None:
            raise ValueError(
                '''A index for the row must be supplied when 
        row is of type list''')
        field = row[index]
    elif type(row) == str:
        if type(start) != int or type(end) != int:
            raise ValueError(
                '''A start position and end position must be supploed when 
        row is of type str''')
        field = row[start:end]
    else:
        raise ValueError('Row must be either a string or list')

    return field


def read_data_file(filepath, delim):
    with open(str(filepath)) as handle:
        if delim:
            reader = csv.reader(handle, delimiter=delim)
        else:
            reader = handle
        for row in reader:
            yield row
    

def read_coding_table(filepath, delim=','):
    with open(str(filepath)) as handle:
        next(handle)  # skip header line
        return {row[0].strip(): row[1].strip() for row in csv.reader(handle, delimiter=delim)}


def count_matches_from_file(filepath, code_table, delim=None, index=None, start=None, end=None):
    print(f'Reading {filepath}')
    matches, mismatches = 0, 0
    rows = read_data_file(filepath, delim)
    for each_row in rows:
        target_field = extract_field_from_row(
            each_row, index=index, start=start, end=end)
        if is_matched(target_field, code_table):
            matches += 1
        else:
            mismatches += 1

    return matches, mismatches


def iteratively_count_matches(filepaths, *args, **kwargs):
    match_dict = {}
    for each_file in filepaths:
        match_dict[str(each_file)] = count_matches_from_file(each_file, *args, **kwargs)
    return match_dict


def match_report_string(match_name, match_dict):
    report = f'\n{match_name} \n{"="*len(match_name)}\n'
    for each_file in match_dict:
        filename = Path(each_file).name
        matches, mismatches = match_dict[each_file]
        report += f'{filename}: {matches} matches and {mismatches} mismatches\n'
    return report


def main():
    state_code_table = read_coding_table(table_path)
    delim_match = iteratively_count_matches(delim_files, state_code_table, delim=',', index=0)
    undelim_match = iteratively_count_matches(undim_files, state_code_table, start=0, end=3)
    delim_report = match_report_string('Delim file tests', delim_match)
    undelim_report = match_report_string('Undelim file tests', undelim_match)
    print(delim_report)
    print(undelim_report)



if __name__ == '__main__':
    main()
