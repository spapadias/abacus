import os
import re

import numpy as np
import pandas as pd


# Print and select the datasets to alter
def obtainDatasets():
    datasets = []
    folders = {}
    # Generate the list as: __A__ a1 a2 a3 __B__ b1 b2 b3 etc
    for folder in [f.name for f in os.scandir(".") if f.is_dir()]:
        datasets.append("__" + folder.upper() + "__")
        for file in [f.name for f in os.scandir(folder) if f.is_file()]:
            # Exclude those which already have deletions
            if "_alpha=0." not in file:
                datasets.append(file)
                folders[file] = folder

    dictionary = {}
    print("\nWrite the indexes of the datasets (separated by commas)\n")

    matrix = []
    row = []
    num = 1
    maxLen = 0
    for elem in datasets:

        # If it is a header: append the former row to the matrix and create a new one
        if elem[:2] == "__":
            if row:
                matrix.append(row)
                maxLen = max(maxLen, len(row))
            row = [elem]

        # If it is an actual element: append it to the list and create entry in the dictionary
        else:
            row.append(str(num) + ": " + elem)
            dictionary[num] = elem
            num += 1

    matrix.append(row)
    maxLen = max(maxLen, len(row))

    for row in matrix:
        for i in range(len(row), maxLen):
            row.append("")

    transpose = np.transpose(matrix)

    widths = [max(map(len, col)) for col in zip(*transpose)]
    for row in transpose:
        print("  ".join((val.ljust(width) for val, width in zip(row, widths))))

    result = input("\nIf you want to use all of them just press Enter:\n")

    return result, dictionary, folders


# Parse and print the response
def parseInput(inp, dic):
    output = []
    inp = inp.split(",")

    for i in range(len(inp)):

        entry = inp[i].strip()
        entries = entry.split('-')

        if entry != '':

            # If the index is provided, obtain the corresponding value
            if entry.isnumeric():
                entry = int(entry)
                if entry in dic:
                    output.append(dic[entry])
                else:
                    print("\n", entry, " is not the index of any dataset\n", sep="")

            # If a range is provided
            elif len(entries) == 2:
                start = entries[0].strip()
                end = entries[1].strip()

                # Check that it is a valid range, if so add all the methods
                if start.isnumeric() and end.isnumeric() and 0 < int(start) and int(end) <= max(dic.keys()):
                    start = int(start)
                    end = int(end)
                    for key in range(start, end + 1):
                        output.append(dic[key])
                else:
                    print("\n", entry, " is not a valid range\n", sep="")

            # If the input is neither a number or a range
            else:
                print("\n", entry, " is not a number or range\n", sep="")
                continue

    # If the user wanted to use all methods
    if len(output) == 0:
        output = dic.values()

    # Remove repeated values
    output = set(output)
    strOutput = str(output)[1:-1]

    return output, strOutput


def obtainFolder():
    folder = input("Write the location of the data folder from the project root:\n")

    # change '\' to '/'
    folder.replace("\\", "/")

    # remove starting and ending '/'
    if folder[0] == "/":
        folder = folder[1:]

    if folder[-1] == "/":
        folder = folder[:-1]

    os.chdir("../../../" + folder)


def cleanDataset(folder, dataset, stamp):
    # read input file
    with open(folder + "/" + dataset) as f:
        lines = f.readlines()

    # remove initial comments

    while True:
        line = lines[0]
        if '%' in line:
            lines.pop(0)
        else:
            break

    # remove trailing whitespace
    while True:
        line = lines[-1]
        if line.isspace() or line == '':
            lines.pop(-1)
        else:
            break

    # Find the separator: a sequence of non-alphanum chars or spaces (after a sequence of alphanum)
    sep = re.search(r'[\w]+([^\w]+)', lines[0]).group(1)

    lines = [line.strip().split(sep)[:2] for line in lines]

    df = pd.DataFrame(lines)

    # If there is no timestamp shuffle
    if not stamp:
        df = df.sample(frac=1).reset_index(drop=True)


    # make sure that the src and dst cols only hold numbers from [1,Inf)
    for col in [0, 1]:

        # The division can't be made if there are non-digits: remove them
        mask = df[col].str.contains(r'\D')
        if mask.any():
            df.loc[mask, col] = df.loc[mask, col].replace(r'\D+', '', regex=True)

        # If some id's are 0 or less change them to max+1, +2, etc.
        min = int(df[col].min())

        if min < 1:
            max = int(df[col].max()) + 1
            for id in range(min, 1):
                df.loc[df[col] == str(id), col] = max
                max += 1

    # Add the operation column
    df["add"] = 1

    return df


if __name__ == '__main__':

    # Get the data folder of the user and change so we can work there
    obtainFolder()

    # obtain the datasets
    inputDatasets, dictDatasets, folders = obtainDatasets()
    datasets, strDatasets = parseInput(inputDatasets, dictDatasets)
    print("\nThese are the selected datasets:\n", strDatasets)

    timestamps = {}
    for dataset in datasets:

        while True:
            stamp = input("\nDoes " + dataset + " have timestamps?(yes/no):\n")
            stamp = stamp.strip().lower()
            if stamp == 'yes' or stamp == 'no':
                break
        timestamps[dataset] = (stamp == 'yes')

    for dataset in datasets:

        folder = folders[dataset]
        stamp = timestamps[dataset]
        df = cleanDataset(folder, dataset, stamp)

        # Write csv
        output_file = folder + "/" + folder + ".dat"
        df.to_csv(output_file, header=None, index=False, sep=' ', mode='w')
        print(dataset, "done")
