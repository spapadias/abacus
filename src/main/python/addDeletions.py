import math
import os
import random

import numpy as np
import pandas as pd


# Input parameters: input file and deletion ratio

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


# Returns whether the ratio is valid or not
def validRatio(ratio):
    try:
        ratio = float(ratio)
    except ValueError:
        print("\n", ratio, " is not a valid ratio\n", sep="")
        return False

    if 1.0 < ratio or ratio <= 0.0:
        print("\n", ratio, " is not a valid ratio\n", sep="")
        return False

    return True


# Parses input ratios
def obtainValidRatios(inputRatios):
    ratios = []
    inputRatios = inputRatios.split(",")

    for i in range(len(inputRatios)):

        ratio = inputRatios[i].strip()
        valid, skipped = False, False

        while (not valid):

            # If we skipped the current ratio and (there are valid ratios or remaining possible ratios)
            if ratio == "" and (len(ratios) != 0 or i != len(inputRatios) - 1):
                valid, skipped = True, True
                continue

            valid = validRatio(ratio)
            if valid:
                ratio = float(ratio)

            else:
                ratio = input("\nPlease insert a valid ratio, or skip with enter:\n").strip()

        # To avoid repetitions and skipped values
        if not skipped and ratio not in ratios:
            ratios.append(ratio)

    ratios.sort()

    return set(ratios)


def addDeletions(dataset, ratio, folder):
    input_file = folder + "/" + dataset

    # Create name for output file
    name_type = dataset.split(".", 1)
    name = name_type[0]
    type = name_type[1]
    new_name = name + '_alpha=' + str(ratio) + '.' + type
    output_file = folder + "/" + new_name

    if os.path.isfile(output_file):
        print("File", new_name, "already exists\n")
        return

    # read input file
    df = pd.read_csv(input_file, sep=' ', names=['src', 'dst', 'add'])

    # They are all additions
    df["index"] = df.index

    # Select n entries to create their deletions (don't select the same item twice)
    n_deletions = math.floor(len(df) * ratio)
    deletions = df.sample(n_deletions, replace=False)
    deletions["add"] = -1

    # Assign to each deletion a random position in [index+1, end]
    deletions["new_pos"] = 0
    total_length = len(df) + n_deletions
    for i in deletions.index:
        deletions.at[i, "new_pos"] = random.randint(deletions["index"][i] + 1, total_length - 1)

    # Merge both df's using the column "index" to reorder to the right positions
    deletions["index"] = deletions["new_pos"]
    deletions = deletions[["src", "dst", "add", "index"]]
    df = df.append(deletions)

    df = df.sort_values("index")
    df = df[["src", "dst", "add"]]

    # Write the result as a txt
    df.to_csv(output_file, header=None, index=False, sep=' ', mode='w')


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


if __name__ == '__main__':

    # Get the data folder of the user and change so we can work there
    obtainFolder()

    # obtain the datasets
    inputDatasets, dictDatasets, folders = obtainDatasets()
    datasets, strDatasets = parseInput(inputDatasets, dictDatasets)
    print("These are the selected datasets:\n", strDatasets, "\n")

    # obtain the deletion ratios
    inputRatios = input("\nWrite the ratios of edge deletions (separated by commas)\n")
    ratios = obtainValidRatios(inputRatios)
    print("\nThese are the selected ratios:\n", str(ratios)[1:len(str(ratios)) - 1], "\n")

    for dataset in datasets:
        folder = folders[dataset]
        print("\n", dataset, sep='')
        for ratio in ratios:
            addDeletions(dataset, ratio, folder)
            print("*", ratio, "done")
