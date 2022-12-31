import os.path
import subprocess
from subprocess import STDOUT, PIPE

import numpy as np

# Print and select the methods/actions/datasets to test
def obtainInput(list, type):
    dictionary = {}
    print("Write the indexes of the", type, "(separated by commas)\n")

    matrix = []
    row = []
    num = 1
    maxLen = 0;
    for elem in list:
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

    result = input("\nIf you want to use all of them just press Enter\n")

    return result, dictionary


# Parse and print the response
def parseInput(inp, dic, typ):
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
                    print("\n", entry, " is not the index of any ", typ, "\n", sep="")

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


# Returns whether the memory budget is valid or not
def validMemoryBudget(budget):
    try:
        budget = int(budget)    # budget should be an integer
    except ValueError:
        print("\n", ratio, " is not a valid memory budget\n", sep="")
        return False

    return True

# Parses input memory budget
def obtainValidMemoryBudget(inputMemoryBudgets):
    budgets = []
    inputMemBudgets = inputMemoryBudgets.split(",")

    for i in range(len(inputMemBudgets)):
        budget = inputMemBudgets[i].strip()
        valid, skipped = False, False

        while (not valid):
            # If we skipped the current ratio and (there are valid ratios or remaining possible ratios)
            if budget == "" and (len(ratios) != 0 or i != len(inputMemBudgets) - 1):
                valid, skipped = True, True
                continue

            valid = validMemoryBudget(budget)
            if valid:
                budget = int(budget)
            else:
                budget = input("\nPlease insert a valid memory budget, or skip with enter:\n").strip()

        # To avoid repetitions and skipped values
        if not skipped and budget not in budgets: budgets.append(str(budget))

    budgets.sort()

    return budgets


# Method to compile java file
def compile_java(java_files):
    cmd = 'javac -cp "*" ' + java_files    # "*" is needed for all OSs
    subprocess.check_call(cmd, shell=True)


# Method to execute java file
def execute_java(java_file, budget, method, dataset, action, step, threads, seed, minibatchsize):
    sep = ":"
    if os.name == 'nt':
        sep = ";"

    # Run java_file at '.' current directory & search at '../../../' for all '*' JAR files
    cmd = 'java -cp ".' + sep + '../../../*" ' + java_file + ' ' + budget + ' ' + method + ' ' + dataset + ' ' + action + ' ' + step + ' ' + threads + ' ' + seed + ' ' + minibatchsize

    proc = subprocess.Popen(cmd, stdout=PIPE, stderr=STDOUT, shell=True)
    stdout, stderr = proc.communicate()

    output = stdout.decode("utf-8")
    print(output)


# Method to execute python scripts
def execute_python(arguments):
    cmd = arguments
    cmd[0] = pythonFiles + cmd[0]
    cmd.insert(0, 'python3')

    proc = subprocess.Popen(cmd, stdout=PIPE, stderr=STDOUT)
    stdout, stderr = proc.communicate()

    output = stdout.decode("utf-8")
    # print(output)
    if not output.isspace() and len(output) != 0:
        print(output)

def getFile(dataset, budget, method, action):
    dataset = dataset.replace(".dat", "") # take out the .dat from the dataset name
    return dataFiles + dataset + "/budget=" + budget + "/" + method + "/" + action + ".txt"

def obtainFolder():
    folder = input("Write the location of the data folder from the project root:\n")

    # change '\' to '/'
    folder.replace("\\", "/")

    # remove starting and ending '/'
    if folder[0] == "/":
        folder = folder[1:]

    if folder[-1] == "/":
        folder = folder[:-1]

    return "../../../" + folder


def obtainDatasets(dir):
    datasets = []
    # Generate the list as: __A__ a1 a2 a3 __B__ b1 b2 b3 etc
    for folder in [f.name for f in os.scandir(dir) if f.is_dir()]:
        datasets.append("__" + folder.upper() + "__")
        for file in [f.name for f in os.scandir(dir + "/" + folder) if f.is_file()]:
            # Exclude those which already have deletions
                datasets.append(file)

    return datasets


if __name__ == '__main__':
    # obtain the datasets
    dir = obtainFolder()
    allDataSets = obtainDatasets(dir)
    inputDatasets, dictDatasets = obtainInput(allDataSets, "datasets")
    datasets, strDatasets = parseInput(inputDatasets, dictDatasets, "dataset")
    print("These are the selected datasets:\n", strDatasets, "\n")

    # obtain the memory budget k
    inputMemoryBudget = input("\nWrite the memory budgets (separated by commas)\n")
    budgets = obtainValidMemoryBudget(inputMemoryBudget)
    print("\nThese are the selected memory budgets:\n", str(budgets)[1:len(str(budgets)) - 1], "\n")

    # obtain the step (every how many edges we will write the counts)
    step = input("\nWrite the step for writing the butterfly counts: ")
    print("\nWe write butterfly counts every ", step, "edges\n")

    # how many threads to use?
    threads = input("\nHow many threads to utilize: ")
    print("\nWe use ", threads, "threads\n")

    # what is the minibatch size?
    miniBatchSize = input("\nMinibatch size: ")
    print("\nWe use ", miniBatchSize, "as the minibatch size\n")

    # choose random seed
    seed = input("\nWhich random seed to use?: ")
    print("\nWe use ", seed, "as random seed\n")

    # Files locations at the time of use
    javaMain = "BCD/ParabacusExperiments"   # Main class in the java project (for Abacus exps)
    javaFiles = "src/main/java/BCD/*.java"  # All java classes that may be required to run javaMain
    dataFiles = "../../../output_abacus/"   # The directory where the output from javaMain can be found
    pythonFiles = "../python/"              # The directory where the python scripts can be found
    # Compilation
    os.chdir("../../../")                   # Go to root directory, assuming in folder with the python scripts
    compile_java(javaFiles)                 # Compile javaFiles given the jarFiles
    os.chdir("src/main/java/")              # Go to the java directory
    print("\nCompilation finished successfully")

    for dataset in datasets:
        for budget in budgets:
            file2 = getFile(dataset, budget, "perEdgeButterflyCountingSmart", "Accuracy")
            if not os.path.exists(file2):
                execute_java(javaMain, budget, "perEdgeButterflyCountingSmart", dataset, "Accuracy", step, threads, seed, miniBatchSize)
            continue