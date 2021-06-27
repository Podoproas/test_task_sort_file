import os
import time
import random
from typing import List


MAX_BATCH_SIZE = 1000            # max number of lines in batch: only that amount of lines can be stored in RAM


def divide_file_on_batches(path: str) -> List[str]:
    """
    This function do divide a big file into some of batches and place them into temp_folder
    :param path: A path to big file to be divided
    :return: A list of new files
    """
    os.makedirs("temp_folder", exist_ok=True)
    batch_counter = 1
    batches = []
    lines = []
    with open(path, "r") as file:
        for line in file:
            lines.append(line)
            if len(lines) == MAX_BATCH_SIZE:
                new_file_name = os.path.join("temp_folder", "batch." + str(batch_counter - 1) + ".txt")
                with open(new_file_name, "w") as batch_file:
                    batch_file.writelines(lines)
                batches.append(new_file_name)
                lines = []
                batch_counter += 1

    if lines:
        new_file_name = os.path.join("temp_folder", "batch." + str(batch_counter - 1) + ".txt")
        with open(new_file_name, "w") as batch_file:
            batch_file.writelines(lines)
        batches.append(new_file_name)

    return batches


def sort_file_inplace(path: str) -> None:
    """
    This function just sorts all the strings in one file
    :param path: Path to a file to be sorted
    :return: None
    """
    with open(path, "r") as fin:
        lines = fin.readlines()
    with open(path, "w") as fout:
        fout.writelines(sorted(lines))


def sort_file(path: str) -> str:
    """
    Sorts file and place in with postfix ".sorted_simple." with bad implementation (in-RAM)
    :param path: Path to a file to be sorted
    :return: New filename
    """
    from shutil import copyfile
    copyfile(path, path[:-4] + ".sorted_simple.txt")
    begin = time.time()
    sort_file_inplace(path[:-4] + ".sorted_simple.txt")
    print(f"Big file sorted simply in {round(time.time() - begin, 3)} seconds")
    return path[:-4] + ".sorted_simple.txt"


def merge(files: List[str], file_num=None) -> str:
    """
    This function receives a list of paths to small batches. It iterates over every 2 files and merge then line by line.
    As they are already sorted: the merge is right.
    :param files: A list of files
    :param file_num: This is used for iterating over batches and creating new sorted files
    :return: filename of a file in which all the data is sorted
    """

    if file_num is None:
        file_num = len(files)

    merged_files = []
    for i in range(0, len(files) - 1, 2):
        with open(files[i]) as file_1:      # First file to merge
            with open(files[i + 1]) as file_2:      # Second file to merge
                merged_name = os.path.join("temp_folder", "batch." + str(file_num) + ".txt")    # New file containing
                file_num += 1                                                                   # merge answer
                merged_files.append(merged_name)
                with open(merged_name, "w") as merged_file:
                    line_1, line_2 = file_1.readline(), file_2.readline()
                    while line_1 and line_2:
                        if line_1 <= line_2:                    # If line from first file is smaller or equal
                            merged_file.writelines([line_1])    # we write it into answer merge file
                            line_1 = file_1.readline()          # read one more line from file_1
                        else:                                   # If line from second file is smaller
                            merged_file.writelines([line_2])    # we write it into answer merge file
                            line_2 = file_2.readline()          # read one more line from file_2

                    while line_1:                               # If second file has ended, but first is not
                        merged_file.writelines([line_1])        # we write all the lines of first file into answer
                        line_1 = file_1.readline()

                    while line_2:                               # If first file has ended, but second is not
                        merged_file.writelines([line_2])        # we write all the lines of second file into answer
                        line_2 = file_2.readline()
        os.remove(files[i])                                     # We remove first (temporary) file
        os.remove(files[i + 1])                                 # We remove second (temporary) file

    if len(files) % 2:                                  # If we couldn't merge all the pairs (one file without a pair)
        merged_files.append(files[-1])                  # we add it to the files to be merged

    if len(merged_files) > 1:                           # If we haven't merged all the files into the only one
        return merge(merged_files, file_num)            # Go deeper into the recursion
    else:
        return merged_files[0]                          # If we merged all files into the only one - we return it's name


def sort_big_file(path: str) -> str:
    """
    This is high-level function that sorts a very big file that couldn't be loaded into RAM.
    :param path: Path to that file
    :return: New filename
    """

    begin = time.time()
    batches = divide_file_on_batches(path)      # Split big file into batches

    for file in batches:
        sort_file_inplace(file)                         # Sort each file

    ans = merge(batches)                        # Merging all the batches

    os.replace(ans, path[:-4] + ".sorted.txt")  # place the sorted file into the directory with postfix ".sorted."
    print(f"Big file sorted in {round(time.time() - begin, 3)} seconds")
    return path[:-4] + ".sorted.txt"


def generate_big_file(name: str, n: int, max_len: int) -> None:
    """
    Function that generates big files.
    :param name: Name of file to be created
    :param n: Num of lines in that file
    :param max_len: Max length of a line
    :return: None
    """

    with open(name, "w") as file:
        begin = time.time()
        for _ in range(n):
            # Next line generates a line of length from [1, max_len] of chars with ord from [33, 126]
            new_line = [chr(random.randint(33, 126)) for _ in range(random.randint(1, max_len))]
            file.write("".join(new_line) + "\n")
        print(f"File generated in {round(time.time() - begin, 3)} seconds")


if __name__ == "__main__":
    # generate_big_file("small_example.txt", 1000, 100)
    generate_big_file("medium_example.txt", 100000, 1000)
    # generate_big_file("big_example.txt", 10000000, 1000)
    sort_big_file("medium_example.txt")
