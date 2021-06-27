import os
import time
import sorting


def clean_up(file):
    os.remove(file)
    os.remove(".".join([file.split(".")[0], "sorted", file.split(".")[1]]))
    os.remove(".".join([file.split(".")[0], "sorted_simple", file.split(".")[1]]))


def test_all(batch_size):
    sorting.MAX_BATCH_SIZE = batch_size

    begin = time.time()
    sorting.generate_big_file("1.txt", batch_size, batch_size)
    assert open(sorting.sort_big_file("1.txt")).readlines() == open(sorting.sort_file("1.txt")).readlines()
    print(f"✓ Test 1 with batch size: {batch_size} passed in {round(time.time() - begin, 3)} seconds")
    clean_up("1.txt")

    begin = time.time()
    sorting.generate_big_file("2.txt", batch_size * 10, batch_size)
    assert open(sorting.sort_big_file("2.txt")).readlines() == open(sorting.sort_file("2.txt")).readlines()
    print(f"✓ Test 2 with batch size: {batch_size} passed in {round(time.time() - begin, 3)} seconds")
    clean_up("2.txt")

    begin = time.time()
    sorting.generate_big_file("3.txt", batch_size, batch_size * 10)
    assert open(sorting.sort_big_file("3.txt")).readlines() == open(sorting.sort_file("3.txt")).readlines()
    print(f"✓ Test 3 with batch size: {batch_size} passed in {round(time.time() - begin, 3)} seconds")
    clean_up("3.txt")

    begin = time.time()
    sorting.generate_big_file("4.txt", batch_size * 100, batch_size * 10)
    assert open(sorting.sort_big_file("4.txt")).readlines() == open(sorting.sort_file("4.txt")).readlines()
    print(f"✓ Test 4 with batch size: {batch_size} passed in {round(time.time() - begin, 3)} seconds")
    clean_up("4.txt")


if __name__ == "__main__":
    test_all(1)
    test_all(10)
    test_all(100)
    print(f"✓✓✓ All tests passed ✓✓✓")
