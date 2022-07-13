import sys, json
from os import listdir
from os.path import isfile, join

his = 25

history_files_path = sys.argv[1]
files = [join(history_files_path, f) for f in listdir(history_files_path) if isfile(join(history_files_path, f))]

for f in files:
    if "trend" in f:
        fd = open(f, "r")
        contents = json.load(fd)
        fd.close()
        new_contents = contents[-his:]
        fd = open(f, "w")
        json.dump(new_contents, fd, indent=2)
        fd.close()
