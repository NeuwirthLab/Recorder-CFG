import sys
with open(sys.argv[1]) as f:
    lines = f.readlines()
    for line in lines:
        if "nc_" in line:
            print(line.replace("\n",""))
