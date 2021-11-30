import numpy as np
import pathpy as pp
from time import process_time
import hypa
import sys
from pathlib import Path

Path("res/HYPA/").mkdir(parents=True, exist_ok=True)
for s in range(101, 121):
    for k in range(2, 6):
        paths = pp.Paths()
        file = 'ArtificialDataHYPA/' + sys.argv[1] + '_' + str(s) + '_' + str(k) + '_' + str(k-1) + '_HYPA.csv'
        time_start = process_time()
        with open(file, 'r') as reader:
            line = reader.readline()
            while line != '':
                splitted = line.split(" Freq: ")
                path = splitted[0].split(" ")
                paths.add_path(path, frequency=int(splitted[1]))
                line = reader.readline()

        hy = hypa.Hypa.from_paths(paths, k=k, implementation='rpy2')
        th = 0.05
        fileOut = 'res/HYPA/' + sys.argv[1] + '_' + str(s) + '_' + str(k) + '_' + str(k-1) + '_HYPA_OUT.txt'
        f = open(fileOut, 'w')
        for edge, edge_data in hy.hypa_net.edges.items():
            val = np.exp(edge_data['pval'])
            if val >= 1 - th or val < th:
                f.write("edge: {} hypa score: {}\n".format(edge, val))
        f.close()
        time_end = process_time()
        print("Total Execution Time in Seconds: ", time_end - time_start)