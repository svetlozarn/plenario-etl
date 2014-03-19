import pandas as pd
from pandas import Series, DataFrame
import argparse, string
import numpy as np

parser = argparse.ArgumentParser()

parser.add_argument('csv', nargs=1,
                    help='path of csv file to preprocess')
parser.add_argument('-d', '--dedup', dest='dedup', action='store_true',
                    help='output deduplicated file and duplicates file')
parser.add_argument('-c', '--cols', dest='cols', nargs='+', type=str, default=None,
                    help='specify names of columns to consider for identifying duplicates (all by default)')
parser.add_argument('-l', '--length', dest='length', action='store_true',
                    help='find range of string lengths in columns')

args = parser.parse_args()

split = args.csv[0].rsplit('/', 1)
if len(split) > 1: path, inFile = split[0] + "/", split[1]
else: path, inFile = "./", split[0]
outFileDup = "dup_" + inFile
outFileDedup = "dedup_" + inFile

tmp = pd.read_csv(args.csv[0], dtype=object, iterator=True, chunksize=10000)
df = pd.concat([chunk for chunk in tmp], ignore_index=True)

if "chicago-environmental-complaints" in inFile:
    df['LOCATION'] = map(lambda x: x.splitlines()[-1]
                          if type(x) == str else np.nan, df['MAPPED LOCATION'])
    df['LATITUDE'] = map(lambda x: x.strip('()').rsplit(', ')[0]
                          if not pd.isnull(x) else np.nan, df['LOCATION'])
    df['LONGITUDE'] = map(lambda x: x.strip('()').rsplit(', ')[1]
                           if not pd.isnull(x) else np.nan, df['LOCATION'])

if args.dedup==True:
    dup = df.duplicated(cols=args.cols)
    dfDup = df[dup == True]
    dfDedup = df[dup == False]
    dfDup.to_csv(outFileDup, index=False)
    dfDedup.to_csv(outFileDedup, index=False)

if args.length==True:
    if args.cols == None:
        args.cols = df.columns
    for c in args.cols:
        print c+": ["+str(len(min(map(lambda x: str(x), df[c]), key=len)))+", "+str(len(max(map(lambda x: str(x), df[c]), key=len)))+"]"
        #print "Longest: "+max(df[c].astype(str), key=len)
