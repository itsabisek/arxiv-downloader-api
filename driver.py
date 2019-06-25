from fetch import ArxivDl
import argparse
import time

parser = argparse.ArgumentParser()
parser.add_argument('--startIndex', type= int ,help='provide index to start from in case your last request was rate limited')
args = parser.parse_args()
start_index = 0

if args.startIndex is not None:
    start_index = args.startIndex

stop = False


while not stop:
    papers = ArxivDl(start_index=start_index)
    stop,start_index = papers.start()
    print("\nSleeping")
    time.sleep(60)

