from fetch import ArxivDl
import argparse
import time
import traceback as tb
from pymongo import MongoClient
import dbUtils



# parser = argparse.ArgumentParser()
# parser.add_argument('--startIndex', type= int ,help='provide index to start from in case your last request was rate limited')
# args = parser.parse_args()
# start_index = 0
#
# if args.startIndex is not None:
#     start_index = args.startIndex
#
# stop = False
#
#
# while not stop:
#     papers = ArxivDl(start_index=start_index)
#     stop,start_index = papers.start()
#     print("\nSleeping")
#     time.sleep(60)

client = None
start_index = 0
papers_to_save = []

while True:
    parser = ArxivDl(start_index=start_index)
    start_index, papers = parser.start()
    papers_to_save.extend(papers)

    time.sleep(600)

    print(f"\nCurrently {len(papers_to_save)} papers fetched.")
    if input(f"Do you want to continue? ") != "n":
        continue
    break


try:
    client = MongoClient('localhost','27017')
    db = client.get_database('arxivdl')
    collection = db['papers']

    result = dbUtils.insert(collection,papers_to_save)
    print(f'Updated {len(result.inserted_ids)} papers to the database')

except Exception:
    tb.print_exc()

finally:
    if client is not None:
        client.close()


