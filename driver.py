import pickle
from fetch import ArxivDl
import argparse
import time
import traceback as tb
from pymongo import MongoClient
import dbUtils
import sys, select


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
def update_to_db(papers):

    global client
    global backup_papers
    try:
        result = dbUtils.insert(collection, papers)
        if result != -1:
            print(f'Updated {len(result.inserted_ids)} papers to the database')

    except Exception:
        tb.print_exc()
        print("Insert Error. Backing up papers to backup_papers")
        backup_papers.extend(papers)


if __name__ == "__main__":
    client = None
    start_index = 0
    backup_papers = []
    try:
        client = MongoClient('localhost', 27017)
        db = client.get_database('arxivdl')
        collection = db['papers']

        while True:
            parser = ArxivDl(start_index=start_index)
            start_index, papers = parser.start()
            update_to_db(papers)

            time.sleep(600)

            print(f"Currently {len(papers)} papers fetched.")
            print("Do you want to continue?[y/n]")
            i, o, e = select.select([sys.stdin], [], [], 10)
            if i:
                break

    except Exception:
        tb.print_exc()

    finally:
        if client is not None:
            client.close()

        with open('backup_papers.backup', 'wb') as file:
            pickle.dump(backup_papers, file)
