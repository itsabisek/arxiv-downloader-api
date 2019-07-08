import pickle
from fetch import ArxivDl
import dns
import time
import traceback as tb
from pymongo import MongoClient
import dbUtils
import sys, select

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
        client = MongoClient('mongodb+srv://abisekmishra:<password>@cluster0-fiaze.mongodb.net/test?retryWrites=true&w=majority')
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
        if len(backup_papers) != 0:
            with open('backup_papers.backup', 'wb') as file:
                pickle.dump(backup_papers, file)
