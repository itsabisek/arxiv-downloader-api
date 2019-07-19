from fetch_new import Parser
import pickle
import traceback as tb

with open('categories', 'rb') as file:
    categories = pickle.load(file)
try:
    for tag, name in categories.items():
        print(f"Fetching papers from {name}\n")
        parser = Parser(tag)
        parser.start()
        no_of_papers = len(parser.papers_in_db)
        print(f"Fetched {no_of_papers}. Updating them to database....")
        parser.updateToDb()


except Exception:
    tb.print_exc()

finally:
    parser.stop()

# parser = Parser('cs.AI')
# try:
#     parser.start()
#     no_of_papers = len(parser.papers_in_db)
#     print(f"Fetched {no_of_papers}. Updating them to database....")
#     parser.updateToDb()

# except Exception:
#     tb.print_exc()

# finally:
#     parser.stop()
