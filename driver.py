from fetch import Parser, InvalidResponse
import pickle
import traceback as tb
from pymongo.bulk import BulkWriteError

categories = {}
with open('categories.txt', 'r') as file:
    data = file.read()
    for category in data.split("\n"):
        categories[category.split("\t")[0]] = category.split("\t")[-1]

parser = None
try:
    for tag, name in categories.items():
        print(f"===================Fetching papers from {name}======================\n")
        parser = Parser(tag)
        parser.start()
        no_of_papers = len(parser.insert_papers) + len(parser.update_papers)
        print(f"Fetched {no_of_papers}. Updating them to database....")
        parser.updateToDb()
        parser.stop()

except InvalidResponse as e:
    print(e)

except BulkWriteError as e:
    print(e.details)

except AssertionError as e:
    print(e)

except Exception:
    tb.print_exc()

finally:
    if parser.updated is False:
        parser.updateToDb()
    parser.stop()

# parser = Parser('cs.ML')
# try:
#     parser.start()
#     no_of_papers = len(parser.insert_papers) + len(parser.update_papers)
#     print(f"Fetched {no_of_papers}. Updating them to database....")
#     parser.updateToDb()
#
# except BulkWriteError as bwe:
#     print(bwe.details)
#
# except AssertionError as e:
#     print(e)
#
# except InvalidResponse as e:
#     print(e)
#
# except Exception:
#     tb.print_exc()
#
# finally:
#     if parser.updated is False:
#         parser.updateToDb()
#     parser.stop()
