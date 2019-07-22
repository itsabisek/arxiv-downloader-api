from fetch_new import Parser, InvalidResponse
import pickle
import traceback as tb
from pymongo.bulk import BulkWriteError

with open('categories', 'rb') as file:
    categories = pickle.load(file)


# categories = {"stat.ME":"Methodology",'stat.ML':"Machine Learning",'stat.TH':"Statistics Theory",'eess.AS':"Audio and Speech Processing",'eess.IV':"Image and Video Processing",'eess.SP':"Signal Processing"}

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
#
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
