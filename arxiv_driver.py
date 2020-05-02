from fetch import fetch_wrapper, parse_wrapper, get_parsed_response
from db_utils import get_db_handle, bulk_insert_or_update
from timer_utils import Timer
from redis_utils import RQHelper, RedisHelper
from redis import Redis
from logger_utils import bootstrap_logger
import argparse

main_logger = bootstrap_logger(__name__)


def parse_set_information():
    with open('categories.txt', 'r') as file:
        set_data = {}
        contents = file.read()
        contents = contents.strip().split("#")
        for content in contents:
            if not content:
                continue
            set_split = content.strip().split('\n')
            _set_no = int(set_split[0])
            _set_data = {category_data.strip().split('\t')[0]: category_data.strip().split(
                '\t')[1] for category_data in set_split[1:]}
            set_data[_set_no] = _set_data

        return set_data


def enque_parse_db_job(rq_helper, response_buffer, replace_version):
    if response_buffer:
        main_logger.info(
            f"Found {len(response_buffer)} reponses to parse. Will enque them all")
        parse_results = rq_helper.enqueue_new_job(
            parse_wrapper, response_buffer, replace_version=replace_version)

        with get_db_handle() as db_handle:
            main_logger.info(
                f"Enqueing db update call for parsed_results - {len(parse_results.result)} results")

            db_result = rq_helper.enqueue_new_job(
                bulk_insert_or_update, depends_on=parse_results, commit_buffer=parse_results.result)


def start_driver(set_data, papers_per_call=1000, retries=5, replace_version=True):
    main_logger.info(
        f"Driver Stats - Papers per call={papers_per_call} :: Retries={retries} :: Replace version={replace_version} :: No of Sets={len(set_data.keys())}")
    try:
        redis_helper = RedisHelper()
        rq_helper = RQHelper(redis_helper.get_redis_connection())
        if not redis_helper or not rq_helper:
            main_logger.error("Redis / RQ worker could not be instantiated")
            raise Exception("Error instantiating Redis connection")
        start_index = 0

        for set_num, data in set_data.items():
            timer = Timer()
            response_buffer = []
            main_logger.info(
                f"Getting data for Set {set_num} :: Categories {data.values()}")
            while not timer.timeout():
                main_logger.info(
                    f"Fetching... index={start_index} :: papers_per_call={papers_per_call}")
                parsed_response, pause_fetch, stop_fetch = fetch_wrapper(
                    data.keys(), start_index, papers_per_call)
                if pause_fetch:
                    main_logger.info("Got empty response ")
                    enque_parse_db_job(rq_helper,
                                       response_buffer, replace_version)
                    if timer.timeout():
                        main_logger.warn(
                            "Timer Timedout. Will stop getting data for current set")
                        break
                    timer.start()
                    continue
                if stop_fetch:
                    main_logger.warn(f"All papers for set {set_num} fetched.")
                    timer.set_timeout()

                response_buffer.append(parsed_response)
                start_index += papers_per_call

            enque_parse_db_job(rq_helper,
                               response_buffer, replace_version)
        return True
    except Exception as e:
        main_logger.exception(f"Exception occured while running driver - {e}")
        return False


if __name__ == '__main__':
    papers_per_call = 1000
    replace_version = True
    retries = 5

    arg_parser = argparse.ArgumentParser(
        description="Get, parse and store arxiv paper metadata")
    arg_parser.add_argument('--papers', action='store', dest='papers_per_call', default=1000,
                            help="Number of papers to be fetched per call")
    arg_parser.add_argument('--retries', metavar='retries', dest='retries', default=5,
                            help="Number of retries before stopping for a set")
    arg_parser.add_argument('--replace', action='store_true', dest='replace_version',
                            help="Replace old version of a paper with a newer version")

    args = arg_parser.parse_args()
    if args.papers_per_call:
        papers_per_call = args.papers_per_call
    if args.retries:
        retries = args.retries
    if args.replace_version:
        replace_version = args.replace_version

    set_data = parse_set_information()
    success = start_driver(set_data, papers_per_call, retries, replace_version)
    print(f"Success = {success}")
