import argparse
import os
from fetch import Arxiv
from timer_utils import Timer
from redis_utils import RQHelper, RedisHelper, PAUSE_SIGNAL, STOP_SIGNAL
from logger_utils import bootstrap_logger


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


def start_driver(set_data, papers_per_call=1000, retries=5, replace_version=True, first_run=False):
    def _get_stop_signal():
        return redis_helper.signal_state(STOP_SIGNAL)

    def _get_pause_signal():
        return redis_helper.signal_state(PAUSE_SIGNAL)

    main_logger.info("Checking for mongo environment variables")
    if not os.environ.get('MONGO_USERNAME', '') or not os.environ.get('MONGO_PASSWORD', ''):
        print(os.environ.items())
        main_logger.error("Mongo DB variables not found in environment variables.")
        return False

    main_logger.info(f"Driver Stats - Papers per call={papers_per_call} :: Retries={retries} :: Replace version={replace_version} :: No of Sets={len(set_data.keys())}")
    try:
        redis_helper = RedisHelper()
        redis_helper.set_signals(PAUSE_SIGNAL, STOP_SIGNAL, value=0)
        if first_run:
            redis_helper.clear_all_keys()

        rq_helper = RQHelper(redis_helper.get_redis_connection())

        timer = Timer(retries=retries, redis_helper=redis_helper)

        arxiv = Arxiv(redis_helper, papers_per_call=papers_per_call,
                      replace_version=replace_version)

        if not redis_helper or not rq_helper:
            raise Exception("Error instantiating Redis/ RQ worker connection")

        index = 0

        for set_num, data in set_data.items():

            main_logger.info(f"Getting data for Set {set_num} :: Categories {data.keys()}")

            while not _get_stop_signal():
                main_logger.info(f"Fetching... index={index} :: papers_per_call={papers_per_call}")

                run_success = arxiv.run_once(data.keys(), index)
                if not run_success:
                    return False

                if _get_pause_signal():
                    main_logger.info("Got empty response. Starting Timer....")

                    rq_helper.enqueue_db_job()

                    if timer.timeout():
                        main_logger.warning("Timer Timeout. Will stop getting data for current set")
                        break
                    timer.start()
                    continue

                index += papers_per_call
                timer.reset()

            rq_helper.enqueue_db_job()

        return True
    except Exception as e:
        main_logger.exception(f"Exception occured while running driver - {e}")
        return False


if __name__ == '__main__':
    papers_per_call = 1000
    replace_version = True
    retries = 5
    first_run = False

    arg_parser = argparse.ArgumentParser(
        description="Get, parse and store arxiv paper metadata")
    arg_parser.add_argument('--papers', action='store', dest='papers_per_call', default=1000,
                            help="Number of papers to be fetched per call")
    arg_parser.add_argument('--retries', metavar='retries', dest='retries', default=5,
                            help="Number of retries before stopping for a set")
    arg_parser.add_argument('--replace', action='store_true', dest='replace_version',
                            help="Replace old version of a paper with a newer version")
    arg_parser.add_argument('--firstrun', action='store_true', dest='first_run',
                            help="Runs the process by clearing all previous data available")

    args = arg_parser.parse_args()
    if args.papers_per_call:
        papers_per_call = args.papers_per_call
    if args.retries:
        retries = args.retries
    if args.replace_version:
        replace_version = args.replace_version
    if args.first_run:
        first_run = args.first_run

    set_data = parse_set_information()
    success = start_driver(set_data, papers_per_call,
                           retries, replace_version, first_run)
    main_logger.info(f"Success = {success}")
