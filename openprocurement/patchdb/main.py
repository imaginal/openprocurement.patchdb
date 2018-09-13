# -*- coding: utf-8 -*-
import sys
import logging
import threading
from .patcher import PatchApp
from .utils import LOG


def main():
    app = PatchApp(sys.argv)
    loglevel = max(
        logging.INFO + 10 * app.args.quiet_count - 10 * app.args.verbose_count,
        logging.DEBUG)
    logformat = '%(asctime)-15s [%(threadName)s] %(levelname)s %(message)s'
    logging.basicConfig(stream=app.args.log, level=loglevel, format=logformat)
    LOG.setLevel(loglevel)
    app.logger = LOG

    if app.args.concurrency > 1:
        LOG.info("Start {} threads...".format(app.args.concurrency))

        threads_list = list()
        modulus = app.args.concurrency
        for remainder in range(modulus):
            thread = threading.Thread(target=app.patch_all,
                                      args=(modulus, remainder))
            threads_list.append(thread)
            thread.daemon = True
            thread.start()

        try:
            for thread in threads_list:
                thread.join()
        except KeyboardInterrupt:
            LOG.error('Program interrupted!')
            app.should_exit = True
            import time
            time.sleep(1)
        finally:
            logging.shutdown()

        app.print_total()
        return

    # else single thread
    try:
        app.patch_all()
    except KeyboardInterrupt:
        LOG.error('Program interrupted!')
    finally:
        logging.shutdown()
    return


if __name__ == '__main__':
    sys.exit(main())
