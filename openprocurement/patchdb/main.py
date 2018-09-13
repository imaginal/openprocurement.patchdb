# -*- coding: utf-8 -*-
import sys
import time
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

    app.init_app()

    if app.args.concurrency > 1:
        LOG.info("Start {} threads...".format(app.args.concurrency))

        threads_list = list()
        app.lock = threading.Lock()
        modulus = app.args.concurrency
        for remainder in range(modulus):
            thread = threading.Thread(target=app.patch_thread,
                                      args=(modulus, remainder))
            threads_list.append(thread)
            thread.daemon = True
            thread.start()

        try:
            for thread in threads_list:
                thread.join(0.1)
            while sum([t.is_alive() for t in threads_list]):
                time.sleep(0.1)
        except (SystemExit, KeyboardInterrupt):
            LOG.error('Program interrupted!')
            app.has_error = True
            for thread in threads_list:
                thread.join(1)
        finally:
            logging.shutdown()

        app.print_total()

        return app.has_error

    # else single thread
    try:
        app.patch_all()
    except KeyboardInterrupt:
        LOG.error('Program interrupted!')
    finally:
        logging.shutdown()

    return app.has_error


if __name__ == '__main__':
    sys.exit(main())
