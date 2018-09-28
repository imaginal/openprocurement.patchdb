# -*- coding: utf-8 -*-
import sys
import time
import logging
import threading
import multiprocessing
from .patcher import PatchApp
from .utils import LOG


def main():
    app = PatchApp(sys.argv)
    loglevel = max(
        logging.INFO + 10 * app.args.quiet_count - 10 * app.args.verbose_count,
        logging.DEBUG)
    logformat = '%(asctime)-15s [%(threadName)s] %(levelname)s %(message)s'
    if app.args.processes > 1:
        logformat = '%(asctime)-15s [%(processName)s:%(process)d] %(levelname)s %(message)s'
    logging.basicConfig(stream=app.args.log, level=loglevel, format=logformat)
    LOG.setLevel(loglevel)
    app.logger = LOG

    app.init_app()

    if app.args.processes > 1:
        LOG.info("Start {} processes...".format(app.args.processes))

        processes_list = list()
        modulus = app.args.processes
        shared_stat = multiprocessing.Array('i', 4 * modulus, lock=False)
        is_alive = 0
        for remainder in range(modulus):
            process_name = "Process-{}".format(remainder + 1)
            process = multiprocessing.Process(target=app.patch_process,
                                              args=(modulus, remainder, shared_stat),
                                              name=process_name)
            processes_list.append(process)
            process.daemon = True
            process.start()
            is_alive += 1

        try:
            while is_alive:
                is_alive = 0
                time.sleep(0.01)
                for p in processes_list:
                    if p.is_alive():
                        is_alive += 1
                        p.join(0.01)
                    elif p.exitcode:
                        LOG.error("{}:{} exitcode {}".format(p.name, p.pid, p.exitcode))
                        raise RuntimeError("Abort by child")
        except (RuntimeError, SystemExit, KeyboardInterrupt) as e:
            LOG.error("{} {}".format(type(e).__name__, e))
            app.has_error = True
            for process in processes_list:
                if p.is_alive():
                    process.terminate()
            for process in processes_list:
                if p.is_alive():
                    process.join(1)
        finally:
            app.update_stat(shared_stat, size=modulus)
            app.print_total()
            logging.shutdown()


    elif app.args.concurrency > 1:
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
                time.sleep(0.01)
        except (SystemExit, KeyboardInterrupt):
            LOG.error('Program interrupted!')
            app.has_error = True
            for thread in threads_list:
                thread.join(1)
        finally:
            app.print_total()
            logging.shutdown()

    else:  # single thread
        try:
            app.patch_all()
        except KeyboardInterrupt:
            LOG.error('Program interrupted!')
        finally:
            app.print_total()
            logging.shutdown()

    return app.has_error


if __name__ == '__main__':
    sys.exit(main())
