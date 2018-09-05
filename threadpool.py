# -*- coding: UTF-8 -*-
"""
野生线程池的大致原理，在初始化阶段，会根据需求，启动相应数量的线程，这些线程不管有没有任务执行，会存在并且通过阻塞的方式一直到程序结束
当他们有任务的时候，会从任务队列中获取任务，拿到任务的线程会去执行，并在执行完成之后，将结果放入结果队列
"""

# standard library modules
import sys
import threading
import traceback
import queue
from time import time


# exceptions
class NoResultsPending(Exception):
    """All work requests have been processed."""
    pass


class NoWorkersAvailable(Exception):
    """No worker threads available to process remaining requests."""
    pass


# internal module helper functions
def _handle_thread_exception(request, exc_info):
    """Default exception handler callback function.

    This just prints the exception info via ``traceback.print_exception``.

    """
    traceback.print_exception(*exc_info)


# utility functions
def makeRequests(callable_, args_list, callback=None,
                 exc_callback=_handle_thread_exception):
    """Create several work requests for same callable with different arguments.

    Convenience function for creating several work requests for the same
    callable where each invocation of the callable receives different values
    for its arguments.

    ``args_list`` contains the parameters for each invocation of callable.
    Each item in ``args_list`` should be either a 2-item tuple of the list of
    positional arguments and a dictionary of keyword arguments or a single,
    non-tuple argument.

    See docstring for ``WorkRequest`` for info on ``callback`` and
    ``exc_callback``.

    """
    requests = []
    for item in args_list:
        if isinstance(item, tuple):
            requests.append(
                WorkRequest(callable_, item[0], item[1], callback=callback,
                    exc_callback=exc_callback)
            )
        else:
            requests.append(
                WorkRequest(callable_, [item], None, callback=callback,
                    exc_callback=exc_callback)
            )
    return requests


class WorkerThread(threading.Thread):
    """Background thread connected to the requests/results queues.

    A worker thread sits in the background and picks up work requests from
    one queue and puts the results in another until it is dismissed.

    """

    def __init__(self, requests_queue, results_queue, poll_timeout=5, **kwds):
        """Set up thread in daemonic mode and start it immediatedly.

        ``requests_queue`` and ``results_queue`` are instances of
        ``Queue.Queue`` passed by the ``ThreadPool`` class when it creates a
        new worker thread.

        """
        threading.Thread.__init__(self, **kwds)
        self.setDaemon(1)
        self._requests_queue = requests_queue
        self._results_queue = results_queue
        self._poll_timeout = poll_timeout
        self._dismissed = threading.Event()
        self._current_idle_times = 0
        self.start()

    def run(self):
        """Repeatedly process the job queue until told to exit."""
        while True:
            if self._dismissed.isSet():
                # we are dismissed, break out of loop
                break

            # get next work request. If we don't get a new request from the
            # queue after self._poll_timout seconds, we jump to the start of
            # the while loop again, to give the thread a chance to exit.
            try:
                request = self._requests_queue.get(True, self._poll_timeout)
            except Queue.Empty:
                self._current_idle_times += 1
                continue
            else:
                # update current idle times
                self._current_idle_times = 0
                if self._dismissed.isSet():
                    # we are dismissed, put back request in queue and exit loop
                    self._requests_queue.put(request)
                    break
                try:
                    result = request.callable(*request.args, **request.kwds)
                    self._results_queue.put((request, result))
                except:
                    request.exception = True
                    self._results_queue.put((request, sys.exc_info()))

    def idle_time(self):
        if self._poll_timeout <= 0:
            return self._current_idle_times * 5
        return self._current_idle_times * self._poll_timeout

    def dismiss(self):
        """Sets a flag to tell the thread to exit when done with current job.
        """
        self._dismissed.set()


class WorkRequest:
    """A request to execute a callable for putting in the request queue later.

    See the module function ``makeRequests`` for the common case
    where you want to build several ``WorkRequest`` objects for the same
    callable but with different arguments for each call.

    """

    def __init__(self, callable_, args=None, kwds=None, requestID=None,
            callback=None, exc_callback=_handle_thread_exception):
        """Create a work request for a callable and attach callbacks.

        A work request consists of the a callable to be executed by a
        worker thread, a list of positional arguments, a dictionary
        of keyword arguments.

        A ``callback`` function can be specified, that is called when the
        results of the request are picked up from the result queue. It must
        accept two anonymous arguments, the ``WorkRequest`` object and the
        results of the callable, in that order. If you want to pass additional
        information to the callback, just stick it on the request object.

        You can also give custom callback for when an exception occurs with
        the ``exc_callback`` keyword parameter. It should also accept two
        anonymous arguments, the ``WorkRequest`` and a tuple with the exception
        details as returned by ``sys.exc_info()``. The default implementation
        of this callback just prints the exception info via
        ``traceback.print_exception``. If you want no exception handler
        callback, just pass in ``None``.

        ``requestID``, if given, must be hashable since it is used by
        ``ThreadPool`` object to store the results of that work request in a
        dictionary. It defaults to the return value of ``id(self)``.

        """
        if requestID is None:
            self.requestID = id(self)
        else:
            try:
                self.requestID = hash(requestID)
            except TypeError:
                raise TypeError("requestID must be hashable.")
        self.exception = False
        self.callback = callback
        self.exc_callback = exc_callback
        self.callable = callable_
        self.args = args or []
        self.kwds = kwds or {}

    def __str__(self):
        return "<WorkRequest id=%s args=%r kwargs=%r exception=%s>" % \
            (self.requestID, self.args, self.kwds, self.exception)


class ThreadPool:
    """A thread pool, distributing work requests and collecting results.

    See the module docstring for more information.

    """

    def __init__(self, min_workers_num=0, max_workers_num=0, q_size=0, resq_size=0, poll_timeout=5, auto_dismiss_time=300):
        """

        """
        self._requests_queue = Queue.Queue(q_size)
        self._results_queue = Queue.Queue(resq_size)
        self.workers = []
        self.max_workers_num = max_workers_num
        self.dismissedWorkers = []
        self.workRequests = {}
        self.poll_timeout = poll_timeout
        self.create_workers(min_workers_num)
        self._inner_lock = threading.Lock()
        self.auto_dismiss_time = auto_dismiss_time

    def create_workers(self, num_workers):
        """Add num_workers worker threads to the pool.

        ``poll_timeout`` sets the interval in seconds (int or float) for how
        often threads should check whether they are dismissed, while waiting for
        requests.

        """
        for _ in range(num_workers):
            self.workers.append(WorkerThread(self._requests_queue, self._results_queue, poll_timeout=self.poll_timeout))

    def dismiss_idle_worker(self, do_join=False):
        dismiss_list = []
        for i in range(len(self.workers)):
            idle_time = self.workers[i].idle_time()
            if idle_time > self.auto_dismiss_time:
                worker = self.workers.pop(i)
                worker.dismiss()
                dismiss_list.append(worker)

        if do_join:
            for worker in dismiss_list:
                worker.join()

    def submit(self, request, block=True, timeout=None):
        with self._inner_lock:
            """submit work request into work queue and save its id for later."""
            assert isinstance(request, WorkRequest)
            # don't reuse old work requests
            assert not getattr(request, 'exception', None)
            self._requests_queue.put(request, block, timeout)
            self.workRequests[request.requestID] = request
            self.adjust_thread_count()

    def adjust_thread_count(self):
        """

        :return:
        """
        if len(self.workers) < self.max_workers_num:
            self.workers.append(WorkerThread(self._requests_queue, self._results_queue, poll_timeout=poll_timeout))

    def poll(self, block=False):
        """Process any new results in the queue."""
        while True:
            # still results pending?
            if not self.workRequests:
                raise NoResultsPending
            # are there still workers to process remaining requests?
            elif block and not self.workers:
                raise NoWorkersAvailable
            try:
                # get back next results
                request, result = self._results_queue.get(block=block)
                # has an exception occured?
                if request.exception and request.exc_callback:
                    request.exc_callback(request, result)
                # hand results to callback, if any
                if request.callback and not \
                       (request.exception and request.exc_callback):
                    request.callback(request, result)
                del self.workRequests[request.requestID]
            except Queue.Empty:
                break

    def wait(self):
        """Wait for results, blocking until all have arrived."""
        while 1:
            try:
                self.poll(True)
            except NoResultsPending:
                break


if __name__ == '__main__':
    pass
