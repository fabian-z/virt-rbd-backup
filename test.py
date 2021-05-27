# Example adapter from Python documentationen for module multiprocessing
# https://docs.python.org/3/library/multiprocessing.html

import time
import random

from multiprocessing import Process, Queue, freeze_support

#
# Function run by worker processes
#

def worker(input, output):
    for image in iter(input.get, None):
        result = process_backup(image)
        output.put(result)

def process_backup(image):
    return (True, "No error occured")

#
#
#

def run_parallel():
    NUMBER_OF_PROCESSES = 4
   
    # Create queues
    task_queue = Queue()
    done_queue = Queue()

    # Submit tasks
    for task in TASKS1:
        task_queue.put(task)

    # Start worker processes
    for i in range(NUMBER_OF_PROCESSES):
        Process(target=worker, args=(task_queue, done_queue)).start()

    # Get and print results
    print('Unordered results:')
    for i in range(len(TASKS1)):
        print('\t', done_queue.get())

    # Tell child processes to stop
    for i in range(NUMBER_OF_PROCESSES):
        task_queue.put(None)

if __name__ == '__main__':
    freeze_support()
    run_parallel()