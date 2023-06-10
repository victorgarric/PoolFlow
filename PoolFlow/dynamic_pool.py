from rich.console import Console
from rich.table import Table
import time
from psutil import virtual_memory as mem
from .utilities import *
from threading import Thread
import sys
class DynamicPool(Thread):
    """
    Creates a pool to submit job and manage tasks based on their estimated costs.
    Pool ends only when user call.

    Examples
    --------
    Setting up the pool
    >>> import PoolFlow as pf
    >>> import os
    >>> import time
    >>> pool = pf.DynamicPool(override_max_value=50) # Maximum value of 50 Go of virtual memory available

    Definition of the job functions
    >>> def test_func(sec): # Making a dummy function example
    >>>     os.system('pythonw')
    >>>     time.sleep(sec)

    Launching pool, adding jobs and waiting for them to end and getting a review of it
    >>> pool.start() # Starting the pool
    >>> for i in range(1,5):
    >>>     pool.submit(test_func, (i,), 20) # Giving jobs to the pool with a cost of 20 Go each
    >>> pool.end() # Wait for all the jobs to terminate
    >>> pool.review() # Synthesize the pool session
                                                                                                       Pool Review
                                                                                               Started - 08/09/22-10:30:24
                                                                                                Ended - 08/09/22-10:30:35
    ┏━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━┓
    ┃ Thread Id ┃ Thread Status ┃ Memory Status ┃ Cost (Go) ┃ Target    ┃ Parameters ┃ Start Date        ┃ End Date          ┃ Running Time (h) ┃
    ┡━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━┩
    │ 1         │ Dead          │ Released      │ 20        │ test_func │ (1,)       │ 08/09/22-10:30:25 │ 08/09/22-10:30:27 │ 0.0              │
    │ 3         │ Dead          │ Released      │ 20        │ test_func │ (3,)       │ 08/09/22-10:30:25 │ 08/09/22-10:30:29 │ 0.0              │
    │ 2         │ Dead          │ Released      │ 20        │ test_func │ (2,)       │ 08/09/22-10:30:27 │ 08/09/22-10:30:30 │ 0.0              │
    │ 4         │ Dead          │ Released      │ 20        │ test_func │ (4,)       │ 08/09/22-10:30:29 │ 08/09/22-10:30:34 │ 0.0              │
    └───────────┴───────────────┴───────────────┴───────────┴───────────┴────────────┴───────────────────┴───────────────────┴──────────────────┘

    Attributes
    ----------
    init_memory : int
        Memory available at the creation of the pool. It can be overrided with override_max_value
    memory : int
        Memory available at the moment
    output : str or None
        The file to write the output. If none, will write on the console. (default is None)
    pool : list
        A list of jobs to be executed
    launched : list
        A list of jobs that have been launched
    idle_time : int
        The time in seconds between each verification by the system of the current resources allocations
        (default is 1)
    refresh_time : int
        The time in seconds between each status emission by the pool (default is 5)
    is_over : bool
        If True, the pool is terminated
    count : int
        The number of jobs submitted
    start_date : datetime object
        The date at which the pool has been created
    end_date : datetime object
        The date at which the pool has been terminated

    """

    def __init__(self, idle_time=1, refresh_time=5, override_max_value=None, output=None, external=False):
        """
        Parameters
        ----------
        idle_time : int, optional
            The time in seconds between each verification by the system of the current resources allocations
            (default is 1)
        refresh_time : int, optional
            The time in seconds between each status emission by the pool (default is 5)
        override_max_value : int or None, optional
            The maximum virtual memory available in the system/for the pool. If None, will get the maximum from the OS
            (default is None)
        output : str or None, optional
            The file to write the output. If none, will write on the console. (default is None)
        external : bool, optional
            If True, will replace command by external python files to run. (default is False)
        """
        super(DynamicPool, self).__init__()
        self.status = None
        if not override_max_value:
            self.init_memory = round(mem().available / 1E9, 0)
        else:
            self.init_memory = override_max_value
        self.output = output
        self.external = external
        self.memory = self.init_memory
        self.pool = []
        self.launched = []
        self.idle_time = idle_time
        self.refresh_time = refresh_time
        self.is_over = False
        self.count = 1
        self.start_date = datetime.datetime.now().strftime("%d/%m/%y-%H:%M:%S")
        self.end_date = ""
        if self.output:
            self.csl = Console(file=open(self.output, 'w'))
        else:
            self.csl = Console()

    def __allocate__(self, calc):
        self.memory += -calc.cost

    def __release__(self, calc):
        self.memory += calc.cost

    def submit(self, target, args, cost, pre_processing=None, post_processing=None):
        """
        Submit a new task to perform the pool

        Parameters
        ----------
        target : fun
            A function containing the task
        args : tuple
            The arguments to be parsed in the function
        cost : int
            The cost of the job in giga octets
        pre_processing : tuple, optional
            A preprocessing function to be called before the running part. Preprocessing cost is assumed to be
            negligible comparing to the job. Preprocessing function are executed before jobs executions and even if no
            virtual memory is available. Preprocessing tuple are given as following: (function, (tuple,with,arguments))
            If None, no preprocessing will be executed. (default is None)
        post_processing : tuple, optional
            A postprocessing function to be called after the running part. Postprocessing cost is assumed to be
            negligible comparing to the job. Postprocessing function are executed after jobs executions and
            memory release. Postprocessing tuple are given as following: (function, (tuple,with,arguments))
            If None, no postprocessing will be executed. (default is None)
        """
        if self.external:
            self.pool.append(Calculation(RawExternalCmd, (args,), cost, self.count, pre_processing, post_processing))
        else:
            self.pool.append(Calculation(target, args, cost, self.count, pre_processing, post_processing))
        self.count += 1

    def empty(self):
        """
        Empty the pool of jobs
        """
        self.pool = []

    def end(self, now=False):
        """
        Ends the pool and wait for it to be dead.

        Parameters
        ----------
        now : bool, optional
            If True, forces the jobs to end (default is False)
        """
        if now:
            if not self.pool:
                self.csl.print('[red]Cannot end pool - Threads are still awaiting. Force with empty method')
        else:
            if not now:
                while self.pool or self.runnings:
                    time.sleep(self.idle_time)
            self.is_over = True
            end = [calc.thread.join() for calc in self.launched]
            self.join()
            time.sleep(self.idle_time)

    def run(self):
        self.status = self.check_status()
        while not self.is_over:
            for calc in self.pool:
                if self.memory > calc.cost and not calc.isrunning and not calc.isdead and calc.ispreprocessed:
                    self.__allocate__(calc)
                    self.launched.append(calc)
                    self.pool.remove(calc)
                    self.launched[-1].start()
                elif not calc.ispreprocessed:
                    calc.launch_pre_processing()
                else:
                    pass
            for calc in self.launched:
                if not calc.isrunning and calc.isdead and not calc.ispostprocessed:
                    calc.launch_post_processing()
                else:
                    pass
            time.sleep(self.idle_time)

    def display_status(self):
        """
        Displays the current status of the pool with running jobs, memory allocation, calculation times and job in the
        queue
        """
        if self.output:
            with open(self.output, 'w') as out:
                out.write('')
        self.csl.print(f'[blue]Pool started - {self.start_date}')
        self.csl.print(f'[bold red] Threading status - {datetime.datetime.now().strftime("%d/%m/%y-%H:%M:%S")}')
        self.csl.print(f'Threads running: {len(self.runnings)}')
        self.csl.print(f'Threads in queue: {len(self.pool)}')
        self.csl.print(f'Threads dead: {len(self.deads)}')
        self.csl.print('[bold blue] Alive threads in pool')
        table = Table()
        table.add_column('Thread Id')
        table.add_column('Cost (Go)')
        table.add_column('Target')
        table.add_column('Parameters')
        table.add_column('Memory Status')
        table.add_column('Start Date')
        if self.external:
            for calc in self.runnings:
                table.add_row(str(calc.id), str(calc.cost), "External File", str(calc.args),
                              MemoryStatus(calc.counted), calc.start_date)
        else:
            for calc in self.runnings:
                table.add_row(str(calc.id), str(calc.cost), str(calc.target.__name__), str(calc.args),
                              MemoryStatus(calc.counted), calc.start_date)
        self.csl.print(table)
        self.csl.print('[bold pink] Memory status')
        self.csl.print(f'Total memory: {self.init_memory} Go\t Available Memory: {self.memory} Go')

    @threaded
    def emit_status(self):
        """
        Execute the display_status method every refresh_time with a time stamp
        """
        while self.is_alive():
            self.display_status()
            sys.stdout.flush()
            time.sleep(self.refresh_time)
        self.display_status()
        self.csl.print(f'[blue]End of pool - {datetime.datetime.now().strftime("%d/%m/%y-%H:%M:%S")}')

    @threaded
    def check_status(self):
        while not self.is_over:
            for calc in self.launched:
                if not calc.thread.is_alive() and not calc.counted:
                    self.__release__(calc)
                    calc.end_date = datetime.datetime.now().strftime("%d/%m/%y-%H:%M:%S")
                    calc.isrunning = False
                    calc.isdead = True
                    calc.counted = True
                else:
                    pass
            time.sleep(self.idle_time)

        self.end_date = datetime.datetime.now().strftime("%d/%m/%y-%H:%M:%S")

    @threaded
    def review(self, awaiting=False):
        """
        Displays a summary of all calculation performed by the pool. It should only be called after when the pool is
        dead (i.e. after the end function is called).

        Parameters
        ----------
        awaiting : bool
            If True, will wait for the pool to end. (default is False)
        """
        if awaiting:
            self.status.join()
            time.sleep(2 * self.idle_time)

        self.csl.print(f'[bold red]Pool Review', justify="center")
        self.csl.print(f'[blue]Started - {self.start_date}', justify='center')
        self.csl.print(f'[blue]Ended - {self.end_date}', justify='center')
        table = Table()
        table.add_column('Thread Id')
        table.add_column('Thread Status')
        table.add_column('Memory Status')
        table.add_column('Cost (Go)')
        table.add_column('Target')
        table.add_column('Parameters')
        table.add_column('Start Date')
        table.add_column('End Date')
        table.add_column('Running Time (h)')
        for calc in (self.pool + self.launched):
            table.add_row(str(calc.id), ThreadStatus(calc.thread.is_alive()), MemoryStatus(calc.counted),
                          str(calc.cost), str(calc.target.__name__), str(calc.args),
                          calc.start_date, calc.end_date, RunningTime(calc.start_date, calc.end_date))
        self.csl.print(table)

    @property
    def runnings(self):
        """
        Returns a list with running jobs
        """
        return [calc for calc in self.launched if calc.isrunning]

    @property
    def deads(self):
        """
        Returns a list of dead jobs
        """
        return [calc for calc in self.launched if calc.isdead]
