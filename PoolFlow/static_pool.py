from rich.console import Console
from rich.table import Table
import time
from psutil import virtual_memory as mem
from threading import Thread
from .utilities import *
import sys

class StaticPool(Thread):
    """
    Creates a pool to submit job and manage tasks based on their estimated costs. Jobs are given at the invocation
    of the instance and ends when all jobs are processed.

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

    Methods
    -------
    allocate(calculation)
        Virtually allocate an amount of memory based on the calculation cost
         - Not meant to be called by the user -
    release(self, calculation)
        Virtually release an amount of memory based on the calculation cost
         - Not meant to be called by the user -
    end(now=False)
        Ends the pool and wait for it to be dead
    run()
        The running function of the pool. Is called by start method
         - Not meant to be called by the user -
    display_status()
        Displays the current status of the pool with running jobs, memory allocation, calculation times and job in the
        queue
    emit_status()
        Execute the display_status method every refresh_time with a time stamp
    check_status()
        A thread that check the current status and health of the pool
        - Not meant to be called by the user -
    review(awaiting=False)
        Displays a summary of all calculation performed by the pool. It should only be called after when the pool is
        dead (i.e. after the end function is called).
    """

    def __init__(self, target, args, cost=20, idle_time=1, refresh_time=5, override_max_value=None,
                 output=None):
        """
        Parameters
        ----------
        target : function
            The function containing the jobs to be performed
        args : list of tuples
            A list of tuples containing the arguments to be parsed. For instance, if n jobs of the function have to be
            executed with k arguments for the function, a list of n tuples, each containing k arguments should be given
        cost : int
            The cost of each job in giga octets
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
        """
        super(StaticPool, self).__init__()
        self.status = None
        if not override_max_value:
            self.init_memory = round(mem().available / 1E9, 0)
        else:
            self.init_memory = override_max_value
        self.output = output
        self.memory = self.init_memory
        self.cost = cost
        self.args = args
        self.threads = []
        self.idle_time = idle_time
        self.refresh_time = refresh_time
        self.len = len(args)
        self.where = 0
        self.target = target
        self.is_over = False
        self.start_date = datetime.datetime.now().strftime("%d/%m/%y-%H:%M:%S")
        self.end_date = ""
        self.pool = []
        self.launched = []
        self.count = 0
        for arg in self.args:
            self.count += 1
            self.pool.append(Calculation(target, arg, self.cost, self.count))
        if self.output:
            self.csl = Console(file=open(self.output, 'w'))
        else:
            self.csl = Console()

    def allocate(self, calc):
        """
        Virtually allocate an amount of memory based on the calculation cost
         - Not meant to be called by the user -

        Parameters
        ----------
        calc : a Calculation object
        """
        self.memory += -calc.cost

    def release(self, calc):
        """
        Virtually release an amount of memory based on the calculation cost
         - Not meant to be called by the user -

        Parameters
        ----------
        calc : a Calculation object
        """
        self.memory += calc.cost

    def run(self):
        """
        The running function of the pool. Is called by start method
         - Not meant to be called by the user -
        """
        self.status = self.check_status()
        while not self.is_over:
            for calc in self.pool:
                if self.memory > calc.cost and calc.ispreprocessed:
                    self.allocate(calc)
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
        """
        A thread that check the current status and health of the pool
        - Not meant to be called by the user -
        """
        while not self.is_over:
            for calc in self.launched:
                if not calc.thread.is_alive() and not calc.counted and calc.isrunning:
                    self.release(calc)
                    calc.end_date = datetime.datetime.now().strftime("%d/%m/%y-%H:%M:%S")
                    calc.isrunning = False
                    calc.isdead = True
                    calc.counted = True
                elif self.pool == [] and self.runnings == [] and len(self.deads) == self.count:
                    self.is_over = True
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
            time.sleep(3 * self.idle_time)

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

    @property
    def runnings(self):
        """
        Returns a list with running jobs
        - Not meant to be called by the user -
        """
        return [calc for calc in self.launched if calc.isrunning]

    @property
    def deads(self):
        """
        Returns a list of dead jobs
        - Not meant to be called by the user -
        """
        return [calc for calc in self.launched if calc.isdead]
