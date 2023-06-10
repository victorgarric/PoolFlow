import datetime
from threading import Thread
import subprocess
import shlex
import os
import warnings
import socketserver
import socket

# Resources management only works on Unix
if os.name == 'posix':
    import resource
else:
    pass


def threaded(fn):
    def wrapper(*args, **kwargs):
        thread = Thread(target=fn, args=args, kwargs=kwargs)
        thread.start()
        return thread

    return wrapper


def ThreadStatus(st):
    if st:
        return "Running"
    else:
        return "Dead"


def MemoryStatus(st):
    if not st:
        return "Allocated"
    else:
        return "Released"


def RunningTime(start, end):
    if end != '/':
        start = datetime.datetime.strptime(start, "%d/%m/%y-%H:%M:%S")
        end = datetime.datetime.strptime(end, "%d/%m/%y-%H:%M:%S")
        return str(round((end - start).days / 24, 3))
    else:
        return '/'


class Calculation:
    """
    An object to launch the jobs, check their status and manage pre and post processing
    Calculation instances are directly created and managed by pool
    - Not meant to be called by users -

    Attributes
    ----------
    target : function
        The function containing the job
    args : tuple
        The arguments of the function to be performed
    cost : int
        The cost of each job in giga octets
    id : int
        The id of the job. Directly managed by the pool.
    pre_processing : tuple or None
        The pre-processing function and its argument to be executed in the form of
        (function, (tuple,with,arguments)). (default is None)
    post_processing : tuple or None
        The post-processing function and its argument to be executed in the form of
        (function, (tuple,with,arguments)). (default is None)
    counted : bool
        An internal flag for pools to manage if Calculation object has been recorded as done
    start_date : datetime object
        The date at which the job has been launched
    end_date : datetime object
        The date at which the job has been ended
    is_done : bool
        An internal flag for pools to manage if Calculation object has been done
    isdead : bool
        An internal flag for pools to manage if job is dead
    ispostprocessed : bool
        An internal flag for pools to manage if post-processing has been executed
    isppreprocessed : bool
        An internal flag for pools to manage if pre-processing has been executed
    isrunning : bool
         An internal flag for pools to manage if job is running
    running_time : datetime
        The time the job has been running
    thread : Thread
        The thread containing the job
    """

    def __init__(self, target, args, cost, id, pre_processing=None, post_processing=None):
        """
        Parameters
        ----------
        target : function
            The function containing the job
        args : tuple
            The arguments of the function to be performed
        cost : int
            The cost of each job in giga octets
        id : int
            The id of the job. Directly managed by the pool.
        pre_processing : tuple or None, optional
            The pre-processing function and its argument to be executed in the form of
            (function, (tuple,with,arguments)). (default is None)
        post_processing : tuple or None, optional
            The post-processing function and its argument to be executed in the form of
            (function, (tuple,with,arguments)). (default is None)
        """
        self.target = target
        self.args = args
        self.id = id
        self.cost = cost
        self.is_done = False
        self.thread = (Thread(target=self.target, args=self.args))
        self.counted = False
        self.start_date = '/'
        self.end_date = '/'
        self.running_time = '/'
        self.isrunning = False
        self.isdead = False
        self.pre_processing = pre_processing
        self.ispreprocessed = False
        self.post_processing = post_processing
        self.ispostprocessed = False

    def start(self):
        """
        Launches the calculation
        """
        self.thread.start()
        self.start_date = datetime.datetime.now().strftime("%d/%m/%y-%H:%M:%S")
        self.isrunning = True

    @threaded
    def launch_pre_processing(self):
        """
        Launches the pre-processing and flag it as performed
        """
        if self.pre_processing:
            th = Thread(target=self.pre_processing[0], args=self.pre_processing[1])
            th.start()
            th.join()
        else:
            pass
        self.ispreprocessed = True

    @threaded
    def launch_post_processing(self):
        """
        Launches the post-processing and flag it as performed
        """
        if self.post_processing:
            th = Thread(target=self.post_processing[0], args=self.post_processing[1])
            th.start()
            th.join()
        else:
            pass
        self.ispostprocessed = True


class LimitedProcess:
    """
    An object to use inside the creation of job functions to limit the usage of virtual memory when using a command
    Only available on Unix platforms

    Attributes
    ----------
    command : str
        The command to execute as it would be written in a command line
    limit : int
        The memory limit to use in giga octets
    ishard : bool
        If True, the limit would not be exceeded even if the job is to crash because of memory limit
    """

    def __init__(self, command, limit, ishard=False):
        """
        Parameters
        ----------
        command : str
            The command to execute as it would be written in a command line
        limit : int
            The memory limit to use in giga octets
        ishard : bool, optional
            If True, the limit would not be exceeded even if the job is to crash because of memory limit
        """
        self.command = command
        self.limit = limit * 1E3 * 1024 * 1024
        self.ishard = ishard

    def launch(self):
        """
        Launches the command with the given limits
        """
        if os.name == 'posix':
            subprocess.run(shlex.split(self.command), preexec_fn=self.limit_virtual_memory)
        else:
            subprocess.run(shlex.split(self.command))
            warnings.warn(f'Platform {os.name} does not support resources limitations')

    def limit_virtual_memory(self):
        """
        The limiter function
        """
        if os.name == 'posix':
            if self.ishard:
                resource.setrlimit(resource.RLIMIT_AS, (self.limit, resource.RLIM_INFINITY))
            else:
                resource.setrlimit(resource.RLIMIT_AS, (self.limit, self.limit))
        else:
            warnings.warn(f'Platform {os.name} does not support resources limitations')


class UnlimitedProcess:
    """
    An object to use inside the creation of job functions to set no limit to the usage of virtual memory
    when using a command
    Only available on Unix platforms

    Attributes
    ----------
    command : str
        The command to execute as it would be written in a command line
    """

    def __init__(self, command):
        self.command = command

    def launch(self):
        """
        Launches the command with the given limits
        """
        if os.name == 'posix':
            subprocess.run(shlex.split(self.command), preexec_fn=self.limit_virtual_memory)
        else:
            subprocess.run(shlex.split(self.command))
            warnings.warn(f'Platform {os.name} does not support resources limitations')

    @staticmethod
    def limit_virtual_memory():
        """
        The limiter function
        """
        if os.name == 'posix':
            resource.setrlimit(resource.RLIMIT_AS, (-1, -1))
        else:
            warnings.warn(f'Platform {os.name} does not support resources limitations')


class TCPHandler(socketserver.StreamRequestHandler):
    """
    The request handler class for our server.

    It is instantiated once per connection to the server, and must
    override the handle() method to implement communication to the
    client.
    """

    def handle(self):
        self.data = self.rfile.readline().strip()
        self.server.data.append([str(self.data)[2:-1], False])


def SubmitToServer(data, HOST='localhost', PORT=7455):
    """
    Submit a calculation to the local pool server

    Examples
    --------
    >>> from PoolFlow.utilities import SubmitToServer as smbv
    >>> smbv(('C:\\Users\\Myself\\Documents\\File.py', '1')) #Sumbit file with 1Go cost
    """
    data = '!@!'.join(data)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((HOST, PORT))
        sock.sendall(bytes(data + "\n", "utf-8"))


def RawExternalCmd(file):
    """
    Used by DynamicPool for ServerPool instance purposes
    """
    return subprocess.run(['python', file])
