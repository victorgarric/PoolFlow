from .dynamic_pool import *
from threading import Thread


class ServerPool(Thread):
    """
    ServerPool is meant to be reached by any other Python terminal using the SumbitToServer function from PoolFlow.utilities.
    It is currently highly experimental !

    Examples
    --------
    Setting up the pool
    >>> import PoolFlow as pf
    >>> import os
    >>> import time
    >>> pool = pf.ServerPool(override_max_value=20, output='test.log')

    In any other instance:
    >>> from PoolFlow.utilities import SubmitToServer as smbv
    >>> smbv(('C:\\Users\\Myself\\Documents\\File.py', '1')) #Sumbit file with 1Go cost
    """

    def __init__(self, idle_time=1, refresh_time=5, override_max_value=None, output='/var/log/ServerPool.log'):
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
        """
        super(ServerPool, self).__init__()
        self.pool = DynamicPool(idle_time, refresh_time, override_max_value, output, external=True)
        self.pool.start()
        self.pool.emit_status()
        self.idle_time = idle_time
        self.refresh_time = refresh_time
        self.output = output

    def run(self):
        """
        The running function of the pool. Is called by start method
         - Not meant to be called by the user -
        """
        self.__start_server__()
        while not self.pool.is_over:
            time.sleep(self.idle_time)
            for element in self.server.data:
                if element[1] == True:
                    pass
                else:
                    args = element[0].split('!@!')
                    if len(args) == 2:
                        self.pool.submit(None, args[0], int(args[1]))
                    elif len(args) == 4:
                        self.pool.submit(None, args[0], int(args[1]), args[2], args[3])
                    else:
                        pass
                    element[1] = True

    def status(self):
        """
        Writes the current status in output file
        """
        self.pool.output = None
        self.pool.display_status()
        self.pool.output = self.output

    @threaded
    def __start_server__(self):
        with socketserver.TCPServer(("localhost", 7455), TCPHandler) as self.server:
            self.server.data = []
            self.server.serve_forever()

    def stop(self):
        """
        Ends server
        """
        self.pool.end(now=True)
        self.pool.review()
        self.server.shutdown()
