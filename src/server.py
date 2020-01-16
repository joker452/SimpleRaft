import argparse
import http.client
import socket
from socketserver import ThreadingMixIn
from threading import Lock
from xmlrpc.client import ServerProxy, Transport
from xmlrpc.server import SimpleXMLRPCRequestHandler
from xmlrpc.server import SimpleXMLRPCServer

from state import State, Follower, Leader
from surfstore import SurfStore


class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)


class ThreadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    pass


class TimeoutTransport(Transport):
    def __init__(self):
        super().__init__(False, True)

    def make_connection(self, host):
        if self._connection and host == self._connection[0]:
            return self._connection[1]
        # create a HTTP connection object from a host descriptor
        chost, self._extra_headers, x509 = self.get_host_info(host)
        self._connection = host, http.client.HTTPConnection(chost, timeout=0.01)
        return self._connection[1]


class SurfstoreServer:
    def __init__(self, proxies, id, num_servers):
        self.surfstore = SurfStore()
        self.file_info_lock = Lock()
        self.num_servers = num_servers  # num_servers is known even when proxies is None
        self.proxies = proxies
        self.id = id
        self.current_term = 0
        self.voted_for = None  # None as null
        self.logs = []  # [(term, cmd)], 1-indexed in paper
        self.commit_index = 0
        self.last_applied = 0
        self.lock = Lock()
        # self.time_out = CHECK_TIMEOUT
        self.num_up = 1
        self.is_crashed = True
        self.state: State = None
        self.crash()  # crashed by default

    def _dispatch(self, method, params):
        # workaround for autograder call methods as surfstore.*
        func_name = method.split(".")[-1]
        return getattr(self, func_name)(*params)

    @staticmethod
    def set_up_connections(server_list, id):
        proxies = {}
        for server_id, (host, port) in enumerate(server_list):
            if server_id != id:  # remove itself
                host = socket.gethostbyname(host)  # localhost is slow on Windows
                proxies[server_id] = ServerProxy(f'http://{host}:{port}', transport=TimeoutTransport())
        return proxies

    def transit_state(self, StateClass):
        """
        Transit to the next state
        Assume calling thread acquired self.lock
        """
        if self.state is not None:
            self.state.stop()

        if StateClass is None:
            self.state = None
        else:
            print(f'{self.id} {self.current_term} {self.state} transit_state(): to {StateClass.__name__}')
            self.state = StateClass(self)

    def isLeader(self):
        """
        Queries whether this metadata store is a leader
        Note that this call should work even when the server is "crashed"
        """
        return isinstance(self.state, Leader)

    def crash(self):
        """
        Crashes this metadata store
        Until Restore() is called, the server should reply to all RPCs
        with an error (unless indicated otherwise), and shouldn't send
        RPCs to other servers
        """
        print(f'{self.id} {self.current_term} {self.state} crash()')
        with self.lock:
            self.is_crashed = True
            self.transit_state(None)
        return True

    def restore(self):
        """
        Restores this metadata store, allowing it to start responding
        to and sending RPCs to other nodes
        """
        print(f'{self.id} {self.current_term} {self.state} restore()')
        with self.lock:
            self.is_crashed = False
            self.transit_state(Follower)
        return True

    def isCrashed(self):
        """
        IsCrashed returns the status of this metadata node (crashed or not)
        This method should always work, even when the node is crashed
        """
        with self.lock:
            return self.is_crashed

    def requestVote(self, term, candidate_id, log_index, log_term):
        """
        Requests vote from this server to become the leader
        """
        if not self.lock.acquire(blocking=False):
            return -1, False
        if self.is_crashed:
            self.lock.release()
            return -1, False
        self.state.on_RequestVote()
        print(f'{self.id} {self.current_term} {self.state} requestVote(): from {candidate_id}')
        if not self.__check_term(term):
            self.lock.release()
            return self.current_term, False
        # If votedFor is null or candidateId, and candidate’s log is at
        # least as up-to-date as receiver’s log, grant vote
        is_leader = self.voted_for is None or self.voted_for == candidate_id
        up_to_date = (self.logs[-1][0] if self.logs else 0, len(self.logs)) <= (log_term, log_index)
        vote_granted = is_leader and up_to_date
        if vote_granted:
            self.voted_for = candidate_id
            print(f'{self.id} {self.current_term} {self.state} requestVote(): voted for {self.voted_for}')
        self.lock.release()
        return term, vote_granted

    def appendEntries(self, term, prev_index, prev_term, entries, leader_commit):
        """Updates fileinfomap to match that of the leader"""
        if not self.lock.acquire(blocking=False):
            return -1, False  # TODO: avoid deadlock by presumably crashed
        if self.is_crashed:
            self.lock.release()
            return -1, False
        self.state.on_AppendEntries()
        if not self.__check_term(term):
            self.lock.release()
            return self.current_term, False
        if entries:
            if len(self.logs) < prev_index or (prev_index > 0 and self.logs[prev_index - 1][0] != prev_term):
                return self.current_term, False
            for index, entry in enumerate(entries, prev_index):
                if index < len(self.logs) and self.logs[index][0] != entry[0]:
                    del self.logs[index:]  # if conflicts, delete
                    break
            self.logs[prev_index:] = entries  # append new entries

        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, len(self.logs))
        # apply committed cmds, commit_index and last_applied are log entry index, both of them are 1-indexed
        if self.commit_index > self.last_applied:
            for idx in range(self.last_applied, self.commit_index):
                self.surfstore.updatefile(*self.logs[idx][1])
            self.last_applied = self.commit_index
        self.lock.release()
        return self.current_term, True

    def __check_term(self, term):
        """
        Check the term of the request is valid, update self.current_term if necessary
        """
        if term < self.current_term:
            return False
        elif term > self.current_term:
            self.current_term = term
            # If a candidate or leader discovers that its term is out of date, reverts to follower state.
            self.voted_for = None
            self.transit_state(Follower)
        return True

    def getfileinfomap(self):
        # contact majority of nodes before reply to readonly request
        while self.isLeader() and self.num_up < self.num_servers // 2 + 1:
            pass
        if self.isLeader():
            with self.file_info_lock:
                return self.surfstore.getfileinfomap()
        raise Exception("isCrashed or is not Leader")

    def updatefile(self, filename, version, blocklist):
        if self.isLeader():
            with self.lock:
                self.logs.append((self.current_term, (filename, version, blocklist)))  # store params only
            pending_index = len(self.logs)
            while self.isLeader() and self.commit_index < pending_index:
                pass
            if self.isLeader():
                self.last_applied = pending_index
                with self.file_info_lock:
                    return self.surfstore.updatefile(filename, version, blocklist)
        raise Exception("isCrashed or is not Leader")

    def tester_getversion(self, filename):
        with self.file_info_lock:
            return self.surfstore.file_infos[filename][0]


def readconfig(config):
    """Reads cofig file"""
    with open(config, 'r') as fd:
        maxnum = int(fd.readline().strip().split(' ')[1])

        server_list = []
        for i, line in enumerate(fd):
            hostport = line.strip().split(' ')[1].split(':')
            server_list.append((hostport[0], int(hostport[1])))

    return server_list, maxnum


def main():
    parser = argparse.ArgumentParser(description="SurfStore server")
    parser.add_argument('config', help='path to config file')
    parser.add_argument('server_num', type=int, help='server number')
    args = parser.parse_args()
    config = args.config
    server_num = args.server_num
    server_list, _ = readconfig(config)

    print("Attempting to start XML-RPC Server...")
    with ThreadedXMLRPCServer(server_list[server_num],
                              requestHandler=RequestHandler, use_builtin_types=True, logRequests=False) as server:
        server.register_introspection_functions()
        surfstore = SurfstoreServer(SurfstoreServer.set_up_connections(server_list, server_num), server_num,
                                    len(server_list))
        server.register_instance(surfstore)
        surfstore.restore()

        print("Started successfully.")
        print("Accepting requests. (Halt program to stop.)")
        server.serve_forever()


if __name__ == "__main__":
    main()
