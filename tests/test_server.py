import os
import time
import unittest
from hashlib import sha256
from threading import Thread

from src.server import SurfstoreServer

LEADER_ELECTION_TIMEOUT = 2
LOG_REPLICATION_TIMEOUT = 0.02
BLOCK_TIMEOUT = 10  # how long consider block


def start_server(servers, index):
    servers[index].proxies = {k: v for k, v in servers.items() if k != index}
    servers[index].restore()


def get_state_info(servers):
    followers, candidates, leaders, crashed = [], [], [], []
    for i, surfstore in servers.items():
        with surfstore.lock:
            if repr(surfstore.state) == 'Leader':
                leaders.append(i)
            elif repr(surfstore.state) == 'Follower':
                followers.append(i)
            elif repr(surfstore.state) == 'Candidate':
                candidates.append(i)
            else:
                crashed.append(i)
    return followers, candidates, leaders, crashed


class StrawmanTest(unittest.TestCase):
    def setUp(self) -> None:
        self.N = 5  # number of servers
        self.majority = self.N // 2 + 1
        self.surfstores = {i: SurfstoreServer({}, i, self.N) for i in range(self.N)}

    def tearDown(self) -> None:
        for server in self.surfstores.values():
            server.crash()
        del self.surfstores

    def start_server(self, index):
        start_server(self.surfstores, index)

    def get_state_info(self):
        return get_state_info(self.surfstores)

    # @unittest.skip
    def test_start_one_by_one(self):
        """ Strawman test in the spec part1"""

        # start first server, it should become candidate after a while
        self.start_server(0)
        time.sleep(LEADER_ELECTION_TIMEOUT)

        self.assertEqual("Candidate", repr(self.surfstores[0].state))

        # start second server, neither of them can become leader
        self.start_server(1)
        time.sleep(LEADER_ELECTION_TIMEOUT)
        for surfstore in self.surfstores.values():
            self.assertNotEqual(repr(surfstore.state), "Leader")

        # start third server, one of them can become leader
        self.start_server(2)
        time.sleep(LEADER_ELECTION_TIMEOUT)
        followers, _, leaders, _ = self.get_state_info()

        self.assertEqual(len(leaders), 1)
        self.assertEqual(len(followers), 2)

        # start the remaining servers, leader should not change, new servers should become followers
        self.start_server(3)
        self.start_server(4)

        time.sleep(LEADER_ELECTION_TIMEOUT)
        followers, _, cur_leaders, _ = self.get_state_info()

        self.assertEqual(len(cur_leaders), 1)
        self.assertEqual(cur_leaders, leaders)
        self.assertEqual(len(followers), 4)

    # @unittest.skip
    def test_crash_one_by_one(self):
        """Strawman test in spec part2"""
        for i in range(self.N):
            self.start_server(i)

        time.sleep(LEADER_ELECTION_TIMEOUT)
        # record the first leader
        followers, _, leaders, _ = self.get_state_info()
        self.assertEqual(len(leaders), 1)
        self.assertEqual(len(followers), 4)
        leader_id = leaders[0]

        # kill leader one by one, up servers >= majority
        for i in range(self.majority - 1):
            self.surfstores[leader_id].crash()
            time.sleep(LEADER_ELECTION_TIMEOUT)
            followers, _, leaders, _ = self.get_state_info()
            self.assertEqual(len(leaders), 1)
            self.assertEqual(len(followers), 3 - i)
            cur_leader = leaders[0]
            self.assertNotEqual(cur_leader, leader_id)
            leader_id = cur_leader

        # kill the last leader, servers < majority
        self.surfstores[leader_id].crash()
        time.sleep(LEADER_ELECTION_TIMEOUT)
        followers, candidates, leaders, _ = self.get_state_info()
        self.assertEqual(len(leaders), 0)


def get_info_map(files, block_size):
    info_map = {}
    for name, (ver, bs) in files.items():
        info_map[name] = [ver, []]
        for i in range(0, len(bs), block_size):
            b = bs[i: i + block_size]
            h = sha256(b).digest()
            info_map[name][1].append(h)
    return info_map


class TestServerAlone(unittest.TestCase):
    def setUp(self) -> None:
        self.N = 5  # number of servers
        self.majority = self.N // 2 + 1
        self.surfstores = {i: SurfstoreServer({}, i, self.N) for i in range(self.N)}

    def tearDown(self) -> None:
        for server in self.surfstores.values():
            server.crash()
        del self.surfstores

    def start_server(self, index):
        start_server(self.surfstores, index)

    def get_state_info(self):
        return get_state_info(self.surfstores)

    # @unittest.skip
    def test_blocks_majority_down(self):
        """Server should block when majority is not available"""

        def foo():
            files = {'lala.bin': [1, os.urandom(10000)]}
            info_map = get_info_map(files, 4096)
            # should block here
            self.surfstores[leader_id].updatefile('lala.bin', info_map['lala.bin'][0], info_map['lala.bin'][1])

        # start majority of servers
        for i in range(self.N // 2 + 1):
            self.start_server(i)

        # elect a leader
        time.sleep(LEADER_ELECTION_TIMEOUT)
        followers, _, leaders, _ = self.get_state_info()
        self.assertEqual(len(leaders), 1)
        self.assertEqual(len(followers), self.N // 2)
        leader_id = leaders[0]
        # crash a follower
        self.surfstores[followers[0]].crash()
        t = Thread(target=foo)
        t.start()
        # sleep 2 minutes
        time.sleep(BLOCK_TIMEOUT)
        self.assertTrue(t.isAlive())
        # crash leader
        self.surfstores[leader_id].crash()
        t.join()

    # @unittest.skip
    def test_leader_updatefile(self):
        """Only leader can call updatefile"""
        for i in range(self.N):
            self.start_server(i)
        # elect a leader
        time.sleep(LEADER_ELECTION_TIMEOUT)
        followers, _, leaders, _ = self.get_state_info()
        self.assertEqual(len(leaders), 1)
        self.assertEqual(len(followers), 4)
        leader_id = leaders[0]

        files = {'lala.bin': [1, os.urandom(10000)]}
        info_map = get_info_map(files, 4096)

        # all servers except leader should raise Exception
        for server_id, surfstore in self.surfstores.items():
            if server_id != leader_id:
                with self.assertRaises(Exception):
                    surfstore.updatefile('lala.bin', info_map['lala.bin'][0], info_map['lala.bin'][1])
            else:
                self.assertTrue(surfstore.updatefile('lala.bin', info_map['lala.bin'][0], info_map['lala.bin'][1]))

        self.assertEqual(self.surfstores[leader_id].getfileinfomap(), info_map)

    # @unittest.skip
    def test_follower_crash_and_restore(self):
        """Followers' version should update if they restore after crash"""
        # start servers, one more than majority
        for i in range(self.N // 2 + 2):
            self.start_server(i)
        # elect a leader
        time.sleep(LEADER_ELECTION_TIMEOUT)
        followers, _, leaders, _ = self.get_state_info()
        self.assertEqual(len(leaders), 1)
        self.assertEqual(len(followers), self.N // 2 + 1)
        leader_id = leaders[0]

        files = {'lala.bin': [1, os.urandom(10000)]}
        info_map = get_info_map(files, 4096)

        # updatefile on leader
        self.assertTrue(self.surfstores[leader_id].updatefile('lala.bin', info_map['lala.bin'][0],
                                                              info_map['lala.bin'][1]))
        time.sleep(LOG_REPLICATION_TIMEOUT)
        # crash a follower
        self.surfstores[followers[0]].crash()

        files = {'lala.bin': [2, os.urandom(10000)],
                 'lala2.bin': [1, os.urandom(10000)]}
        info_map = get_info_map(files, 4096)

        # updatefile on leader
        for file_name, info in info_map.items():
            self.assertTrue(self.surfstores[leader_id].updatefile(file_name, info[0], info[1]))

        self.surfstores[followers[0]].restore()
        for i in range(self.N // 2 + 2, self.N):
            self.start_server(i)

        # time to send appendentries
        time.sleep(LOG_REPLICATION_TIMEOUT)
        # all servers should have up-to-date version, call tester_getversion on followers to avoid Exception
        for server_id, surfstore in self.surfstores.items():
            if server_id != leader_id:
                for file_name, info in info_map.items():
                    self.assertEqual(self.surfstores[server_id].tester_getversion(file_name),
                                     info_map[file_name][0])
            else:
                self.assertEqual(self.surfstores[leader_id].getfileinfomap(), info_map)

    # @unittest.skip
    def test_follower_updatefile(self):
        """Followers' file version should update after leader commit"""
        for i in range(self.N):
            self.start_server(i)
        # elect a leader
        time.sleep(LEADER_ELECTION_TIMEOUT)
        followers, _, leaders, _ = self.get_state_info()
        self.assertEqual(len(leaders), 1)
        self.assertEqual(len(followers), 4)
        leader_id = leaders[0]

        files = {'lala.bin': [1, os.urandom(10000)]}
        info_map = get_info_map(files, 4096)

        # all servers except leader should raise Exception
        for server_id, surfstore in self.surfstores.items():
            if server_id != leader_id:
                with self.assertRaises(Exception):
                    surfstore.updatefile('lala.bin', info_map['lala.bin'][0], info_map['lala.bin'][1])
            else:
                self.assertTrue(surfstore.updatefile('lala.bin', info_map['lala.bin'][0], info_map['lala.bin'][1]))
        # time to send appendentries
        time.sleep(LOG_REPLICATION_TIMEOUT)
        # all servers should have up-to-date version, call tester_getversion on followers to avoid Exception
        for server_id, surfstore in self.surfstores.items():
            if server_id != leader_id:
                self.assertEqual(self.surfstores[server_id].tester_getversion('lala.bin'), info_map['lala.bin'][0])
            else:
                self.assertEqual(self.surfstores[leader_id].getfileinfomap(), info_map)


if __name__ == '__main__':
    unittest.main()
