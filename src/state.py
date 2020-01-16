import random
from abc import ABC
from functools import reduce
from threading import Thread, Event

HEARTBEAT_TIMEOUT = 0.01
ELECTION_TIMEOUT = 0.5, 1.0


class State(ABC):
    def __init__(self, server):
        from server import SurfstoreServer

        self.server: SurfstoreServer = server
        self.majority = self.server.num_servers // 2 + 1

        self.stop_event = Event()

    def stop(self):
        self.stop_event.set()

    def on_AppendEntries(self):
        pass

    def on_RequestVote(self):
        pass


class Follower(State):
    def __init__(self, server):
        super().__init__(server)
        self.received_reponse = False  # Locked by self.server.lock
        Thread(target=self.convert_to_candidate, daemon=True).start()

    @property
    def timeout(self):
        return random.uniform(ELECTION_TIMEOUT[0], ELECTION_TIMEOUT[1])

    def convert_to_candidate(self):
        while not self.stop_event.wait(self.timeout):
            with self.server.lock:
                if not self.received_reponse:
                    print(
                        f'{self.server.id} {self.server.current_term} {self} convert_to_candidate(): become candidate')
                    self.server.transit_state(Candidate)
                    break
            self.received_reponse = False

    def on_AppendEntries(self):
        self.received_reponse = True

    def on_RequestVote(self):
        self.received_reponse = True

    def __repr__(self):
        return "Follower"


class Candidate(State):
    def __init__(self, server):
        super().__init__(server)
        Thread(target=self.elect_leader, daemon=True).start()

    @property
    def timeout(self):
        return random.uniform(ELECTION_TIMEOUT[0], ELECTION_TIMEOUT[1])

    def elect_leader(self):
        while True:
            # preempt response to AppendEntries and RequestVote
            # no dead lock because we do not send RequestVote to self
            # should work because if AppendEntries, RequestVote pending:
            # 1. this round of election should fail
            # 2. after this round of election, timer should be canceled
            with self.server.lock:
                self.server.current_term += 1
                print(f'{self.server.id} {self.server.current_term} {self} elect_leader()')

                # vote for self
                self.server.voted_for = self.server.id
                votes = 1
                latest_term = self.server.current_term
                for server_id, proxy in self.server.proxies.items():
                    try:
                        term, vote_granted = proxy.requestVote(self.server.current_term, self.server.id,
                                                               len(self.server.logs),
                                                               self.server.logs[-1][0] if self.server.logs else 0)
                    except OSError:
                        continue
                    latest_term = max(latest_term, term)
                    votes += vote_granted

                if votes >= self.majority:
                    self.server.transit_state(Leader)
                    break
                elif latest_term > self.server.current_term:
                    self.server.current_term = latest_term
                    self.server.voted_for = None
                    self.server.transit_state(Follower)
                    break
            if self.stop_event.wait(self.timeout):
                break

    def __repr__(self):
        return "Candidate"


class Leader(State):
    def __init__(self, server):
        super().__init__(server)
        self.next_indexes = {server_id: len(self.server.logs) + 1
                             for server_id in server.proxies.keys() if server_id != server.id}
        self.match_indexes = {server_id: 0 for server_id in server.proxies.keys() if server_id != server.id}
        Thread(target=self.append_entry, daemon=True).start()

    @property
    def timeout(self):
        return HEARTBEAT_TIMEOUT

    def append_entry(self):
        while True:
            entries = {server_id: [] for server_id in self.next_indexes.keys()}
            with self.server.lock:
                for server_id, next_index in self.next_indexes.items():
                    # if last log index >= next_index for a follower, call appendEntry RPC
                    if len(self.server.logs) >= next_index:
                        entries[server_id] = self.server.logs[next_index - 1:]
                latest_term = self.server.current_term
                num_up = 1  # count self
                for server_id, proxy in self.server.proxies.items():
                    prev_index = self.next_indexes[server_id] - 1
                    prev_term = self.server.logs[prev_index - 1][0] if prev_index else 0
                    try:
                        term, successful = proxy.appendEntries(self.server.current_term, prev_index, prev_term,
                                                               entries[server_id], self.server.commit_index)
                    except OSError:
                        continue
                    latest_term = max(latest_term, term)
                    # update indexes if succeed, else decrement next_index then retry
                    if successful:
                        self.next_indexes[server_id] = len(self.server.logs) + 1
                        self.match_indexes[server_id] = len(self.server.logs)
                        num_up += 1
                    elif term != -1:
                        self.next_indexes[server_id] -= 1
                        num_up += 1
                self.server.num_up = num_up
                # update commit_index if a log is replicated on majority of servers and is in self.currentTerm
                for follower_commit in range(max(self.match_indexes.values()), self.server.commit_index, -1):
                    num = reduce(lambda n, match_index: n + (follower_commit <= match_index),
                                 self.match_indexes.values(), 0)
                    # leader already append entry, num >= self.majority - 1 is enough
                    if num >= self.majority - 1 and self.server.logs[follower_commit - 1][
                        0] == self.server.current_term:
                        self.server.commit_index = follower_commit
                        break

                if latest_term > self.server.current_term:
                    self.server.current_term = latest_term
                    self.server.transit_state(Follower)
                    break
            if self.stop_event.wait(self.timeout):
                break

    def __repr__(self):
        return "Leader"
