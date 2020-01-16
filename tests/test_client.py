import os
import shutil
import tempfile
import unittest
from hashlib import sha256

from src.client import SurfstoreClient
from src.server import SurfstoreServer


def versioned_files_to_index(files, block_size):
    index = ''
    for name, (ver, bs) in files.items():
        hashes = []
        for i in range(0, len(bs), block_size):
            b = bs[i: i + block_size]
            hashes.append(sha256(b).digest())
        hashes = ' '.join(h.hex() for h in hashes) if hashes else '0'
        index += f'{name} {ver} {hashes}\n'
    return index


def versioned_files_to_server(files, block_size):
    server = SurfstoreServer()
    for name, (ver, bs) in files.items():
        server.file_infos[name] = [ver, []]
        for i in range(0, len(bs), block_size):
            b = bs[i: i + block_size]
            h = sha256(b).digest()
            server.blocks[h] = b
            server.file_infos[name][1].append(h)
    return server


def server_to_versioned_files(server):
    files = {}
    for name, (ver, hs) in server.file_infos.items():
        files[name] = [ver, b'']
        for h in hs:
            files[name][1] += server.blocks[h]
    return files


def files_to_folder(folder, files):
    for name, bs in files.items():
        assert bs, "Empty file not allowed"
        with open(os.path.join(folder, name), 'bw') as f:
            f.write(bs)


def folder_to_files(folder):
    files = {}
    for name in os.listdir(folder):
        if name == 'index.txt':
            continue
        with open(os.path.join(folder, name), 'rb') as f:
            files[name] = f.read()
    return files


class TestClientAlone(unittest.TestCase):
    """
    Test client's functionality without server or RPC.
    """

    def setUp(self) -> None:
        self.block_size = 4096
        # create a temporary directory
        self.base_dir = tempfile.mkdtemp()
        self.index_txt = os.path.join(self.base_dir, 'index.txt')

    def test_read_update_index(self):
        files = {'lala.bin': [1, [os.urandom(256) for _ in range(10)]],
                 'lala2.bin': [2, [os.urandom(256) for _ in range(5)]]}

        client = SurfstoreClient(None, self.base_dir, self.block_size)
        client.update_index(files)
        self.assertEqual(client.read_index(), files)

    def test_scan_base(self):
        client = SurfstoreClient(None, self.base_dir, self.block_size)
        files = {'lala.bin': os.urandom(10000),
                 'lala2.bin': os.urandom(10000)}
        files_to_folder(self.base_dir, files)
        open(self.index_txt, 'w').close()  # empty index.txt

        # create a subdirectory, client should ignore this
        sub_dir = "sub"
        os.mkdir(os.path.join(self.base_dir, sub_dir))
        for n, bs in files.items():
            with open(os.path.join(self.base_dir, sub_dir, n), 'bw') as f:
                f.write(bs)

        base_infos = client.scan_base()

        for f, infomap in base_infos.items():
            self.assertIn(f, files)
            self.assertEqual(None, infomap[0])
            for h in infomap[1]:
                self.assertIs(type(h), bytes)

    def tearDown(self) -> None:
        # remove tmp directory after test
        shutil.rmtree(self.base_dir)


class TestClientWithServer(unittest.TestCase):
    """
    Test client's functionality together with server.
    Assume client handles each file independently
    Test name is test_{server compared to index}__{base compared to index}
    """

    def setUp(self) -> None:
        self.block_size = 4096
        # create a temporary directory
        self.base_dir = tempfile.mkdtemp()
        self.index_txt = os.path.join(self.base_dir, 'index.txt')

    def test_identical_identical(self):
        files = {'lala.bin': [1, os.urandom(10000)],  # exists
                 'lala2.bin': [2, b'']}  # deleted

        server = versioned_files_to_server(files, self.block_size)
        files_to_folder(self.base_dir, {n: bs for n, (_, bs) in files.items() if bs})
        index_str = versioned_files_to_index(files, self.block_size)
        with open(self.index_txt, 'w') as f:
            f.write(index_str)

        client = SurfstoreClient(server, self.base_dir, self.block_size)
        client.run()

        # nothing should change
        self.assertEqual(files, server_to_versioned_files(server))
        with open(self.index_txt) as f:
            self.assertEqual(index_str, f.read())
        self.assertEqual({n: bs for n, (_, bs) in files.items() if bs}, folder_to_files(self.base_dir))

    def test_identical_deleted(self):
        files = {'lala.bin': [1, os.urandom(10000)]}

        server = versioned_files_to_server(files, self.block_size)
        files_to_folder(self.base_dir, {})
        with open(self.index_txt, 'w') as f:
            f.write(versioned_files_to_index(files, self.block_size))

        client = SurfstoreClient(server, self.base_dir, self.block_size)
        client.run()

        files = {'lala.bin': [2, b'']}
        self.assertEqual(files, server_to_versioned_files(server))
        self.assertEqual({}, folder_to_files(self.base_dir))
        with open(self.index_txt) as f:
            self.assertEqual(versioned_files_to_index(files, self.block_size), f.read())

    def test_identical_modified(self):
        files = {'lala.bin': [1, os.urandom(10000)],  # exists
                 'lala1.bin': [2, b'']}  # deleted
        new_files = {'lala.bin': [2, os.urandom(10000)], 'lala1.bin': [3, os.urandom(10000)]}

        server = versioned_files_to_server(files, self.block_size)
        with open(self.index_txt, 'w') as f:
            f.write(versioned_files_to_index(files, self.block_size))
        files_to_folder(self.base_dir, {n: bs for n, (_, bs) in new_files.items()})

        client = SurfstoreClient(server, self.base_dir, self.block_size)
        client.run()

        self.assertEqual(new_files, server_to_versioned_files(server))
        self.assertEqual({n: bs for n, (_, bs) in new_files.items()}, folder_to_files(self.base_dir))
        with open(self.index_txt) as f:
            self.assertEqual(versioned_files_to_index(new_files, self.block_size), f.read())

    def test_modified_whatever(self):
        # index holds old versions
        index_files = {'lala.bin': [1, os.urandom(10000)], 'lala1.bin': [1, os.urandom(10000)],
                       'lala2.bin': [1, os.urandom(10000)]}
        # sever holds new versions
        server_files = {'lala.bin': [2, os.urandom(10000)], 'lala1.bin': [2, os.urandom(10000)],
                        'lala2.bin': [2, os.urandom(10000)]}
        # base holds whatever
        base_files = {'lala.bin': index_files['lala.bin'][1],  # identical
                      'lala1.bin': os.urandom(10000),  # modified
                      'lala2.bin': b''}  # deleted

        server = versioned_files_to_server(server_files, self.block_size)
        with open(self.index_txt, 'w') as f:
            f.write(versioned_files_to_index(index_files, self.block_size))
        files_to_folder(self.base_dir, {n: b for n, b in base_files.items() if b})

        client = SurfstoreClient(server, self.base_dir, self.block_size)
        client.run()

        self.assertEqual(server_files, server_to_versioned_files(server))
        self.assertEqual({n: bs for n, (_, bs) in server_files.items()}, folder_to_files(self.base_dir))
        with open(self.index_txt) as f:
            self.assertEqual(versioned_files_to_index(server_files, self.block_size), f.read())

    def test_deleted_whatever(self):
        # index holds old versions
        index_files = {'lala.bin': [1, os.urandom(10000)], 'lala1.bin': [1, os.urandom(10000)],
                       'lala2.bin': [1, os.urandom(10000)]}
        # sever deletes them
        server_files = {'lala.bin': [2, b''], 'lala1.bin': [2, b''], 'lala2.bin': [2, b'']}
        # base holds whatever
        base_files = {'lala.bin': index_files['lala.bin'][1],  # identical
                      'lala1.bin': os.urandom(10000),  # modified
                      'lala2.bin': b''}  # deleted

        server = versioned_files_to_server(server_files, self.block_size)
        with open(self.index_txt, 'w') as f:
            f.write(versioned_files_to_index(index_files, self.block_size))
        files_to_folder(self.base_dir, {n: b for n, b in base_files.items() if b})

        client = SurfstoreClient(server, self.base_dir, self.block_size)
        client.run()

        self.assertEqual(server_files, server_to_versioned_files(server))
        self.assertEqual({}, folder_to_files(self.base_dir))
        with open(self.index_txt) as f:
            self.assertEqual(versioned_files_to_index(server_files, self.block_size), f.read())

    def test_nonexistent_created(self):
        files = {}
        new_files = {'lala.bin': [1, os.urandom(10000)]}

        server = versioned_files_to_server(files, self.block_size)
        with open(self.index_txt, 'w') as f:
            f.write(versioned_files_to_index(files, self.block_size))
        files_to_folder(self.base_dir, {n: bs for n, (_, bs) in new_files.items()})

        client = SurfstoreClient(server, self.base_dir, self.block_size)
        client.run()

        self.assertEqual(new_files, server_to_versioned_files(server))
        self.assertEqual({n: bs for n, (_, bs) in new_files.items()}, folder_to_files(self.base_dir))
        with open(self.index_txt) as f:
            self.assertEqual(versioned_files_to_index(new_files, self.block_size), f.read())

    def test_created_nonexistent(self):
        files = {}
        new_files = {'lala.bin': [1, os.urandom(10000)]}

        server = versioned_files_to_server(new_files, self.block_size)
        with open(self.index_txt, 'w') as f:
            f.write(versioned_files_to_index(files, self.block_size))

        client = SurfstoreClient(server, self.base_dir, self.block_size)
        client.run()
        self.assertEqual(new_files, server_to_versioned_files(server))
        self.assertEqual({n: bs for n, (_, bs) in new_files.items()}, folder_to_files(self.base_dir))
        with open(self.index_txt) as f:
            self.assertEqual(versioned_files_to_index(new_files, self.block_size), f.read())

    def tearDown(self) -> None:
        # remove tmp directory after test
        shutil.rmtree(self.base_dir)


if __name__ == '__main__':
    unittest.main()
