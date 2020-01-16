import os
import unittest
from hashlib import sha256
from src.surfstore import SurfStore


class TestServerAlone(unittest.TestCase):
    """
    Test server basic functionality without RPC and client
    """

    def setUp(self) -> None:

        self.server = SurfStore()

    def test_get_set_blocks(self):
        b = os.urandom(4096)
        self.server.putblock(b)
        self.assertEqual(b, self.server.getblock(sha256(b).digest()))

    def test_info(self):
        infos = self.server.getfileinfomap()
        self.assertEqual(infos, {})
        file_name = 'lala.txt'
        infos = {file_name:
                     [1,
                      [sha256(os.urandom(4096)).digest(),
                       sha256(os.urandom(4096)).digest(),
                       sha256(os.urandom(4096)).digest()]]}
        self.server.updatefile(file_name, infos[file_name][0], infos[file_name][1])
        self.assertEqual(infos, self.server.getfileinfomap())


if __name__ == '__main__':
    unittest.main()
