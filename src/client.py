import argparse
import os
import xmlrpc.client
from hashlib import sha256


class SurfstoreClient:
    def __init__(self, server, base_dir, block_size):
        self.server = server
        self.base_dir = base_dir
        self.block_size = block_size
        self.file_blocks = {}  # {file_name: blocks}

        os.makedirs(self.base_dir, exist_ok=True)

    def download(self, file_name):
        """
        Get server's fileinfomap first, then download all its blocks to self.files
        :param file_name: name of the file download from server
        """
        remote_infos = self.get_fileinfomap()
        if remote_infos[file_name][1]:  # don't download deleted file
            with open(os.path.join(self.base_dir, file_name), 'wb') as f:
                for h in remote_infos[file_name][1]:
                    f.write(self.server.getblock(h))

    def delete(self, file_name, version):
        """
        Delete a file on server
        :param file_name: name of the file to delete
        """
        self.server.updatefile(file_name, version, [])

    def upload(self, file_name, file_info):
        """
        Upload a new version of file to server
        :param file_name: name of the file to upload
        :param file_info: infomap of the new file
        :return: True if succeed otherwise False
        """
        # hash of blocks already on the server
        hash_server = set(self.server.hasblocks(file_info[1]))
        for i in range(len(file_info[1])):
            if not file_info[1][i] in hash_server:
                # use index to get corresponding block
                self.server.putblock(self.file_blocks[file_name][i])
        return self.server.updatefile(file_name, file_info[0], file_info[1])

    def read_index(self):
        """
        Read index.txt, if not existent, return empty dict
        :return: dict {file_name: [version, [blocks' hash]]} or {}
        """
        index = {}

        index_path = os.path.join(self.base_dir, 'index.txt')
        if not os.path.exists(index_path):  # create if not exists
            open(index_path, 'w').close()

        with open(index_path, 'r') as f:
            for line in f:
                name, ver, *hs = line.split()
                hs = [bytes.fromhex(h) for h in hs] if hs != ['0'] else []
                index[name] = [int(ver), hs]
        return index

    def scan_base(self):
        """
        Scan base dir, calculate hashlist for each file, use placeholder 1 for version
        Populate files with file block
        :return: dict {file_name: [version, [blocks' hash]]}
        """
        base_infos = {}
        for name in os.listdir(self.base_dir):
            if name == 'index.txt':  # skip index
                continue
            path = os.path.join(self.base_dir, name)
            if not os.path.isfile(path):
                continue  # skip directory
            if os.path.getsize(path) == 0:
                continue  # skip empty file
            infomap = [None, []]  # None as version number placeholder
            self.file_blocks[name] = []
            with open(path, 'rb') as f:
                while True:
                    block = f.read(self.block_size)
                    if not block:
                        break
                    infomap[1].append(sha256(block).digest())
                    self.file_blocks[name].append(block)
            base_infos[name] = infomap
        return base_infos

    def get_fileinfomap(self):
        """
        Get server's fileinfomap
        :return: dict {file_name: [version, [blocks' hash]]}
        """
        return self.server.getfileinfomap()

    def update_index(self, local_infos):
        """
        Update index.txt
        """
        with open(os.path.join(self.base_dir, 'index.txt'), 'w') as f:
            for name in sorted(local_infos.keys()):  # sort by names for human searching
                ver, hs = local_infos[name]
                hs = " ".join(h.hex() for h in hs) if hs else '0'
                f.write(f'{name} {ver} {hs}\n')

    def run(self):
        """
        Assume server and base do not change when running
        """
        # file_info from base dir
        base_infos = self.scan_base()
        # file_info from index.html
        index_infos = self.read_index()
        # file_info from server
        remote_infos = self.get_fileinfomap()
        local_infos = {}

        all_files = set().union(base_infos, index_infos, remote_infos)

        for file_name in all_files:
            index_info = index_infos.get(file_name, [0, []])
            base_info = base_infos.get(file_name, [0, []])
            remote_info = remote_infos.get(file_name, [0, []])
            if remote_info == index_info and remote_info[1] and not base_info[1]:
                # local delete file
                self.delete(file_name, index_info[0] + 1)
                local_infos[file_name] = [index_info[0] + 1, []]
            elif remote_info == index_info and base_info[1] != index_info[1]:
                # local modify file
                base_info[0] = index_info[0] + 1
                assert self.upload(file_name, base_info)
                local_infos[file_name] = base_info
            elif remote_info != index_info:
                # remote delete file
                if not remote_info[1] and base_info[1]:
                    os.remove(os.path.join(self.base_dir, file_name))
                else:
                    # remote update file
                    self.download(file_name)
                local_infos[file_name] = remote_info
            else:
                local_infos[file_name] = index_info

        self.update_index(local_infos)

        return


def main():
    parser = argparse.ArgumentParser(description="SurfStore client")
    parser.add_argument('hostport', help='host:port of the server')
    parser.add_argument('basedir', help='The base directory')
    parser.add_argument('blocksize', type=int, help='Block size')
    args = parser.parse_args()
    print(args)

    with xmlrpc.client.ServerProxy(f'http://{args.hostport}', use_builtin_types=True) as proxy:
        client = SurfstoreClient(proxy, args.basedir, args.blocksize)
        client.run()


if __name__ == "__main__":
    main()
