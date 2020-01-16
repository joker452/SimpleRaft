from hashlib import sha256


class SurfStore:
    def __init__(self):
        self.blocks = {}  # {hash: block}
        self.file_infos = {}  # {file_name: [version, [blocks' hash]]}

    def getblock(self, h):
        """Gets a block, given a specific hash value"""
        print(f"GetBlock({h})")
        assert isinstance(h, bytes), "Hash must be bytes"
        assert h in self.blocks, "Can only get existing blocks"
        return self.blocks[h]

    def putblock(self, b):
        """Puts a block"""
        print("PutBlock()")
        assert isinstance(b, bytes), "Block must be bytes"
        assert len(b) > 0, "Block must be at least one byte large!"
        h = sha256(b).digest()
        self.blocks[h] = b

        return True

    def hasblocks(self, blocklist):
        """Get blocks on this server with hashes in input"""
        print("HasBlocks()")
        # don't need to return hashes
        return [h for h in blocklist if h in self.blocks]

    def getfileinfomap(self):
        """Gets the fileinfo map"""
        print("GetFileInfoMap()")
        return self.file_infos

    def updatefile(self, filename, version, blocklist):
        """Updates a file's fileinfo entry"""
        assert isinstance(version, int), "Version must be int"
        # assert all(isinstance(h, bytes) for h in blocklist), "Hash must be bytes"
        if filename in self.file_infos and version != self.file_infos[filename][0] + 1:
            # version of modifying existing file must increase by one
            return False
        assert not (filename not in self.file_infos and version != 1), "Version of file creation must be one"
        self.file_infos[filename] = [version, blocklist]
        return True
