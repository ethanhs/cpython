# TODO
import sys
from _zstd import ZSTDCompressor
c = ZSTDCompressor()
c.compress(sys.stdin.read().encode('utf-8'), 0)
sys.stdout.buffer.write(c.flush(2))