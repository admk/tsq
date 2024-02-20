import sys

from tqdm import tqdm
from time import sleep

for i in tqdm(range(0, int(sys.argv[1]))):
	sleep(1)
