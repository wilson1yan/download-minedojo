import argparse
import os
import os.path as osp
import multiprocessing as mp
import numpy as np
import skvideo.io
from tqdm import tqdm
import json
from enum import Enum


class Status(Enum):
    SUCCESS = 1
    NO_FILE = 2
    READ_ERROR = 3


status_to_str = {
    Status.SUCCESS: 'success',
    Status.NO_FILE: 'no_file',
    Status.READ_ERROR: 'read_error'
}


def worker(id):
    exts = ['mp4', 'webm']
    for ext in exts:
        fname = osp.join(root, 'data', f'{id}.{ext}')
        if osp.exists(fname):
            break
    else:
        return None, Status.NO_FILE
    
    try:
        video = skvideo.io.vread(fname)
        return video.shape[0], Status.SUCCESS
    except:
        return None, Status.READ_ERROR


def main():
    global root

    parser = argparse.ArgumentParser()
    parser.add_argument('--subset', type=str, default='/home/wilson/minedojo/subset.json')
    parser.add_argument('--n_chunks', type=int, default=1)
    parser.add_argument('--chunk_id', type=int, default=0)
    parser.add_argument('--n_workers', type=int, default=64)
    args = parser.parse_args()

    root = osp.dirname(args.subset)
    ids = json.load(open(args.subset, 'r'))
    print(f'Found {len(ids)} ids')

    ids = np.array_split(ids, args.n_chunks)[args.chunk_id].tolist()
    print(f'Chunk {len(ids)}')
    
    pool = mp.Pool(args.n_workers)
    results = list(tqdm(pool.imap(worker, ids), total=len(ids)))

    info = {}
    for id, r in zip(ids, results):
        info[id] = (r[0], status_to_str[r[1]])
    json.dump(info, open(f'subset_info_{args.chunk_id}_{args.n_chunks}.json', 'w'))
    print('done')


if __name__ == '__main__':
    main()
