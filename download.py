import os
import os.path as osp
from functools import partial
from tqdm import tqdm
import argparse
import multiprocessing as mp
from minedojo.data import YouTubeDataset


def worker(url, output, resolution):
    cmd = f"yt-dlp -f '(mp4)worstvideo[height>={resolution}]+bestaudio' --write-auto-subs {url} -o '{output}/f%(id)s.%(ext)s'"    
    os.system(cmd)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-o', '--output', type=str, required=True)
    parser.add_argument('-r', '--resolution', type=int, default=256)
    parser.add_argument('-w', '--num_workers', type=int, default=32)
    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)
    
    dset = YouTubeDataset(full=True, download=True)
    urls = [d['url'] for d in dset]

    pool = mp.Pool(args.num_workers)
    list(tqdm(pool.imap(worker, urls), total=len(urls)))