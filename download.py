import os
import os.path as osp
from functools import partial
from tqdm import tqdm
import argparse
import multiprocessing as mp
import json
from minedojo.data import YouTubeDataset


def worker(url):
    try:
        cmd = f"yt-dlp -f 'worstvideo[ext=mp4]+worstvideo[height>={args.resolution}]' --write-auto-subs {url} -o '{args.output}/f%(id)s.%(ext)s'"# >/dev/null 2>&1"
        os.system(cmd)
    except:
        return True
    return False


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-o', '--output', type=str, required=True)
    parser.add_argument('-r', '--resolution', type=int, default=128)
    parser.add_argument('-w', '--num_workers', type=int, default=32)
    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)
    
    dset = YouTubeDataset(full=True, download=True)
    urls = [d['link'] for d in dset][:1000]

    pool = mp.Pool(args.num_workers)

    errored = []
    pbar = tqdm(total=len(urls))
    for result in pool.imap(worker, urls):
        errored.append(result)
        pbar.update(1)
        pbar.set_description(f'Errored: {sum(errored)} / {len(errored)}')
    assert len(errored) == len(urls)

    errored_urls = [url for url, err in zip(urls, errored) if err]
    json.dump(errored_urls, open('errored.json', 'w'))
