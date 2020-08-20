import pandas as pd
import argparse
import os
import shutil
import subprocess
from joblib import delayed
from joblib import Parallel
import youtube_dl

from stem import Signal
from stem.control import Controller
import requests

import json

REQUIRED_COLUMNS = ['label', 'youtube_id', 'time_start', 'time_end', 'split', 'is_cc']
TRIM_FORMAT = '%06d'
URL_BASE = 'https://www.youtube.com/watch?v='

VIDEO_EXTENSION = '.mp4'
VIDEO_FORMAT = 'mp4'
TOTAL_VIDEOS = 0


def create_file_structure(path, folders_names):
    """
    Creates folders in specified path.
    :return: dict
        Mapping from label to absolute path folder, with videos of this label
    """
    mapping = {}
    if not os.path.exists(path):
        os.mkdir(path)
    for name in folders_names:
        dir_ = os.path.join(path, name)
        dir_ = dir_.replace(' ', '_')
        if not os.path.exists(dir_):
            os.mkdir(dir_)
        mapping[name] = dir_
        print(dir_)
    return mapping

def test_proxy(config):
    print('----------------------------------------------')
    print('Running Proxy Test')
    print('Your IP:')
    ip_test = requests.get('http://httpbin.org/ip').json()
    print(requests.get('http://httpbin.org/ip').json())
    renew_connection(config['tor_password'])
    print('Tor IP:')
    tor_ip_test = requests.Session().get('http://httpbin.org/ip',
                                         proxies={'http': 'socks5://127.0.0.1:9050'}).json()
    print(tor_ip_test)
    test_result = False
    if ip_test != tor_ip_test:
        print('Tor IP working correctly')
        test_result = True
    else:
        print('Your IP and Tor IP are the same: check you are running tor from commandline')

    return test_result


def renew_connection(tor_password):
    print(tor_password)
    with Controller.from_port(port=9051) as controller:
        controller.authenticate(password=tor_password)
        controller.signal(Signal.NEWNYM)


def download_video(config, row, label_to_dir, trim, count):
    print('New Tor IP Adddress Allocated')
    renew_connection(config['tor_password'])

    label = row['label']
    videoId = row['youtube_id']
    # time_start = row['time_start']
    # time_end = row['time_end']

    # if trim, save full video to tmp folder
    output_path = label_to_dir['tmp'] if trim else label_to_dir[label]
    if not os.path.exists(os.path.join(output_path, videoId + VIDEO_EXTENSION)):
        print('===========================')
        print('Start downloading: ', videoId)
        print('===========================')

        ydl = youtube_dl.YoutubeDL({
            # 'outtmpl': os.path.dirname(os.path.realpath(__file__)) + '/videos/' + '%(id)s.%(ext)s',
            'outtmpl': output_path + '/' + '%(id)s.%(ext)s',
            'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best',
            'proxy': 'socks5://127.0.0.1:9050',
            'verbose': config['verbose_logging'],
            'nocheckcertificate': True,
            # 'postprocessors': '-ss 649.044 -t 3.0'
        })

        with ydl:
            # handle looping over files
            result = ydl.extract_info(
                f'http://www.youtube.com/watch?v={videoId}',
                download=True
            )
    else:
        print('Already downloaded: ', videoId)

    print('o==> Processed %i out of %i' % (count + 1, TOTAL_VIDEOS))


def main(input_csv, output_dir, trim, num_jobs):
    global TOTAL_VIDEOS

    # read config and test Tor
    config_file = open('config.json', 'r')
    config = json.load(config_file)

    test_proxy(config)

    assert input_csv[-4:] == '.csv', 'Provided input is not a .csv file'
    links_df = pd.read_csv(input_csv)
    assert all(elem in REQUIRED_COLUMNS for elem in links_df.columns.values),\
        'Input csv doesn\'t contain required columns.'

    # Creates folders where videos will be saved later
    # Also create 'tmp' directory for temporary files
    folders_names = links_df['label'].unique().tolist() + ['tmp']
    label_to_dir = create_file_structure(path=output_dir,
                                         folders_names=folders_names)
    
    TOTAL_VIDEOS = links_df.shape[0]
    print(f'\n\no====> Total videos {TOTAL_VIDEOS}, num-ojbs: {num_jobs}\n\n')
    # Download files by links from dataframe
    Parallel(n_jobs=num_jobs)(delayed(download_video)(
            config, row, label_to_dir, trim, count) for count, row in links_df.iterrows())

    # Clean tmp directory
    shutil.rmtree(label_to_dir['tmp'])


if __name__ == '__main__':
    description = 'Script for downloading and trimming videos from Kinetics dataset.' \
                  'Supports Kinetics-400 as well as Kinetics-600.'
    p = argparse.ArgumentParser(description=description)
    p.add_argument('input_csv', type=str,
                   help=('Path to csv file, containing links to youtube videos.\n'
                         'Should contain following columns:\n'
                         'label, youtube_id, time_start, time_end, split, is_cc'))
    p.add_argument('output_dir', type=str,
                   help='Output directory where videos will be saved.\n'
                        'It will be created if doesn\'t exist')
    p.add_argument('--trim', action='store_true', dest='trim', default=False,
                   help='If specified, trims downloaded video, using values, provided in input_csv.\n'
                        'Requires "ffmpeg" installed and added to environment PATH')
    p.add_argument('--num-jobs', type=int, default=1,
                   help='Number of parallel processes for downloading and trimming.')
    
    main(**vars(p.parse_args()))
