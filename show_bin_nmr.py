#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function

import argparse
import struct
from pathlib import Path
import time

import numpy as np
import matplotlib.pyplot as plt

__author__ = ['Mattia Ceccarelli']
__email__ = ['mattia.ceccarelli5@unibo.it']


def parse_args():

    description = 'show a slideshow of '

    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('-i', '--indir',
                        dest='indir',
                        required=True,
                        type=str,
                        action='store',
                        help='NAME or PATH of the directory were bin files are stored'
                        )

    parser.add_argument('-r', '--rgb_range',
                    dest='rgb_range',
                    required=False,
                    type=int,
                    action='store',
                    default=255,
                    help='maximum value of the image file'
                    )

    args = parser.parse_args()

    return args


def read_bin(filename):
    with open(filename, 'rb') as file:
        w, h = struct.unpack('ii', file.read(8))
        img = np.asarray(struct.unpack('f' * (w * h), file.read())).reshape(w, h)

    return img

def main():

    args = parse_args()
    INDIR = Path(args.indir)

    filenames = list(INDIR.glob('*.bin'))
    filenames = sorted(filenames, key=str)

    fig, ax = plt.subplots(1, 1)

    for filename in filenames:
        img = read_bin(filename)

        ax.imshow(img, vmin=0, vmax=args.rgb_range, cmap='gray')
        plt.pause(.1)
        plt.cla()


if __name__ == '__main__':
    main()