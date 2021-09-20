#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function

import os
import argparse
from glob import glob
from tqdm import tqdm
from time import time as now
import datetime

import pandas as pd

__author__ = ['Mattia Ceccarelli']
__email__ = ['mattia.ceccarelli5@unibo.it']


def parse_args():

    description = 'Merging References Dataset for PRIMA project'

    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('-i', '--indir',
                        dest='indir',
                        required=True,
                        type=str,
                        action='store',
                        help='NAME or PATH of the directory were raw data are stored'
                        )
    parser.add_argument('-o', '--outdir',
                        dest='outdir',
                        required=False,
                        type=str,
                        action='store',
                        default='',
                        help='Name of the directory were the full dataset will be held'
                        )

    args = parser.parse_args()

    return args


def merge_single_day_dataset(daily_datafiles):
    '''
    This functions merge all dataframe from a SINGLE DAY
    '''

    daily_data = pd.DataFrame(columns=['TimeInt', 'Date'])
    daily_data.Date = pd.to_datetime(daily_data.Date)

    for filename in daily_datafiles:

        data = pd.read_html(filename, header=0)[0]

        # Estrae il nome della Variabile considerata
        var_name = filename.split(os.sep)[-1].split('.')[0]

        # Qui rinomino i nomi delle Variabili in modo da renderli unici per quella Variabile
        names = {'TimeStr': 'Date',
                 'IsInitValue': 'IsInitValue' + f' {var_name}',
                 'Value': var_name,
                 'IQuality': 'IQuality' + f' {var_name}'
                 }

        data.rename(columns=names, inplace=True)
        data.Date = pd.to_datetime(data.Date, dayfirst=True)

        daily_data = pd.merge(daily_data, data, how='outer')

    return daily_data


def merge_dataset(datafiles, last_datetime_recorded):

    dataset_list = []

    for daily_datafiles in tqdm(datafiles):

        recording_date = extract_date_from_path(daily_datafiles)

        if recording_date > last_datetime_recorded:

            daily_data = merge_single_day_dataset(daily_datafiles)
            dataset_list.append(daily_data)

    if dataset_list:
        new_data = pd.concat(dataset_list)

        return new_data

    else:
        raise ValueError('No New Data to add, leaving everything untouched')


def extract_date_from_path(filename):

    date_list = [datetime.datetime.strptime(file.split(os.sep)[-2], '%Y.%m.%d').date() for file in filename]

    # Controlla che siano tutte uguali
    check = all(el == date_list[0] for el in date_list)

    if check:
        # Se sono tutte uguali, ritorna la prima
        return date_list[0]

    else:
        raise ValueError('Not all dates in files are the same, check the number of variables')


def separate_filename_by_date(datafiles, nmb_of_vars=8):

    sep = [list(datafiles[i: i + nmb_of_vars]) for i in range(0, len(datafiles), nmb_of_vars)]

    return sep


def main():
    '''
    INDIR è la cartella dove sono contenute le directory chiamate yyyy.mm.dd
    OUTDIR è la cartella di output del merged dataset.
    Il nome di output è semplicemente "yyyy-mm-dd_pisa.csv", dove la data è
    l'ultima disponibile nel dataset

    Quando LANCI, assicurati: INDIR PATH è CORRETTO.

    - Se L'ultima "data" contenuta nei dati raw è già la data più recente
      nel file completo, nulla sarà modificato.
    - I dati raw sono in formato HTML
    - I dati completi in formato .csv
    - Il file completo verrà cercato nella cartella INDIR
    - Il file completo nuovo verrà salvato, e quello vecchio eliminato
    '''

    args = parse_args()

    INDIR = args.indir
    OUTDIR = args.outdir if args.outdir else INDIR
    APPEND = glob(os.path.join(INDIR, '*_pisa.csv'))

    if os.path.exists(INDIR):
        datafiles = sorted(glob(os.path.join(INDIR, '**', '*.HTML'), recursive=True))

        if len(datafiles) % 8 != 0:
            raise ValueError(f'The number of datafiles is not a multiple of 8 but its {len(datafiles)}')

        else:
            datafiles = separate_filename_by_date(datafiles)

    else:
        raise ValueError(f'path {INDIR} does not exist')

    # Se APPEND esiste già, partiamo da quello e aggiungiamo nuovi dati.
    if APPEND:
        old_data = pd.read_csv(APPEND[0])
        old_data.Date = pd.to_datetime(old_data.Date, dayfirst=True)
        last_datetime_recorded = old_data.Date.max().date()

    else:
        old_data = pd.DataFrame()
        last_datetime_recorded = datetime.date.min

    print('\n\n', '*' * 10, 'MERGING DATASETS', '*' * 10)

    # MERGE the datasets for all the commons columns
    tic = now()
    new_data = merge_dataset(datafiles, last_datetime_recorded)
    toc = now()

    full_data = pd.concat([old_data, new_data])

    # Scrivo il nome del nuovo dataset come 'yyyy-mm-dd_pisa.csv'
    name = os.path.join(OUTDIR, str(new_data.Date.max().date()) + '_pisa.csv')

    # Rimuove il vecchio file se esiste
    if APPEND:
        if os.path.exists(APPEND[0]):
            os.remove(APPEND[0])

    # Imposto l'indice come datetime Salvo
    full_data = full_data.set_index('Date')

    # Scrivo il nome e salva il dataset
    full_data.to_csv(name)

    print('\n', f'Finished Merging {len(datafiles)} datasets in {toc-tic:.4}s. Saved to {OUTDIR}/')


if __name__ == '__main__':

    main()
