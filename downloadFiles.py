# -*- coding: utf-8 -*-
"""Description package/module main.py.

TODO: Improve desc

"""
from zipfile import ZipFile
import os
import bz2
from logging import getLogger
from urllib.request import urlopen
import shutil


# Settings
MAIN_ZIP_FOLDER = 'ipinyou.contest.dataset/training3rd/'
MAP_FILE = 'ipinyou.contest.dataset/training3rd/city.en.txt'
NEEDED_EXT = 'bz2'
OUTPUT_FOLDER = os.path.join(os.getcwd(), 'downloadsHW1')
URL = "http://goo.gl/lwgoxw"

# Logger initialization
log = getLogger(__name__)


def get_large_file(url, file, length=16 * 1024):
    """Save large file."

    :param url:
    :param file:
    :param length:
    :return:
    """
    req = urlopen(url)
    with open(file, 'wb') as fp:
        shutil.copyfileobj(req, fp, length)
    log.info("finish loading")


def read_in_chunks(file_object, chunk_size=1024):
    """Lazy function (generator) to read a file piece by piece.
    Default chunk size: 1k."""
    while True:
        data = file_object.read(chunk_size)
        if not data:
            break
        yield data


def save_file(zip_file, file_name: str, output_directory: str):
    """Save file in input directory.

    :param zip_file:
    :param file_name:
    :param output_directory:
    :return:
    """

    output_path = os.path.join(output_directory, file_name.split("/")[-1])
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
    with open(output_path, 'wb') as output_f:
        with zip_file.open(file_name) as input_f:
            for chunk_data in read_in_chunks(input_f):
                output_f.write(chunk_data)
    log.info(f"write {output_path}")


def extract_bz2_file(zip_file, file_name: str, output_directory: str):
    """Extract and write bz2 file.

    :param zip_file:
    :param file_name:
    :param output_directory:
    :return:
    """
    output_path = os.path.join(output_directory,
                               file_name.replace('.txt.bz2', '.txt').split(
                                   "/")[-1])
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
    with open(output_path, 'wb') as output_f:
        with zip_file.open(file_name, 'r') as input_f:
            decompressor = bz2.BZ2Decompressor()
            for data in iter(lambda: input_f.read(100 * 1024), b''):
                if data:
                    try:
                        output_f.write(decompressor.decompress(data))
                    except Exception as err:
                        log.info("finish unzipping")
    log.info(f"write {output_path}")


def write_data(filename):
    """Write needed data from zip to output folder."""
    with open(filename, 'rb') as input_data:
        with ZipFile(input_data) as my_zip_file:
            for contained_file in my_zip_file.namelist():
                if contained_file.endswith(f'.{NEEDED_EXT}'):
                    extract_bz2_file(my_zip_file, contained_file, OUTPUT_FOLDER)
                elif contained_file == MAP_FILE:
                    save_file(my_zip_file, contained_file, OUTPUT_FOLDER)


if __name__ == "__main__":
    result_filename = "result.zip"
    if not os.path.exists(result_filename):
        get_large_file(URL, "result.zip")
    write_data(result_filename)
