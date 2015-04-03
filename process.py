import os
import urllib
import zipfile
import logging
import sys
from cStringIO import StringIO
from collections import namedtuple

import shapefile
# import pyproj
# import Shapely

DATA_PATH = "./data"
URL_FORMAT = "http://www2.census.gov/geo/tiger/TIGER2010BLKPOPHU/tabblock2010_%02d_pophu.zip"
MAX_INDEX = 57
SKIP_INDICES = (3, 7, 14, 43, 52)
FIELD = 'POP10'

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

Block = namedtuple('Block', ['lat', 'lng', 'pop', 'area'])


def makeReportHook(prefix):
    def reporthook(count, blockSize, totalSize):
        done = count*blockSize/float(totalSize)
        sys.stdout.write("\r{0} ({1:.0%})".format(prefix, done))
        sys.stdout.flush()

    return reporthook


def process_url(url):
    filename = url.split('/')[-1]
    basename = filename.split('.')[0]

    dirpath = os.path.join(DATA_PATH, basename)
    if not os.path.isdir(dirpath):
        filepath = os.path.join(DATA_PATH, filename)

        if not os.path.isfile(filepath):
            prefix = 'Downloading %s' % filename
            logging.info(prefix)
            urllib.urlretrieve(url, filepath, makeReportHook(prefix))
            print
        else:
            logging.info('Found cached %s' % filename)

        os.mkdir(dirpath)
        logging.info('Extracting to %s' % dirpath)
        zipfile.ZipFile(filepath, 'r').extractall(dirpath)
    else:
        logging.info('Extracted data found at %s' % dirpath)

    sf = shapefile.Reader(os.path.join(dirpath, basename))
    # Subtract one for the deletion flag
    fieldIndex = [f[0] for f in sf.fields].index(FIELD) - 1

    blocks = []
    for sr in sf.iterShapeRecords():
        pop = sr.record[fieldIndex]

        lat = sum(p[0] for p in sr.shape.points) / len(sr.shape.points)
        lng = sum(p[1] for p in sr.shape.points) / len(sr.shape.points)
        area = 0
        # area = polygon_area(shape.points)

        blocks.append(Block(lat=lat, lng=lng, pop=pop, area=area))

    return blocks

for idx in range(1, MAX_INDEX):
    if idx in SKIP_INDICES:
        continue
    process_url(URL_FORMAT % idx)
