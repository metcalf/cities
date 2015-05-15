import os
import urllib
import zipfile
import logging
import sys
import csv
import struct
import json
import math
import bisect
import base64
from operator import attrgetter
from cStringIO import StringIO
from collections import namedtuple

import shapefile
from pyproj import Proj
from shapely.geometry import shape

DATA_PATH = "./data"
URL_FORMAT = "http://www2.census.gov/geo/tiger/TIGER2010BLKPOPHU/tabblock2010_%02d_pophu.zip"
MAX_INDEX = 57
SKIP_INDICES = (3, 7, 14, 43, 52)
POP_FIELD = 'POP10'
FIND_DISTANCE = 80000.0
EARTH_RADIUS = 6370997.0
MAX_CITIES = 100

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

Block = namedtuple('Block', ['lat', 'lon', 'pop', 'area'])
BlockStruct = struct.Struct("iiII")


def makeReportHook(prefix):
    def reporthook(count, blockSize, totalSize):
        done = count*blockSize/float(totalSize)
        sys.stdout.write("\r{0} ({1:.0%})".format(prefix, done))
        sys.stdout.flush()

    return reporthook


def download(url):
    filename = url.split('/')[-1]
    filepath = os.path.join(DATA_PATH, filename)

    if not os.path.isfile(filepath):
        prefix = 'Downloading %s' % filename
        logging.info(prefix)
        urllib.urlretrieve(url, filepath, makeReportHook(prefix))
        print
    else:
        logging.debug('Found existing %s', filename)

    return filepath


def extract_blocks(filepath):
    basename = os.path.basename(filepath).split('.')[0]

    with zipfile.ZipFile(filepath, 'r') as zipped:
        kwargs = {}
        for ext in ('shp', 'shx', 'dbf'):
            fn = '%s.%s' % (basename, ext)
            kwargs[ext] = StringIO(zipped.open(fn).read())

        logging.debug('Read data into memory, processing')

        sf = shapefile.Reader(**kwargs)
        # Subtract one for the deletion flag
        fieldIndex = [f[0] for f in sf.fields].index(POP_FIELD) - 1

        for sr in sf.iterShapeRecords():
            pop = sr.record[fieldIndex]

            lons, lats = zip(*sr.shape.points)

            lon = sum(lons) / len(lons)
            lat = sum(lats) / len(lats)

            pa = Proj(
                "+proj=aea +lat_1=%.6f +lat_2=%.6f +lat_0=%.6f +lon_0=%.6f" % (
                    min(lats), max(lats), lat, lon
                ))
            area = shape({
                "type": "Polygon",
                "coordinates": [zip(*pa(lons, lats))]
            }).area

            yield Block(lat=lat, lon=lon, pop=pop, area=area)


def read_blockfile(filepath):
    with open(filepath, 'r') as blockfile:
        while True:
            binary = blockfile.read(BlockStruct.size)
            if binary == "":
                break

            data = BlockStruct.unpack(binary)

            yield Block(
                lon=data[0] / 1.0e6,
                lat=data[1] / 1.0e6,
                pop=data[2],
                area=data[3]
            )


def pack_block(block):
    area = block.area
    if area > 4294967295:
        logging.debug("Found huge block: %s", str(block))
        area = 0  # Zero as sentinel for "really huge"

    return BlockStruct.pack(
        int(round(block.lon * 1e6)),
        int(round(block.lat * 1e6)),
        block.pop,
        area)


# Find all blocks that are within a dist meter square of the supplied point
# Assumes that blocks are sorted by longitude and just naively iterates through
# the relevant range of longitudes, checking the latitude for each.
def find_blocks(lon, lat, dist, blocks, block_lons):
    d_lat = math.degrees(dist /
                         (2 * EARTH_RADIUS))
    d_lon = math.degrees(dist /
                         (2 * EARTH_RADIUS * math.cos(math.radians(lat))))

    min_lat = lat - d_lat
    max_lat = lat + d_lat
    min_lon = lon - d_lon
    max_lon = lon + d_lon

    i = bisect.bisect_left(block_lons, min_lon)
    while block_lons[i] <= max_lon:
        block = blocks[i]
        if block.lat >= min_lat and block.lat <= max_lat:
            yield block

        i += 1


def process():
    blocks = []
    cities = []

    for idx in range(1, MAX_INDEX):
        if idx in SKIP_INDICES:
            continue
        filename = os.path.join(DATA_PATH, "%d.blk" % idx)

        if not os.path.isfile(filename):
            zippath = download(URL_FORMAT % idx)

            logging.info("Generating %s", filename)
            # Collect in a buffer and write at once to avoid incomplete files
            buf = StringIO()
            for block in extract_blocks(zippath):
                blocks.append(block)
                buf.write(pack_block(block))

            logging.debug("Writing %s", filename)
            with open(filename, 'w') as blockfile:
                blockfile.write(buf.getvalue())
        else:
            logging.info('Reading existing %s', filename)
            for block in read_blockfile(filename):
                blocks.append(block)

    logging.debug('Got %d input blocks', len(blocks))

    blocks.sort(key=attrgetter('lon'))
    block_lons = [b.lon for b in blocks]

    with open(os.path.join(DATA_PATH, 'cities.csv')) as cityfile:
        i = 0
        for city in csv.DictReader(cityfile):
            if i >= MAX_CITIES:
                break
            i += 1

            for k in ('2013-population', '2010-population'):
                city[k] = int(city[k])
            for k in ('area', 'lat', 'lon'):
                city[k] = float(city[k])

            # Represent as degrees East.
            city['lon'] = -city['lon']

            block_gen = find_blocks(city['lon'], city['lat'],
                                    FIND_DISTANCE, blocks, block_lons)
            block_data = ''.join([pack_block(b) for b in block_gen])
            city['blocks'] = base64.b64encode(block_data)

            cities.append(city)
            logging.debug("Processed %s, %s with %d blocks",
                          city['city'], city['state'],
                          len(block_data) / BlockStruct.size)



    json.dump(cities, open(os.path.join(DATA_PATH, 'cities.json'), 'w'))

if __name__ == '__main__':
    process()
