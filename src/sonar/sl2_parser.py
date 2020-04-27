
# See info about the sl2 file format at:
# * https://wiki.openstreetmap.org/wiki/SL2
# * https://github.com/kmpm/node-sl2format/blob/master/doc/sl2fileformat.md
# * https://www.geotech1.com/forums/showthread.php?11159-Lowrance-MCC-saved-data-structure
# * https://github.com/Chris78/sl2decode/blob/master/sl2decode.rb
# * https://github.com/Chris78/sl2decode

import os
import os.path
import glob
import struct
import math

import numpy
import pandas
import fastparquet
import dask
import dask.dataframe
import xarray

PARAQUET_ENGINE='pyarrow'
#PARAQUET_ENGINE='fastparquet'

# Max items in a single pandas.DataFrame, we split and use dask to handle them
MAX_SEGMENT_SIZE = 10 * 1024

import logging
logger = logging.getLogger('sl2_parser')

class LowranceDecoder(object):
	def __init__(self, file):
		self.file = file
		self.offset = 0
		self.file_size = None
		try:
			self.file.seek(0, os.SEEK_END)
			self.file_size = self.file.tell()
		finally:
			self.file.seek(0)
		
	def SetEof(self):
		self.file = None
		
	def Get(self, size, format, offset=None):
		temp = self.GetBytes(size, offset)
		return struct.unpack(format, temp)[0]
	
	def GetBytes(self, size, offset=None):
		off = self.offset
		if offset is not None:
			off = offset

		if off + size > self.file_size:
			raise EOFError()
		
		if self.file is None:
			raise EOFError()
		
		self.file.seek(off)
		temp = self.file.read(size)
		if len(temp) < size:
			raise EOFError()
		
		if offset is None:
			self.offset += size
		
		return temp
	
	def GetUInt8(self): return int(self.Get(1, "B"))
	def GetUInt16(self): return int(self.Get(2, "<H"))
	def GetUInt32(self): return int(self.Get(4, "<I"))
	def GetFloat(self): return float(self.Get(4, "<f"))

class NoneDecoder(object):
	def __init__(self):
		self.offset = 0
	
	def Get(self, size, format, offset=None): return None
	def GetBytes(self, size, offset=None): return None
	def GetUInt8(self): return None
	def GetUInt16(self): return None
	def GetUInt32(self): return None
	def GetFloat(self): return None


class RawFileHeader(object):
	def __repr__(self): return "{}({!r})".format(self.__class__.__name__, self.__dict__)

	def __init__(self, decoder=NoneDecoder()):
		# 1 = slg
		# 2 = sl2
		# 3 = sl3
		self.format = decoder.GetUInt16()

		# 0 = HDS 7 etc...
		# 1 = Elite 4 CHIRP etc...
		self.version = decoder.GetUInt16()

		# 1970=Downscan #b207
		# 3200=Sidescan #800c
		self.block_size = decoder.GetUInt16()
		
		# always 0
		self.reserved = decoder.GetUInt16()

class RawBlock(object):

	#FREQUENCY_MAP = {
	#	0 : '200 KHz',
	#	1 : '50 KHz',
	#	2 : '83 KHz',
	#	3 : '455 KHz',
	#	4 : '800 KHz',
	#	5 : '38 KHz',
	#	6 : '28 KHz',
	#	7 : '130 KHz - 210 KHz',
	#	8 : '90 KHz - 150 KHz',
	#	9 : '40 KHz - 60 KHz',
	#	10 : '25 KHz - 45 KHz',
	#	# Any other value is treated like 200 KHz
	#}
	
	#FLAGS = {
	#	# all other values are unknown.
	#	# @todo need to verify the bits with LE reading
	#	'track_valid' : 1 << 15,
	#	'water_speed_valid' : 1 << 14,
	#	'position_valid' : 1 << 12,
	#	'water_temp_valid' : 1 << 10,
	#	'gps_speed_valid' : 1 << 9,
	#	'altitude_valid' : 1 << 1,
	#	'heading_valid' : 1 << 0,
	#}

	def __repr__(self):
		tmp = [(k,self.__dict__[k]) for k in sorted(self.__dict__.keys())]
		return "{}({!r})".format(self.__class__.__name__, tmp)

	def __init__(self, header=None, decoder=NoneDecoder()):
		begin_block_offset = decoder.offset

		# @todo Dont want this in the repr, so far not required
		self.block_offset = decoder.GetUInt32()
		self.last_primary_channel_block_offset = decoder.GetUInt32()
		self.last_secondary_channel_block_offset = decoder.GetUInt32()
		self.last_downscan_channel_block_offset = decoder.GetUInt32()
		self.last_left_sidescan_channel_block_offset = decoder.GetUInt32()
		self.last_right_sidescan_channel_block_offset = decoder.GetUInt32()
		self.last_composite_sidescan_channel_block_offset = decoder.GetUInt32()

		assert(decoder.offset == begin_block_offset+28)

		#decoder.offset = begin_block_offset + 28
		self.current_block_bytes = decoder.GetUInt16()
		self.last_block_bytes = decoder.GetUInt16()

		self.channel = decoder.GetUInt16()
		self.sonar_data_size = decoder.GetUInt16()

		#decoder.offset = begin_block_offset + 36
		self.frame_index = decoder.GetUInt32()
		
		self.upper_limit = decoder.GetFloat()
		self.lower_limit = decoder.GetFloat()
		self.unknown1 = decoder.GetUInt16()

		assert(decoder.offset == begin_block_offset+50)
		self.frequency = decoder.GetUInt8()
		self.unknown2 = decoder.GetUInt8()
		self.unknown3 = decoder.GetUInt32()
		self.unknown4 = decoder.GetUInt32()
		self.unknown5 = decoder.GetUInt32()

		assert(decoder.offset == begin_block_offset+64)
		self.water_depth = decoder.GetFloat()
		self.keel_depth_feet = decoder.GetFloat()

		self.unknown6 = decoder.GetUInt32()
		self.unknown7 = decoder.GetUInt32()
		self.unknown8 = decoder.GetUInt32()
		self.unknown9 = decoder.GetUInt32()
		self.unknown10 = decoder.GetUInt32()
		self.unknown11 = decoder.GetUInt32()
		self.unknown12 = decoder.GetUInt32()

		assert(decoder.offset == begin_block_offset+100)
		self.speed_gps_knots = decoder.GetFloat()
		self.temperature_celcius = decoder.GetFloat()

		self.lowrance_longitude = decoder.GetUInt32()
		self.lowrance_latitude = decoder.GetUInt32()

		self.speed_water_knots = decoder.GetFloat()

		self.course_over_ground_radians = decoder.GetFloat()
		self.altitude_feet = decoder.GetFloat()
		self.heading_radians = decoder.GetFloat()
		self.flags = decoder.GetUInt16()
		self.unknown13 = decoder.GetUInt16()
		self.unknown14 = decoder.GetUInt32()

		assert(decoder.offset == begin_block_offset+140)
		# time1 : first value contains ms since 1970 if multiplied by 1000, 
		# consecutive values have time encoded somehow different
		self.time = decoder.GetUInt32()

		self.sonar_data = []
		for i in range(0, self.sonar_data_size):
			self.sonar_data.append(decoder.GetUInt8())

		# The last block has a current_block_bytes and last_block_bytes of 0
		if self.current_block_bytes != 0:
			assert(decoder.offset - begin_block_offset <= self.current_block_bytes)
			decoder.offset = begin_block_offset + self.current_block_bytes
		else:
			decoder.SetEof();

def LoadRawLowranceLog(file_name, on_block, on_header=None, early_complete=None):
	file_size = os.path.getsize(file_name)
	with open(file_name, mode='rb') as file:
		decoder = LowranceDecoder(file)
		raw_file_header = RawFileHeader(decoder)
		logger.debug('FileHeader: %s', raw_file_header)
		if on_header is not None:
			on_header(raw_file_header)

		percent_complete = -1
		while decoder.offset < file_size:
			new_percent_complete = int(100 * decoder.offset / file_size)
			if new_percent_complete != percent_complete:
				percent_complete = new_percent_complete
				logger.info ('%s %%' % (new_percent_complete))
				if early_complete is not None and new_percent_complete > early_complete:
					logger.info('Exiting load early as asked to finish when loaded: %s %% of the file', early_complete)
					return
				
			try: raw_block = RawBlock(raw_file_header, decoder)
			except EOFError: 
				logger.warning('EOF in middle of incomplete block while reading sonar file: %s, this is common in normal scenarios as the logged sonar produced by devices is not always complete but may indicate a corrupted file', file_name)
				return
			
			logger.debug('RawBlock: %s', raw_block)
			if on_block is not None:
				on_block(raw_block)


def FeetToMeters(value): return value / 3.2808399

SECONDS_PER_MINUTE = 60
MINUTES_PER_HOUR = 60
def KnotsToKmph(value): return (value / 1.94385) / 1000 * SECONDS_PER_MINUTE * MINUTES_PER_HOUR

#class ChannelType(enum.Enum):
#	PRIMARY = 0
#	SECONDARY = 1
#	DOWNSCAN = 2
#	LEFT_SIDESCAN = 3
#	RIGHT_SIDESCAN = 4
#	COMPOSITE_SIDESCAN = 5

PRIMARY = 0
SECONDARY = 1
DOWNSCAN = 2
LEFT_SIDESCAN = 3
RIGHT_SIDESCAN = 4
COMPOSITE_SIDESCAN = 5

def ChannelToStr(id):
	if id == PRIMARY: return 'primary'
	elif id == SECONDARY: return 'secondary'
	elif id == DOWNSCAN: return 'downscan'
	elif id == LEFT_SIDESCAN: return 'left_sidescan'
	elif id == RIGHT_SIDESCAN: return 'right_sidescan'
	elif id == COMPOSITE_SIDESCAN: return 'composite_sidescan'
	else: return 'unknown' + str(id)

# SL2 format stores Easting and Northing coordinates in Spherical Mercator Projection, 
# using WGS84 POLAR Earth radius
#
# OpenStreetMap and Google instead use the WGS84 EQUATORIAL Earth Radius
# So we will convert and use the more popular format
POLAR_EARTH_RADIUS = 6356752.3142;

# https://www.movable-type.co.uk/scripts/latlong-utm-mgrs.html
# A Universal Transverse Mercator coordinate comprises a zone number, a hemisphere (N/S), an easting and a northing. 
# Eastings are referenced from the central meridian of each zone, & northings from the equator, both in metres. 
# To avoid negative numbers, ‘false eastings’ and ‘false northings’ are used:

# Eastings are measured from 500,000 metres west of the central meridian. Eastings (at the equator) range from 166,021m to 833,978m (the range decreases moving away from the equator); a point on the the central meridian has the value 500,000m.
#
#In the northern hemisphere, northings are measured from the equator – ranging from 0 at the equator to 9,329,005m at 84°N). In the southern hemisphere they are measured from 10,000,000 metres south of the equator (close to the pole) – ranging from 1,116,915m at 80°S to 10,000,000m at the equator.

def UniversalTransverseMercatorToWGS84EquatorialLongitude(polar_longitude):
	plon = int(polar_longitude)
	equatorial_longitude = plon / POLAR_EARTH_RADIUS * (180.0 / math.pi)
	return equatorial_longitude

# From: https://github.com/Chris78/sl2decode/blob/master/sl2decode.rb 
# https://github.com/Chris78/sl2decode
UINT32_MAX = 0xffffffff # 4294967295U
def UniversalTransverseMercatorToWGS84EquatorialLatitude(polar_latitude, is_northern_hemisphere=False):
	plat = int(polar_latitude)
	if not is_northern_hemisphere:
		plat = plat - UINT32_MAX
		
	temp = plat / POLAR_EARTH_RADIUS
	temp = math.exp(temp)
	temp = (2 * math.atan(temp)) - (math.pi / 2)
	equatorial_latitude = temp * (180/math.pi)
	
	# @todo Horrible hack, I need to understand this better so we can do the 
	# correct thing not just copy from others
	if equatorial_latitude == -90.0 and not is_northern_hemisphere:
		return UniversalTransverseMercatorToWGS84EquatorialLatitude(polar_latitude, True)
	return equatorial_latitude


class ChannelData(object):
	def __init__(self, file_name, cache_name, channel, next_index=0):
		self.file_name = file_name
		self.cache_name = cache_name
		self.channel = channel
		self.next_index = next_index
		self.data = []
		self.current_segment = 0
	
	def FinishSegment(self):
		if len(self.data) > 0:
			logger.debug('Merging xarray')
			current = xarray.concat(self.data, dim='time')

			netcdf_file_name = self.cache_name + '.cache.chan' + str(self.channel) + '.' + str(self.current_segment) + '.nc'
			logger.debug('Writing segment to file: %s with index range (%s - %s)', netcdf_file_name, self.next_index - len(self.data), self.next_index)

			if not os.path.isdir(os.path.dirname(netcdf_file_name)):
				os.makedirs(os.path.dirname(netcdf_file_name))
			current.to_netcdf(netcdf_file_name, engine='netcdf4')

			self.current_segment += 1
			self.data = []
	
class DataFrameLoader(object):
	def __init__(self, file_name, cache_name):
		self.file_name = file_name
		self.cache_name = cache_name
		self.channel_data = {}
	
	def FinishSegment(self):
		for channel, channel_data in self.channel_data.items():
			channel_data.FinishSegment()
	
	def OnBlock(self, block):
		longitude = UniversalTransverseMercatorToWGS84EquatorialLongitude(block.lowrance_longitude)
		latitude = UniversalTransverseMercatorToWGS84EquatorialLatitude(block.lowrance_latitude)

		if longitude == 0.0 or latitude == -90.0:
			logger.warning('Strange SL2 file: %s, Skipping block with frame_index: %s has invalid GPS coordinates: %s, %s mapping to WGS84: %s, %s', self.file_name, block.frame_index, block.lowrance_longitude, block.lowrance_latitude, longitude, latitude)
			return
			
		if block.channel not in self.channel_data:
			logger.info('New channel %s found in SL2 file starting at index: %s', block.channel, block.frame_index)
			self.channel_data[block.channel] = ChannelData(self.file_name, self.cache_name, block.channel, block.frame_index)
		
		# @todo would be nice to have real times (but not replace indexes)
		data_index = self.channel_data[block.channel].next_index
		self.channel_data[block.channel].next_index += 1
		
		i = [block.frame_index]
		d = {
			'channel': block.channel,
			'index': block.frame_index,
			'water_depth': FeetToMeters(block.water_depth),
			'upper_limit': FeetToMeters(block.upper_limit),
			'lower_limit': FeetToMeters(block.lower_limit),
			'longitude': longitude,
			'latitude': latitude,
			'data_index': data_index
		}
		
		#logger.info('Converted longitude: (sl2 %s to WGS84 %s) latitude: (sl2 %s to WGS84 %s)', block.lowrance_longitude, d['longitude'], block.lowrance_latitude, d['latitude'])
		sonar_data = numpy.array(block.sonar_data, dtype=numpy.uint8)
		ds = xarray.Dataset({
				'sl2_index': (['channel', 'time'],  numpy.array([[ d['index'] ]])),
				'depth': (['channel', 'time'],  numpy.array([[ d['water_depth'] ]]), {'units': 'meters'}),
				'longitude': (['channel', 'time'],  numpy.array([[ d['longitude'] ]])),
				'latitude': (['channel', 'time'],  numpy.array([[ d['latitude'] ]])),
				'upper_limit': (['channel', 'time'],  numpy.array([[ d['upper_limit'] ]]), {'units': 'meters'}),
				'lower_limit': (['channel', 'time'],  numpy.array([[ d['lower_limit'] ]]), {'units': 'meters'}),
				'data': (['channel', 'time', 'depth_bin'],  [[ sonar_data ]], {'units': 'amplitude'}),
			},
			coords={
				'depth_bin': (['depth_bin'], range(0,len(sonar_data))),
				'time': (['time'], [data_index]),
				'channel': (['channel'], [block.channel])
			})

		if data_index != block.frame_index:
			logger.warning('Strange SL2 file: %s, data_index: %s != frame_index: %s', self.file_name, data_index, block.frame_index)

		self.channel_data[block.channel].data.append(ds)
		if len(self.channel_data[block.channel].data) > MAX_SEGMENT_SIZE:
			self.channel_data[block.channel].FinishSegment()

@dask.delayed
def load_parquet_chunk(pth):
	# Subset of columns for now, will add others later as necessary
	return fastparquet.ParquetFile(pth).to_pandas(columns=['index', 'longitude', 'latitude', 'channel', 'water_depth', 'upper_limit', 'lower_limit', 'data_index'])

class SL2Data(object):
	def __init__(self, meta, channels):
		self.meta = meta
		self.channels = channels

def LoadSonarFile(file_name, regen_cache=True):
	logger.info('Loading sonar file: %s', file_name)
	# Load the pre-cached NetCDF files if they exist
	dir_name, base_name = os.path.split(file_name)
	cache_name = os.path.join(dir_name, 'cache', base_name)
	
	dirty_file_name = cache_name + '.cache.dirty'
	dirty = os.path.isfile(dirty_file_name)
	files = glob.glob(glob.escape(cache_name) + '.cache.*.nc')
	if regen_cache or dirty or len(files) == 0:

		if not os.path.isdir(os.path.dirname(dirty_file_name)):
			os.makedirs(os.path.dirname(dirty_file_name))

		import pathlib
		pathlib.Path(dirty_file_name).touch()

		logger.info('Generating NetCDF cache')
		for f in files: os.remove(f)

		# If not, then we will generate parquet files from the sl2 file
		loader = DataFrameLoader(file_name, cache_name)
		LoadRawLowranceLog(file_name, loader.OnBlock)
		loader.FinishSegment()
		files = glob.glob(glob.escape(cache_name) + '.cache.*.nc')
		os.remove(dirty_file_name)

	data = xarray.open_mfdataset(files, combine='by_coords', parallel=True, engine='netcdf4')
	data.attrs['file_name'] = file_name
	data.attrs['cache_name'] = cache_name
	return data


