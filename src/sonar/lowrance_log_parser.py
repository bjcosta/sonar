# See info about the sl2 file format at:
# * https://wiki.openstreetmap.org/wiki/SL2
# * https://github.com/kmpm/node-sl2format/blob/master/doc/sl2fileformat.md
# * https://www.geotech1.com/forums/showthread.php?11159-Lowrance-MCC-saved-data-structure
# * https://github.com/Chris78/sl2decode/blob/master/sl2decode.rb
# * https://github.com/Chris78/sl2decode
#
# Also has sl2+sl3 and extra comments about the field meanings, includes encoder as well:
# https://github.com/risty/SonarLogApi/tree/master/SonarLogAPI/Lowrance
#
# Note: I think all info that appears public about the "flags" field is wrong.
# None of the decoders make use of the flags and I saw little correlation between
# the flags and changes in associated data. There are however some correlations
# like when the GPS values change that appear to correlate with a few flags etc


import os
import os.path
import glob
import struct
import math
import re

import numpy
import pandas
import dask
import dask.dataframe
import xarray
import json
import datetime

import pytz
import tzlocal

import logging
logger = logging.getLogger('lowrance_log_parser')


CACHE_VERSION = 3

PARAQUET_ENGINE='pyarrow'

# Max items in a single pandas.DataFrame, we split and use dask to handle them
MAX_SEGMENT_SIZE = 5 * 1024

# Max bytes of data we expect to see in the sonar data segment
SONAR_DATA_MAX_SIZE = 3072

FORMAT_SLG = 1
FORMAT_SL2 = 2
FORMAT_SL3 = 3

FORMAT_MAP = {
    1: 'slg',
    2: 'sl2',
    3: 'sl3',
}
def FormatToStr(format):
    if format not in FORMAT_MAP: 
        return 'UNK(' + str(format) + ')'
    return FORMAT_MAP[format]

FAMILY_MAP = {
    0: 'HDS 7',
    1: 'Elite 4 CHIRP',
}
def FamilyToStr(family):
    if family not in FAMILY_MAP: 
        return 'UNK(' + str(family) + ')'
    return FAMILY_MAP[family]

# Traditional Sonar
PRIMARY = 0

# Traditional Sonar
SECONDARY = 1

# DownScan Imaging
DOWNSCAN = 2
LEFT_SIDESCAN = 3
RIGHT_SIDESCAN = 4
COMPOSITE_SIDESCAN = 5
# 7,8 found in yens new HDS Live sonar log.
# 7 looks like a normal sonar or downscan or something
# but has 2x samples for each frame index
#
# 8 looks like some kinds of other non-image data
# maybe not even uint8 also has 2x samples for each frame index
# like channel 7
#
# All other channels have 1 sample per frame index so it is weird
THREE_DIMENSIONAL = 9

def ChannelToStr(id):
    if id == PRIMARY: return 'primary'
    elif id == SECONDARY: return 'secondary'
    elif id == DOWNSCAN: return 'downscan'
    elif id == LEFT_SIDESCAN: return 'left_sidescan'
    elif id == RIGHT_SIDESCAN: return 'right_sidescan'
    elif id == COMPOSITE_SIDESCAN: return 'composite_sidescan'
    else: return 'unknown' + str(id)

#Sonar transducer frequency
FREQUENCY_MAP = {
    0 : '200 KHz',
    1 : '50 KHz',
    2 : '83 KHz',
    3 : '455 KHz',
    4 : '800 KHz',
    5 : '38 KHz',
    6 : '28 KHz',
    7 : '130 KHz - 210 KHz',
    8 : '90 KHz - 150 KHz',
    9 : '40 KHz - 60 KHz',
    10 : '25 KHz - 45 KHz',
    # Any other value is treated like 200 KHz
}
def FrequencyToStr(freq):
    if freq not in FREQUENCY_MAP: 
        #logger.warning('Invalid frequency input: %s assuming 200 KHz', freq)
        return 'Unknown frequency value %s assume 200 KHz' % (freq)
    return FREQUENCY_MAP[freq]

# Included both forward/reverse because still trying to figure out
# what the flags mean. The public docs I saw didnt seem to match up
# with my sl2 files.

# From: https://github.com/risty/SonarLogApi/blob/master/SonarLogAPI/Lowrance/Frame.cs
#TrackValid = 0,
#Preset_0_1 = 1,
#Unknown0_2 = 2,
#PositionValid = 3,
#Unknown_0_4 = 4,
#CourseOrSpeed_0_5 = 5,
#SpeedValid = 6,
#Preset_0_7 = 7,
#Unknown1_0 = 8,
#AltitudeOrCourseOrSpeed_1_1 = 9,
#Unknown1_2 = 10,
#Unknown1_3 = 11,
#Unknown1_4 = 12,
#Unknown1_5 = 13,
#AltitudeValid = 14,
#HeadingValid = 15

FLAGS_MAP_FORWARD = {
    'course_over_ground_valid' : 1 << 15,
    'speed_water_valid' : 1 << 14,
    'flag_unknown01' : 1 << 13,
    'position_valid' : 1 << 12,
    'flag_unknown02' : 1 << 11,
    'temperature_valid' : 1 << 10,
    'speed_gps_valid' : 1 << 9,
    'flag_unknown03' : 1 << 8,
    'flag_unknown04' : 1 << 7,
    'flag_unknown05' : 1 << 6,
    'flag_unknown06' : 1 << 5,
    'flag_unknown07' : 1 << 4,
    'flag_unknown08' : 1 << 3,
    'flag_unknown09' : 1 << 2,
    'altitude_valid' : 1 << 1,
    'heading_valid' : 1 << 0,
}

FLAGS_MAP_REVERSE = {
    'course_over_ground_valid' : 1 << 0,
    'speed_water_valid' : 1 << 1,
    'flag_unknown01' : 1 << 2,
    'position_valid' : 1 << 3,
    'flag_unknown02' : 1 << 4,
    'temperature_valid' : 1 << 5,
    'speed_gps_valid' : 1 << 6,
    'flag_unknown03' : 1 << 7,
    'flag_unknown04' : 1 << 8,
    'flag_unknown05' : 1 << 9,
    'flag_unknown06' : 1 << 10,
    'flag_unknown07' : 1 << 11,
    'flag_unknown08' : 1 << 12,
    'flag_unknown09' : 1 << 13,
    'altitude_valid' : 1 << 14,
    'heading_valid' : 1 << 15,
}

# The settings below are used to help try and figure out /reverse engineer the purpose
# of the flags item in the sl2 file by correlating changes in data with
# true/false values in the flags.
#
# Other reverse engineering summaries say these flags mean XYZ_valid I.e. 
# when true we expect updates to the data for XYZ. So for example 
# position_valid indicates there should be valid data in the longitude/latitude
# fields. I have also noticed that most data fields tend to maintain previous values
# until updated. So you might get a speed_gps of 2km/h and it stays exactly the
# same value for a few data samples, then it changes to something else.
#
# I was expecting thus that the speed_gps_valid flag would be set to true
# when we see this change in the speed_gps value.
#
# Likewise more importantly, I should NEVER see the data for speed_gps change
# while speed_gps_valid = false. 
# 
# It turns out these assumptions are not correct which leads me to believe
# that these flags have different meanings than previously described

# Choose the bitmask allocation of names to bit
#FLAGS_MAP = FLAGS_MAP_FORWARD
FLAGS_MAP = FLAGS_MAP_REVERSE
def FlagsToStr(flags, sep=','):
    s = []
    for k,v in FLAGS_MAP.items():
        if v & flags: s.append(k + '=true')
        else: s.append(k + '=false')
    return sep.join(s)


def FeetToMeters(value): return value / 3.2808399

SECONDS_PER_MINUTE = 60
MINUTES_PER_HOUR = 60
def KnotsToKmph(value): return (value / 1.94385) / 1000 * SECONDS_PER_MINUTE * MINUTES_PER_HOUR
def RadiansToDegrees(rad): return rad * 180.0 / math.pi

# Copied from datashader.utils, importing datashader is very slow so we duplicate this here
# as we dont care about the rest of it
def lnglat_to_meters(longitude, latitude):
    """
    Projects the given (longitude, latitude) values into Web Mercator
    coordinates (meters East of Greenwich and meters North of the Equator).

    Longitude and latitude can be provided as scalars, Pandas columns,
    or Numpy arrays, and will be returned in the same form.  Lists
    or tuples will be converted to Numpy arrays.

    Examples:
       easting, northing = lnglat_to_meters(-40.71,74)

       easting, northing = lnglat_to_meters(np.array([-74]),np.array([40.71]))

       df=pandas.DataFrame(dict(longitude=np.array([-74]),latitude=np.array([40.71])))
       df.loc[:, 'longitude'], df.loc[:, 'latitude'] = lnglat_to_meters(df.longitude,df.latitude)
    """
    if isinstance(longitude, (list, tuple)):
        longitude = numpy.array(longitude)
    if isinstance(latitude, (list, tuple)):
        latitude = numpy.array(latitude)

    origin_shift = numpy.pi * 6378137
    easting = longitude * origin_shift / 180.0
    northing = numpy.log(numpy.tan((90 + latitude) * numpy.pi / 360.0)) * origin_shift / numpy.pi
    return (easting, northing)


# Implemented just doing reverse of lnglat_to_meters
def meters_to_lnglat(easting, northing):
    if isinstance(easting, (list, tuple)):
        easting = numpy.array(easting)
    if isinstance(northing, (list, tuple)):
        northing = numpy.array(northing)

    origin_shift = numpy.pi * 6378137
    longitude = easting / origin_shift * 180.0
    latitude = (numpy.arctan(numpy.exp(northing / origin_shift * numpy.pi)) / numpy.pi * 360.0) - 90.0
    
    return (longitude, latitude)


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
    # correct thing not just copy from others something that only worked for the northern hemisphere
    if equatorial_latitude == -90.0 and not is_northern_hemisphere:
        return UniversalTransverseMercatorToWGS84EquatorialLatitude(polar_latitude, True)
    return equatorial_latitude



# Copied from datashader.utils, importing datashader is very slow so we duplicate this here
# as we dont care about the rest of it
def LowranceLongitudeToWebMercator(longitude):
    longitude = UniversalTransverseMercatorToWGS84EquatorialLongitude(longitude)
    origin_shift = numpy.pi * 6378137
    easting = longitude * origin_shift / 180.0
    return easting

def LowranceLatitudeToWebMercator(latitude):
    latitude = UniversalTransverseMercatorToWGS84EquatorialLatitude(latitude)
    origin_shift = numpy.pi * 6378137
    northing = numpy.log(numpy.tan((90 + latitude) * numpy.pi / 360.0)) * origin_shift / numpy.pi
    return northing




#Ideally I would like to define the parsing in a single location/definition that we use and is fast
#Something like:
class Field:
    def __init__(self, offset, struct_format, name, dtype, attrs={}, conversion=None, description='', notes=''):
        self.offset = offset
        self.struct_format = struct_format
        self.name = name
        self.conversion = conversion
        self.dtype = None
        if dtype is not None:
            self.dtype = numpy.dtype(dtype)
        self.attrs = attrs
        self.description = description
        self.notes = notes
        
        if self.conversion is None:
            self.conversion = lambda a : a

def GetIfNone(f, inp, name):
    if inp is not None: 
        return inp
    return getattr(f, name)
    
def FieldCopy(src, offset, name, struct_format=None, dtype=None, attrs=None, conversion=None, description=None, notes=None):
    for f in src:
        if f.name == name:
            struct_format = GetIfNone(f, struct_format, 'struct_format')
            dtype = GetIfNone(f, dtype, 'dtype')
            attrs = GetIfNone(f, attrs, 'attrs')
            conversion = GetIfNone(f, conversion, 'conversion')
            description = GetIfNone(f, description, 'description')
            notes = GetIfNone(f, notes, 'notes')
            return Field(offset, struct_format, name, dtype, attrs, conversion, description, notes)
    raise Exception('Failed to copy as no field matching name: %s' % (name))


MSEC_TO_NSEC = numpy.uint64(1000000)
def ToDatetime64ns(arg):
    # Is in milliseconds (except the first value)
    return (numpy.uint64(arg) * MSEC_TO_NSEC).astype('datetime64[ns]')

slX_header_parser = [
    Field(0, 'H', 'format', 'uint16', description='Lowrance sonar log file format', notes='One of 1:slg, 2:sl2, 3:sl3'),
    
    # Not sure what options exist and mean here. So far have seen:
    # 0 = HDS 7 (Not sure where i got this info from)
    # 1 = Elite 5 CHIRP (My unit reports this, not sure what others do)
    # 2 = HDS Live 7 (Yens unit reports this, not sure what others do)
    Field(2, 'H', 'family', 'uint16', description='Lowrance product hardware version'),

    # Not sure about this. So far have seen:
    # 1970=Downscan #b207
    # 3200=Sidescan #800c
    Field(4, 'H', 'block_size', 'uint16', description='Sonar type or blocksize'),
    
    # Supposed to be always 0 according to sl2, but in sl3 logs I have seen it has value of 1
    #Field(6, 'H', 'reserved', 'uint16'),
    
    # Mark the end of the fixed length struct without requiring a value decoded from it
    Field(8, None, None, None)
]

sl2_block_parser = [
    # FrameOffset = 0, //int, 4 bytes : https://github.com/risty/SonarLogApi/tree/master/SonarLogAPI/Lowrance
    # Used sometimes to detect we are in a valid offset for decoding
    # The last block in some log files (when sonar crashed) this has all zeros
    Field(0,   'I', 'block_offset',        'uint32', description='The offset in bytes from start of file this block starts'),

    #LastPrimaryChannelFrameOffset = 4, //int, 4 bytes : https://github.com/risty/SonarLogApi/tree/master/SonarLogAPI/Lowrance
    #last_primary_channel_block_offset

    #LastSecondaryChannelFrameOffset = 8, //int, 4 bytes : https://github.com/risty/SonarLogApi/tree/master/SonarLogAPI/Lowrance
    #last_secondary_channel_block_offset

    #LastDownScanChannelFrameOffset = 12, //int, 4 bytes : https://github.com/risty/SonarLogApi/tree/master/SonarLogAPI/Lowrance
    #last_downscan_channel_block_offset

    #LastSidescanLeftChannelFrameOffset = 16, //int, 4 bytes : https://github.com/risty/SonarLogApi/tree/master/SonarLogAPI/Lowrance
    #last_left_sidescan_channel_block_offset

    #LastSidescanRightChannelFrameOffset = 20, //int, 4 bytes : https://github.com/risty/SonarLogApi/tree/master/SonarLogAPI/Lowrance
    #last_right_sidescan_channel_block_offset

    #LastSidescanCompositeChannelFrameOffset = 24, //int, 4 bytes : https://github.com/risty/SonarLogApi/tree/master/SonarLogAPI/Lowrance
    #last_composite_sidescan_channel_block_offset

    #ThisFrameSize = 28, //short, 2 bytes. Bytes to next frame Start : https://github.com/risty/SonarLogApi/tree/master/SonarLogAPI/Lowrance
    Field(28,  'H', 'current_block_bytes', 'uint16', description=''),

    #PreviousFrameSize = 30, //short, 2 bytes. Bytes to previous frame Start
    #last_block_bytes

    #ChannelType = 32,//short, 2 bytes
    Field(32,  'H', 'channel',             'uint16', description='Identifies type of sonar data like primary, side-scan, downscan etc'),

    #PacketSize = 34,//short, 2 bytes : https://github.com/risty/SonarLogApi/tree/master/SonarLogAPI/Lowrance
    Field(34,  'H', 'data_size',           'uint16', description='Size of sounding/bounce data'),

    #FrameIndex = 36,//int, 4 bytes
    Field(36,  'I', 'frame_index',         'uint32', description='Starts at 0. Used to match frames/block on different channels.'),

    #UpperLimit = 40,//float, 4 bytes
    Field(40,  'f', 'upper_limit',         'float32', {'units': 'meters'}, FeetToMeters, description=''),

    #LowerLimit = 44,//float, 4 bytes
    Field(44,  'f', 'lower_limit',         'float32', {'units': 'meters'}, FeetToMeters, description=''),

    #Frequency = 50,//byte
    Field(50,  'B', 'sonar_frequency',     'uint8', description='Sonar frequency'),

    #CreationDataTime = 60,//int, 4 bytes, 
    # value in first frame = Unix time stamp of file creation.
    # other frames - time from device boot.
    Field(60,  'I', 'datetime',            'datetime64[ns]', {}, ToDatetime64ns, description='Unix timestamp of file creation'),

    # Depth = 64,//float, 4 bytes
    Field(64,  'f', 'water_depth',         'float32', {'units': 'meters'}, FeetToMeters, description='Depth under water surface(transponder)'),
    
    # Depth under keel
    # KeelDepth = 68,//float, 4 bytes
    
    #SpeedGps = 100,//float, 4 bytes
    # @todo Validate speed correct, one says m/s we want k/h original said in knots
    Field(100, 'f', 'speed_gps',           'float32', {'units': 'km/h'}, KnotsToKmph, description='Speed from GPS in km/h'),

    #Temperature = 104,//float, 4 bytes
    Field(104, 'f', 'temperature',         'float32', {'units': 'celsius'}, description='Temperature, in Celsius'),
    
    # IntLongitude = 108,//int, 4 bytes
    Field(108, 'I', 'longitude',           'float32', {'units': 'meters', 'coordinate system': 'Web Mercator'}, LowranceLongitudeToWebMercator, description=''),
    
    # IntLatitude = 112,//int, 4 bytes
    Field(112, 'I', 'latitude',            'float32', {'units': 'meters', 'coordinate system': 'Web Mercator'}, LowranceLatitudeToWebMercator, description=''),
    
    # If such a sensor is not present, it takes the value from "Speed" (GPS NMEA data) 
    # and sets WaterSpeedValid to false.
    #WaterSpeed = 116,//float, 4 bytes
    Field(116, 'f', 'speed_water',         'float32', {'units': 'kmph'}, KnotsToKmph, description='WaterSpeed in m/s. This value is taken from an actual Water Speed Sensor (such as a paddle-wheel).'),

    #CourseOverGround = 120,//float, 4 bytes
    Field(120, 'f', 'course_over_ground',  'float32', {'units': 'degrees'}, RadiansToDegrees, description='Track/Course-Over-Ground in radians. Real direction of boat movement. Taken from GPS NMEA data. '),
    
    #Altitude = 124,//float, 4 bytes
    Field(124, 'f', 'altitude',            'float32', {'units': 'meters'}, FeetToMeters, description='Altitude in meters. Taken from GPS NMEA data.'),

    #Heading = 128,//float, 4 bytes
    Field(128, 'f', 'heading',             'float32', {'units': 'degrees'}, RadiansToDegrees, description='Heading in radians. Angle in radians between magnetic north and transducer.'),

    #Flags = 132, // two bytes
    #Field(132, 'H', 'flags',               'uint16',  {}, description=''),

    # I validated that this appears in logs from my device to be msec since the device
    # booted. I.e. Not start of log as it doesnt start at t=0
    # TimeOffset = 140,//int, 4 bytes.
    Field(140, 'I', 'time_offset',           'uint32',  {}, description='Duration since device boot in msec'), 

    # Mark the end of the fixed length struct without requiring a value decoded from it
    Field(144, None, None, None)

    # Contains sounding/bounce data
    #SoundedData = 144// bytes array, size of PacketSize
]

sl3_block_parser = [
    #FrameOffset = 0,//int, 4 bytes
    FieldCopy(sl2_block_parser, 0,   'block_offset'),

    #ThisFrameSize = 8, //short, 2 bytes. Bytes to next frame Start : https://github.com/risty/SonarLogApi/tree/master/SonarLogAPI/Lowrance
    FieldCopy(sl2_block_parser, 8,  'current_block_bytes'),

    #PreviousFrameSize = 10,//short, 2 bytes. Bytes to previous frame Start
    #last_block_bytes

    #ChannelType = 12,//short, 2 bytes
    FieldCopy(sl2_block_parser, 12,  'channel'),

    #FrameIndex = 16,//int, 4 bytes
    FieldCopy(sl2_block_parser, 16,  'frame_index'),

    #UpperLimit = 20,//float, 4 bytes
    FieldCopy(sl2_block_parser, 20,  'upper_limit'),

    #LowerLimit = 24,//float, 4 bytes
    FieldCopy(sl2_block_parser, 24,  'lower_limit'),

    #CreationDataTime = 40,//int, 4 bytes, value at fist frame = Unix time stamp of file creation. if GPS cant find position value will be "-1"    # value in first frame = Unix time stamp of file creation.
    #//other frames - time in milliseconds from device boot.
    FieldCopy(sl2_block_parser, 40,  'datetime'),

    #PacketSize = 44,//short, 2 bytes
    FieldCopy(sl2_block_parser, 44,  'data_size'),

    #Depth = 48,//float, 4 bytes
    FieldCopy(sl2_block_parser, 48,  'water_depth'),

    #Frequency = 52,//byte
    FieldCopy(sl2_block_parser, 52,  'sonar_frequency'),

    #SpeedGps = 84,//float, 4 bytes
    FieldCopy(sl2_block_parser, 84,  'speed_gps'),

    #Temperature = 88,//float, 4 bytes
    FieldCopy(sl2_block_parser, 88,  'temperature'),

    #IntLongitude = 92,//int, 4 bytes
    FieldCopy(sl2_block_parser, 92,  'longitude'),

    #IntLatitude = 96,//int, 4 bytes
    FieldCopy(sl2_block_parser, 96,  'latitude'),

    #WaterSpeed = 100,//float, 4 bytes
    FieldCopy(sl2_block_parser, 100, 'speed_water'),

    #CourseOverGround = 104,//float, 4 bytes
    FieldCopy(sl2_block_parser, 104, 'course_over_ground'),

    #Altitude = 108,//float, 4 bytes
    FieldCopy(sl2_block_parser, 108, 'altitude'),

    #Heading = 112,//float, 4 bytes
    FieldCopy(sl2_block_parser, 112, 'heading'),

    #Flags = 116, // two bytes
    #FieldCopy(sl2_block_parser, 116, 'flags'),

    #TimeOffset = 124,//int, 4 bytes, time in milliseconds from log file creation.
    FieldCopy(sl2_block_parser, 124, 'time_offset'),


    # ... Need extra fields ...

    # Mark the end of the fixed length struct without requiring a value decoded from it
    Field(144, None, None, None)

    # Data starts at different locations for channel 7,8 and others
    # others start at 168, channels 7,8 start at 128
    #SoundedData = 168// bytes array
]


# Only used for certain channels as overlaps sonar data otherwise on some of the newer channels
# Newer channels 7 & 8 dont seem to have this block and also have 2x blocks per frame_index
sl3_extra_offsets_parser = [
    #LastPrimaryChannelFrameOffset = 128, //int, 4 bytes
    #last_primary_channel_block_offset

    #LastSecondaryChannelFrameOffset = 132, //int, 4 bytes
    #last_secondary_channel_block_offset

    #LastDownScanChannelFrameOffset = 136, //int, 4 bytes
    #last_downscan_channel_block_offset

    #LastSidescanLeftChannelFrameOffset = 140, //int, 4 bytes
    #last_left_sidescan_channel_block_offset

    #LastSidescanRightChannelFrameOffset = 144, //int, 4 bytes
    #last_right_sidescan_channel_block_offset

    #LastSidescanCompositeChannelFrameOffset = 148,//int, 4 bytes
    #last_composite_sidescan_channel_block_offset

    #LastThreeDChannelFrameOffset = 164,//int, 4 bytes
    #last_3d_channel_frame_offset

    # Mark the end of the fixed length struct without requiring a value decoded from it
    Field(168, None, None, None)
]

def ToStruct(format):
    struct_format = '<'
    current_offset = 0
    for field in format:
        # MUST be in offset order
        assert(field.offset >= current_offset)
        
        # Add any unused padding bytes to move to the next offset
        struct_format += 'x' * (field.offset - current_offset)
        current_offset = field.offset
        
        # Simply moves to the next field after setting the current_offset
        if field.struct_format is None:
            assert(field.name is None)
            assert(field.dtype is None)
            continue

        # Add the new field
        size = struct.calcsize(field.struct_format)
        struct_format += field.struct_format
        current_offset += size
    logger.debug('Creating struct with format: %s', struct_format)
    return struct.Struct(struct_format)

class DecodedObject:
    def __repr__(self): return "{}({!r})".format(self.__class__.__name__, self.__dict__)

def UnpackedToObject(format, unpacked):
    obj = DecodedObject()
    for i in range(0, len(format)):
        field = format[i]
        data = unpacked[i]
        value = field.dtype.type(field.conversion(data))
        setattr(obj, field.name, value)
    return obj

class ChannelData(object):
    def __init__(self, file_name, cache_name, channel, frame_index, format):
        self.file_name = file_name
        self.cache_name = cache_name
        self.channel = channel
        self.format = format
        self.current_segment = 0
        
        # Used for mapping new channels when seeing a duplicate frame index
        # Stores the frame_index we saw last for this channel
        self.last_frame_index = frame_index
        self.last_frame_index_count = 1

        self.data = DecodedObject()
        for field in self.format:
            value = numpy.zeros(MAX_SEGMENT_SIZE, dtype=field.dtype)
            setattr(self.data, field.name, value)
        self.data.data = numpy.zeros((MAX_SEGMENT_SIZE, SONAR_DATA_MAX_SIZE), dtype='uint8')
        self.data_size = 0
    
    def AddBlock(self, block):
        for field in self.format:
            value = getattr(self.data, field.name)
            value[self.data_size] = getattr(block, field.name)
        # In xarray we will pre-allocate to SONAR_DATA_MAX_SIZE and use data_size to know how much of it is in use
        self.data.data[self.data_size][0:len(block.data)] = block.data
        self.data.data[self.data_size][len(block.data):] = numpy.zeros(SONAR_DATA_MAX_SIZE - len(block.data), dtype='uint8')
        self.data_size += 1
        if self.data_size >= MAX_SEGMENT_SIZE:
            self.FinishSegment()
    
    def FinishSegment(self):
        if self.data_size > 0:
            ds_dict = {}
            for field in self.format:
                value = getattr(self.data, field.name)
                ds_dict[field.name] = (
                    ['channel', 'frame_index'],
                    [value[0:self.data_size]],
                    field.attrs
                )

            # @todo Not sure exactly what units the sonar measures in, would be good to know though
            ds_dict['data'] = (
                ['channel', 'frame_index', 'depth_bin'], 
                [self.data.data[0:self.data_size]], 
                {'units': 'amplitude'}
            ) 

            # Using these as a coords instead of vars
            del ds_dict['channel']
            del ds_dict['frame_index']
            
            # Really dont want this in the exported data is only required for parsing the file
            del ds_dict['current_block_bytes']
            
            # @todo Can we make data_size a coord as well like channel as a single dimension per channel?
            ds = xarray.Dataset(ds_dict, coords={
                'depth_bin': (['depth_bin'], range(0, len(self.data.data[0]))),
                'frame_index': (['frame_index'], self.data.frame_index[0:self.data_size]),
                'channel': (['channel'], [self.channel])
            })
            
            netcdf_file_name = self.cache_name + '.cache.chan' + str(self.channel) + '.' + str(self.current_segment) + '.nc'
            #logger.debug('Writing segment to file: %s with index range (%s - %s)', netcdf_file_name, self.next_data_index - len(self.data), self.next_data_index)
        
            if not os.path.isdir(os.path.dirname(netcdf_file_name)):
                os.makedirs(os.path.dirname(netcdf_file_name))
            ds.to_netcdf(netcdf_file_name, engine='netcdf4')
        self.current_segment += 1
        self.data_size = 0

class LogData(object):
    def __init__(self, file_name, cache_name, cache_meta):
        self.file_name = file_name
        self.cache_name = cache_name
        self.channel_data = {}
        self.cache_meta = cache_meta
        self.first_datetime = None
    
    def FinishSegment(self):
        for channel, channel_data in self.channel_data.items():
            channel_data.FinishSegment()
    
    def OnHeader(self, header):
        logger.debug('header: %s', header)
        self.cache_meta['format'] = FormatToStr(header.format)
        self.cache_meta['family'] = FamilyToStr(header.family)
        self.cache_meta['block_size'] = int(header.block_size)

    def OnFirstBlock(self, block, unpacked_block, format):
        # According to: https://github.com/risty/SonarLogApi/tree/master/SonarLogAPI/Lowrance
        # First item in block.datetime is secs since epoc
        # However sometimes it has value -1 and then secs since epoc
        # is first item in time_offset. I think this description is
        # incorrect, at least for my device as I see -1 in first item
        # but time_offset always seems to be msec since device boot.
        #
        # Docs say from: https://github.com/risty/SonarLogApi/tree/master/SonarLogAPI/Lowrance
        # * datetime:
        #       value in fist frame = Unix time stamp of file creation.
        #       other frames - time from device boot.
        # * time_offset
        #       if creation data(CreationDataTime) in fist frame exist(not -1) - time from device boot?
        #       time1 : first value contains ms since 1970 if multiplied by 1000, 
        #       consecutive values have time encoded somehow different
        #
        # Note: In the small.sl2 example I have the datetime first item looks more like an offset
        # So we really need a lot more logs from different units to be sure what to do here
        local_timezone = tzlocal.get_localzone()
        
        # We multiple by 1000000 in conv function to convert from msec to nsec
        # We will undo that here so the first value makes sense
        # As it is a UTC offset of UINT32_MAX
        incoming_first_datetime32ms = numpy.uint64(block.datetime.astype('uint64') / MSEC_TO_NSEC)
        incoming_first_datetime64ns = block.datetime
        block.datetime = numpy.uint64(0).astype('datetime64[ns]')
        
        # I think UINT32_MAX means it doesnt know. So I will:
        # Check the name of the file for a date-time and if cant
        # find that then use the OS timestamp of the file
        #
        # I will search for one of two formats:
        # * Chart 21_02_2000 [0].2019_10_07_13_16_00+1100.Shellharbour.sl2
        # * Yens_Sonar_2020-07-10_06.37.12.sl3
        #
        # The first is my own naming format used for local logs I collected on my sonar, 
        # the second is what lowrance uses on newer units or at least from Yens HDS Live 7
        time_source_msg = ''
        if self.first_datetime is None:
            m = re.search(r'(?P<datetime>\d\d\d\d_\d\d_\d\d_\d\d_\d\d_\d\d[^\.]*)', self.file_name)
            if m:
                self.first_datetime = datetime.datetime.strptime(m['datetime'], '%Y_%m_%d_%H_%M_%S%z')
                time_source_msg = 'parsing the source filename with datetime: %s' % (self.first_datetime)

        if self.first_datetime is None:
            m = re.search(r'(?P<datetime>\d\d\d\d-\d\d-\d\d_\d\d.\d\d.\d\d)(?P<timezone>[\+\-0-9]*)', self.file_name)
            if m:
                if m.group('timezone') != '':
                    self.first_datetime = datetime.datetime.strptime(m['datetime']+m['timezone'], '%Y-%m-%d_%H.%M.%S%z')
                else:
                    logger.warning('Assuming local time for generation of file: %s (Please add timezone details to file name if possible)', self.file_name)
                    self.first_datetime = datetime.datetime.strptime(m['datetime'], '%Y-%m-%d_%H.%M.%S').replace(tzinfo=local_timezone)
                time_source_msg = 'parsing the lowrance filename with datetime: %s' % (self.first_datetime)

        if self.first_datetime is None:
            # This is low down in the priority as it doesnt seem to be correct
            # and is very different on all sorts of units
            if incoming_first_datetime32ms != numpy.iinfo(numpy.uint32).max:
                logger.debug('Decoded datetime from sonar log content is: %s (Not always reliable we may use file creation time instead)', incoming_first_datetime64ns)
                logger.debug('Decoded time_offset from sonar log content is: %s (Not always reliable we may use file creation time instead)', block.time_offset)
                
                # Many of these just have very small values that are not correct (these sonars didnt exist in 1970)
                # @todo Use a proper date to make it clear what the threshold is for now just hard code to something largish
                if incoming_first_datetime32ms > 1000 * 60 * 60 * 24 * 365 * 10:
                    self.first_datetime = incoming_first_datetime64ns
                    time_source_msg = 'first block entry in sonar log with value: %s' % (self.first_datetime)


        if self.first_datetime is None:
            # Note: I dont think i really see the sonar file creation time correctly ever
            # Mine is near random as I think there is a datetime wrapping issue
            # Yens we see:
            # * Filename says:               2020-07-10 06:37:12 (I added a +1000)
            # * Windows file system says: FS 2020-07-09 22:22
            # 
            # If using the windows FS assuming it is in UTC, we get an offset of 8 hours
            # This is not the Sydney timezone where it was recorded of 10 hours
            # 
            # So for now we will leave the code below.
            # This code will basically ensure that we assume local filesystem local time
            # and support thinkgs like downloading a new file.
            mtime = os.path.getmtime(self.file_name)
            self.first_datetime = datetime.datetime.fromtimestamp(mtime, local_timezone)
            #self.first_datetime = self.first_datetime.replace(tzinfo=datetime.timezone.utc)
            time_source_msg = 'OS file creation time with value: %s' % (self.first_datetime)

        epoch = datetime.datetime.utcfromtimestamp(0).replace(tzinfo=datetime.timezone.utc)
        self.first_datetime = numpy.uint64((self.first_datetime - epoch).total_seconds()) * numpy.uint64(1000000000)
        self.first_datetime = self.first_datetime.astype('datetime64[ns]')
    
        # Convert to a timedelta so we can easily add it to a datetime64[ns]
        self.first_datetime = self.first_datetime.astype('timedelta64[ns]')
        #local = datetime.datetime.fromtimestamp(float(self.first_datetime.astype('uint64') / numpy.uint64(1000000000)), local_timezone)
        utc = datetime.datetime.fromtimestamp(float(self.first_datetime.astype('uint64') / numpy.uint64(1000000000)), pytz.utc)
        logger.info('Datetime used for log is: %s (UTC) obtained from %s', utc, time_source_msg)

        #import code
        #code.interact(local=dict(globals(), **locals()))

        self.OnBlock(block, unpacked_block, format)
        
    def OnBlock(self, block, unpacked_block, format):
        block.datetime = self.first_datetime + block.datetime
        
        # if creation data(CreationDataTime) in fist frame exist(not -1) - time from device boot?
        # time1 : first value contains ms since 1970 if multiplied by 1000, 
        # consecutive values have time encoded somehow different
        
        #import code
        #code.interact(local=dict(globals(), **locals()))

        #logger.info('block: %s', block)
        if block.longitude == 0.0 or block.latitude == -90.0:
            logger.warning('Strange lowrance log file: %s, Skipping block with frame_index: %s has invalid GPS coordinates: %s, %s', self.file_name, block.frame_index, block.longitude, block.latitude)
            return

        # Ideally xarray would handle duplicates for us, but seems it doesnt
        # like duplicates very much. So we will map duplicate frame_index's to 
        # new channels

        # For this strategy to work we require all channels read from sonar log have values <=255 or we will need to extend the bit width of the channel
        assert(block.channel <= 255)
        if block.channel in self.channel_data:
            # When we create a channel we create it with a frame index
            assert(self.channel_data[block.channel].last_frame_index is not None)

            # We always expect frame_index for the same channel to increment or repeat never go backwards. I.e. Is ordered
            # If it is not ordered and this assertion fails, then we will need a much more complex solution where we store
            # a map of seen frame indexes and their duplicates which will grow through reading a sl2 file
            assert(block.frame_index >= self.channel_data[block.channel].last_frame_index)

            # If this is a duplicate for this channel, then we will create a new channel for it
            if block.frame_index == self.channel_data[block.channel].last_frame_index:
                # channel is uint16, currently lowrance use values from 0 - 9
                # We will reserve the low byte for lowrance, and we will use the high byte for duplicates
                # The first item with 0 duplicates, we will place a 0 in the top byte thus == lowrance default channel
                # When we have 1 duplicates we will place a 1 in the top byte thus a new channel
                # When we have 2 duplicates we will place a 2 in the top byte thus a new channel
                # ...
                
                # Lets allocate new channels by using the upper byte
                # So each duplicate found in order 
                self.channel_data[block.channel].last_frame_index_count += 1
                upper_byte = numpy.left_shift(numpy.uint16(self.channel_data[block.channel].last_frame_index_count - 1), 8)
                
                # This strategy can only handle upto 255 duplicate frames for a single channel
                assert(self.channel_data[block.channel].last_frame_index_count <= 255)
                
                # Modify the channel id
                block.channel = numpy.bitwise_or(upper_byte, numpy.uint16(block.channel))

            else:
                # New index
                self.channel_data[block.channel].last_frame_index_count = 1
                self.channel_data[block.channel].last_frame_index = block.frame_index

        if block.channel not in self.channel_data:
            logger.info('New channel %s found in lowrance log file starting at index: %s with sonar data size: %s', ChannelToStr(block.channel), block.frame_index, block.data_size)
            self.channel_data[block.channel] = ChannelData(self.file_name, self.cache_name, block.channel, block.frame_index, format)


        self.channel_data[block.channel].AddBlock(block)

def LoadLowranceLog(file_name, cache_name, cache_meta):
    log_data = LogData(file_name, cache_name, cache_meta)
    
    file_size = os.path.getsize(file_name)
    logger.info('Loading lowrance log file: %s of size: %s', file_name, file_size)
    with open(file_name, mode='rb') as file:
        # Read the header of the sl2/sl3 file
        header_struct = ToStruct(slX_header_parser)
        data = file.read(header_struct.size)
        unpacked_header = header_struct.unpack_from(data)
        
        # Exclude the null at the end so UnpackedToObject doesnt need to check null in loop
        header = UnpackedToObject(slX_header_parser[:-1], unpacked_header)
        log_data.OnHeader(header)
        
        # Based on the type of header, we will create a block decoder
        block_struct = None
        if header.format == FORMAT_SL2:
            logger.info('Decoding as SL2 lowrance log file')
            block_parser = sl2_block_parser
            
        elif header.format == FORMAT_SL3:
            logger.info('Decoding as SL3 lowrance log file')
            block_parser = sl3_block_parser

        else:
            raise Exception('Unsupported lowrance file format: %s', header.format)
        
        # Get these as separate fields is slightly faster
        block_struct = ToStruct(block_parser)
        block_struct_size = block_struct.size
        block_struct_unpack = block_struct.unpack
        start_block = file.tell()
        
        count = 0
        
        # We will handle the first block specially so some setup can be done without if statements every block read
        # The extra if statements add significant cost in the decoding each time
        count += 1
        data = file.read(block_struct_size)
        if data: 
            unpacked_block = block_struct_unpack(data)

            # Exclude the null item at the end so our for loops dont need to check it
            block = UnpackedToObject(block_parser[:-1], unpacked_block)
            if block.block_offset != 0 and block.current_block_bytes != 0:
                assert(block.block_offset == start_block)

                # Read sonar data directly into numpy array
                # It is variable length and so not handled by the struct above
                assert(block.data_size < block.current_block_bytes)
                file.seek(start_block + block.current_block_bytes - block.data_size)
                block.data = numpy.fromfile(file, dtype='uint8', count=block.data_size)

                log_data.OnFirstBlock(block, unpacked_block, block_parser[:-1])
                
                file.seek(start_block + block.current_block_bytes)
                start_block += block.current_block_bytes
        
        percent_complete = 0
        while True:
            count += 1
            data = file.read(block_struct_size)
            if not data: 
                logger.debug('Block: %s Failed to read %s bytes from file', count, block_struct_size)
                break

            new_percent_complete = int(100 * int(start_block) / file_size)
            if new_percent_complete != percent_complete:
                percent_complete = new_percent_complete
                logger.info ('%s %%' % (new_percent_complete))

            unpacked_block = block_struct_unpack(data)

            # Exclude the null item at the end so our for loops dont need to check it
            block = UnpackedToObject(block_parser[:-1], unpacked_block)
            if block.block_offset == 0:
                if block.current_block_bytes == 0:
                    logger.debug('Last block %s has offset/size of 0. Treating as EOF. Read: %s of %s', count, file.tell(), file_size)
                    break
                
                logger.warning('Skipping block: %s with zero offset: %s', count, block)
                file.seek(start_block + block.current_block_bytes)
                start_block += block.current_block_bytes
                continue
            assert(block.block_offset == start_block)
            
            
            # Read sonar data directly into numpy array
            # It is variable length and so not handled by the struct above
            assert(block.data_size < block.current_block_bytes)
            file.seek(start_block + block.current_block_bytes - block.data_size)
            block.data = numpy.fromfile(file, dtype='uint8', count=block.data_size)
            
            # @todo For SL3 we may need to look at channel and do remaining block decode depending on channel value
            # I.e. SL3 from Yen uses 40 more bytes for sonar data instead of other unnecessary fields used in previous formats
            # It may be simpler/faster to just delete the extra fields though as they are not valid
            #if header.format == FORMAT_SL3:
            
            # Exclude the null item at the end so our for loops dont need to check it
            log_data.OnBlock(block, unpacked_block, block_parser[:-1])
            
            file.seek(start_block + block.current_block_bytes)
            start_block += block.current_block_bytes
        log_data.FinishSegment()


def LoadSonarFile(file_name, regen_cache=True):
    '''Loads a specified lowrance sonar log file and produces a NetCDF cache file for it to speed up loading in future requests'''
    
    logger.info('Loading sonar file: %s', file_name)
    # Load the pre-cached NetCDF files if they exist
    dir_name, base_name = os.path.split(file_name)
    gen_name = os.path.join(dir_name, 'gen')
    cache_name = os.path.join(gen_name, 'cache', base_name)
    
    cache_meta_name = cache_name + '.cache.meta.json'
    try:
        with open(cache_meta_name) as json_file:
            cache_meta = json.load(json_file)
    except:
        cache_meta = {}
    
    dirty_file_name = cache_name + '.cache.dirty'
    dirty = os.path.isfile(dirty_file_name)
    files = glob.glob(glob.escape(cache_name) + '.cache.*.nc')
    
    if regen_cache: 
        logger.info('Regenerating the lowrance log file cache as requested')

    elif dirty: 
        logger.info('Regenerating the lowrance log file cache as the existing cache is incomplete and was cancelled during a previous file load request')
        regen_cache = True

    elif len(files) == 0: 
        logger.info('Loading the lowrance log file from scratch and generating a cache as there is no existing cache files that speed up reload')
        regen_cache = True

    elif cache_meta.get('cache_version') != CACHE_VERSION: 
        logger.info('Regenerating the lowrance log file cache as the existing cache is old and the format has since changed')
        regen_cache = True
    
    # @todo Add checking of the file timestamp for files[0] compared to file_name
    
    else:
        logger.debug('The lowrance log file cache is up to date, using faster cache load')

    if regen_cache:
        cache_meta = {}
        
        logger.info('Erasing old cache files')
        for f in glob.glob(glob.escape(cache_name) + '*'): os.remove(f)

        logger.info('Creating cache dirty marker file: %s', dirty_file_name)
        if not os.path.isdir(os.path.dirname(dirty_file_name)):
            os.makedirs(os.path.dirname(dirty_file_name))
        import pathlib
        pathlib.Path(dirty_file_name).touch()

        # If not, then we will generate .nc files from the sl2 file
        LoadLowranceLog(file_name, cache_name, cache_meta)
        
        cache_meta['cache_version'] = CACHE_VERSION
        with open(cache_meta_name, 'w') as outfile:
            json.dump(cache_meta, outfile)
        
        files = glob.glob(glob.escape(cache_name) + '.cache.*.nc')
        os.remove(dirty_file_name)

    logger.debug('Reloading using xarray to permit parallel dask usage')
    data = xarray.open_mfdataset(files, combine='by_coords', parallel=True, engine='netcdf4')
    
    # Name of original file 
    data.attrs['file_name'] = file_name
    
    # Add the cache meta-data
    data.attrs['cache_meta_file_name'] = cache_meta_name
    data.attrs['meta'] = cache_meta
    
    # Name prefix for any cache files generated from the original sl2 file
    data.attrs['cache_name'] = cache_name
    
    # Name prefix used for any generated files like png,txt etc this is so we can ensure a consistent naming of outputs
    # however is optional to use
    data.attrs['gen_name'] = os.path.join(gen_name, base_name)
    
    if regen_cache:
        summary_file_name = data.attrs['gen_name'] + '.txt'
        logger.info('Summarize the per-channel data into: %s', summary_file_name)
        # Summarise data overall and per channel
        info = []
        for chan_id in [None] + [v for v in data.channel.values]:
            if chan_id is None:
                channel = data
                channel_name = 'COMBINED'
            else:
                channel = data.sel(channel=chan_id)
                channel_name = ChannelToStr(chan_id)

            for k in channel.keys():
                v = getattr(channel, k)
                mean = v.mean().values.item()
                if k == 'datetime':
                    # @todo There appears to be a bug in xarray calling min/max on 'datetime64[ns]'
                    if chan_id is None:
                        min = v.values[0][0]
                        max = v.values[-1][-1]
                    else:
                        min = v.values[0]
                        max = v.values[-1]
                else:
                    min = v.min().values.item()
                    max = v.max().values.item()
                range = max - min
                info.append('Channel:%s Key:%s min:%s mean:%s max:%s range:%s shape:%s' % (channel_name, k, min, mean, max, range, v.shape))
        
        # Some key info (also above) that we want nicer info about
        duration = data.time_offset.max().values.item() - data.time_offset.min().values.item()
        duration_sec = duration / 1000
        duration_min = duration_sec / 60
        info += [
            'Duration: %s minutes' % (duration_min),
            'Unique Data Points: %s' % (len(data.frame_index)),
            'Channels: %s' % (','.join([ChannelToStr(v) for v in data.channel.values])),
        ]
        with open(summary_file_name, 'w') as f:
            for i in info:
                #logger.info('%s', i)
                print (i, file=f)

    return data

