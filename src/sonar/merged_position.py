'''Provides features to merge multiple sonar logs by position to give spatially organized data across multiple recordings'''

# @todo Dont just restrict merging data to primary channel
# @todo rename to not just be depth but later more things 
# @todo Add list of contributors to the merged node maybe?
#	If so do we just id contributor file for file + index? Will get huge!
# @todo Filter depth values that introduce a significant gradient in bottom slope due to sonar error
# @todo Calc mean offset to account for tides / water level diffs for overlapping data
#	How to account for already merged data that had incorrect offset dur to bad or no overlap? I.e. merging islands of data

import sys
import os
import os.path
import math
import glob
import json

import numpy
import xarray
import holoviews
import holoviews.operation.datashader
import sonar.sl2_parser
import datashader
import bisect

import numpy
import scipy.spatial
import sonar.sl2_parser
import scipy.interpolate
import stl.mesh
import matplotlib.pyplot
import PIL

import logging
logger = logging.getLogger('merged_position')

ENABLE_DEBUG = False

def QuantizeLongitudeLatitude(longitude, latitude):
	'''Quantize the latitude/longitude to a consistent grid size
	
	This is must always quantize to something greater than cm accuracy
	as that is the limit of the merged position produced by LongitudeLatitudeToPosition
	
	The actual quantization at the moment is in a metre grid, though INT32_MAX
	am considering changing it to a 10 cm grid
	'''
	
	# This is not *required* but the GPS data is not that accurate and this greatly 
	# reduces the data points for processing. At the moment this is a 1m quantized grid
	# though I am considering changing it to a 10cm grid.
	
	longitude = numpy.int64(longitude)
	latitude = numpy.int64(latitude)
	return (longitude, latitude)

def LongitudeLatitudeToPosition(longitude, latitude):
	'''Converts a longitude/latitude into a single combined "position" value 
	used for ordering positions in a performant way to improve performance 
	of lookup/merging.
	
	Note: This will quantize the positions. The max resolution possible with the 
	64 bit data type is centimeter accuracy in web-mercator projection. 
	'''
	
	# According to: https://epsg.io/3857
	# Projected bounds are:
	# longitude: -20,026,376.39 to 20,026,376.39
	# latitude:  -20,048,966.10 to 20,048,966.10
	# 
	# INT32_MAX = 2,147,483,647
	#
	# So we can encode the lon/lat in a uint64 for comparison purposes and still have cm accuracy
	# 
	# Also will use a standard offset to force positive values for both lon/lat to make everything simpler.
	longitude, latitude = QuantizeLongitudeLatitude(longitude, latitude)

	OFFSET = numpy.int64(2004896700)

	signed_longitude_cm = numpy.int64(longitude * 100)
	unsigned_longitude_cm = numpy.uint64(signed_longitude_cm + OFFSET)
	merged_lon_lat = numpy.left_shift(unsigned_longitude_cm, numpy.uint64(32))
	
	signed_latitude_cm = numpy.int64(latitude * 100)
	unsigned_latitude_cm = numpy.uint64(signed_latitude_cm + OFFSET)
	merged_lon_lat = numpy.bitwise_or(merged_lon_lat, unsigned_latitude_cm)
	
	return merged_lon_lat

def ValidateMergedPositionData(data):
	'''Used to validate that the merged data only contains unique positions and is ordered correctly.
	
	Note: This is really only used for debugging and asserts on failure
	'''

	for v in data.contributors.values: assert(v != 0)
	
	# We expect it to be ordered by longitude then latitude
	last_longitude = None
	last_latitude = None
	last_position = None
	for index in range(0, len(data.position)):
		longitude = data.longitude.values[index]
		latitude = data.latitude.values[index]
		position = data.position.values[index]
		
		if last_longitude is None:
			last_longitude = longitude
			last_latitude = latitude
			last_position = position
			continue

		assert(last_position <= position)
		assert(last_longitude <= longitude)
		if last_longitude == longitude:
			assert(last_latitude <= latitude)

class MergedPosition(object):
	def __init__(self, max_len): 
		self.size = 0
		self.max_len = max_len
		self.longitude = numpy.zeros(max_len)
		self.latitude = numpy.zeros(max_len)
		self.depth = numpy.zeros(max_len)
		self.contributors = numpy.zeros(max_len, dtype=numpy.uint32)
		self.position = numpy.arange(0, max_len, dtype=numpy.uint64)
	
	def MergePoint(self, 
		new_position,
		new_longitude,
		new_latitude,
		new_depth,
		new_contributors,
		index,
		):
		
		existing_depth = self.depth[index]
		existing_count = self.contributors[index]

		merged_contributors = existing_count + new_contributors
		merged_depth = float(((existing_depth * existing_count) + new_depth)) / merged_contributors
		
		self.depth[index] = merged_depth
		self.contributors[index] = merged_contributors
		
		quantized_longitude, quantized_latitude = QuantizeLongitudeLatitude(new_longitude, new_latitude)
		
		# In some cases when merging, this should be unnecessary
		if existing_count == 0:
			self.longitude[index] = quantized_longitude
			self.latitude[index] = quantized_latitude
			self.position[index] = new_position
			
		else:
			assert(quantized_longitude == self.longitude[index])
			assert(quantized_latitude == self.latitude[index])
			assert(new_position == self.position[index])
	
	def ToXArray(self): 
		return xarray.Dataset({
			'longitude':   (['position'], self.longitude[0:self.size]),
			'latitude':    (['position'], self.latitude[0:self.size]),
			'depth':       (['position'], self.depth[0:self.size]),
			'contributors': (['position'], self.contributors[0:self.size]),
		}, coords={'position': (['position'], self.position[0:self.size])})
	
	
def MergePositionData(current_sonar_data, position_data):
	if ENABLE_DEBUG: ValidateMergedPositionData(position_data)
	new_unique_data = MergedPosition(max_len=len(current_sonar_data.position.values))
	new_merged_data = MergedPosition(max_len=len(current_sonar_data.position.values))
	old_merged_data = MergedPosition(max_len=len(current_sonar_data.position.values))

	# Used to improve performance as most common is merging into last position
	last_new_unique_position = None
	last_new_position = None
	
	# Also need to store the last index in position_data so we can speed up future searches
	last_position_data_index = 0
	for index in range(0, len(current_sonar_data.position.values)):

		# Get the next item to be merged
		new_position = current_sonar_data.position.values[index]
		new_longitude = current_sonar_data.longitude.values[index]
		new_latitude = current_sonar_data.latitude.values[index]
		new_depth = current_sonar_data.depth.values[index]
		new_contributors = current_sonar_data.contributors.values[index]
		
		# First see if it is the same as the last item in new_unique_data, as that will be very common and fast to find
		if last_new_unique_position == new_position:
			new_unique_data.MergePoint(
				new_position,
				new_longitude,
				new_latitude,
				new_depth,
				new_contributors,
				new_unique_data.size - 1
			)

		# Next see if it is the same as the last item in new_merged_data, as that will also be very common and fast to find
		elif last_new_position == new_position:

			# Note: We will merge into both new_merged_data and old_merged_data
			new_merged_data.MergePoint(
				new_position,
				new_longitude,
				new_latitude,
				new_depth,
				new_contributors,
				new_merged_data.size - 1
			)

			old_merged_data.MergePoint(
				new_position,
				new_longitude,
				new_latitude,
				new_depth,
				new_contributors,
				old_merged_data.size - 1
			)

		else:
			# Not the same as the last position merged into either new unique or new merged data arrays.
			# so we need to search position_data to see if it is a new item to do 
			# into either new_merged_data or new_unique_data

			# We will use a binary search.
			position_data_index = bisect.bisect_left(position_data.position.values, new_position, lo=last_position_data_index)
			already_exists_in_position_data = False
			if position_data_index < len(position_data.position.values) and position_data.position.values[position_data_index] == new_position:
				already_exists_in_position_data = True

				# Future searches no need to look earlier than this now, so update last_position_data_index
				last_position_data_index = position_data_index
			
			if already_exists_in_position_data:
				last_new_position = new_position

				new_merged_data.MergePoint(
					new_position,
					new_longitude,
					new_latitude,
					new_depth,
					new_contributors,
					new_merged_data.size
				)
				new_merged_data.size += 1

				# We need to copy the data from position_data.*.values[position_data_index]
				# into old_merged_data as this is the first time we have seen this position
				# and we want a value we can replace
				old_merged_data.MergePoint(
					position_data.position.values[position_data_index],
					position_data.longitude.values[position_data_index],
					position_data.latitude.values[position_data_index],
					position_data.depth.values[position_data_index],
					position_data.contributors.values[position_data_index],
					old_merged_data.size
				)
				
				# Then merge the new value into it
				old_merged_data.MergePoint(
					new_position,
					new_longitude,
					new_latitude,
					new_depth,
					new_contributors,
					old_merged_data.size
				)
				old_merged_data.size += 1

			else:
				last_new_unique_position = new_position

				# Add new entry in new_unique_position_data
				new_unique_data.MergePoint(
					new_position,
					new_longitude,
					new_latitude,
					new_depth,
					new_contributors,
					new_unique_data.size
				)
				new_unique_data.size += 1

	#import code
	#code.interact(local=dict(globals(), **locals()))

	# Convert the data read so far into an XArray
	sliced_new_unique = new_unique_data.ToXArray()
	sliced_new_merged = new_merged_data.ToXArray()
	sliced_old_merged = old_merged_data.ToXArray()
	
	if ENABLE_DEBUG: ValidateMergedPositionData(sliced_old_merged)
	if ENABLE_DEBUG: ValidateMergedPositionData(sliced_new_unique)
	if ENABLE_DEBUG: ValidateMergedPositionData(sliced_new_merged)

	logger.debug('Concat new unique items to this_data and sort them')
	this_data = xarray.concat([sliced_new_merged, sliced_new_unique], 'position')
	this_data = this_data.sortby(['position'])
	ValidateMergedPositionData(this_data)

	# replace items in position_data with those from sliced_old_merged
	# Apparently keeps items from first object so put sliced_old_merged first
	logger.debug('Concat new unique items to position_data and sort them')

	# Merge takes attrs from sliced_old_merged so we need to save/restore them specifically
	old_attrs = position_data.attrs
	
	# Keep items defined in sliced_old_merged and add remaining items not common from position_data
	updated_sonar_data = sliced_old_merged.combine_first(position_data)
	updated_sonar_data = xarray.concat([updated_sonar_data, sliced_new_unique], 'position')
	updated_sonar_data = updated_sonar_data.sortby(['position'])
	updated_sonar_data.attrs = old_attrs
	if ENABLE_DEBUG: ValidateMergedPositionData(updated_sonar_data)

	return (updated_sonar_data, this_data)


def MergeSonarLogByPosition(sonar_data, position_data):
	# We convert the input sonar_data into a format consumable by the MergePositionData() method and then pass it along

	# @todo No need to restrict to just primary channel for this
	channel = sonar_data.sel(channel=sonar.sl2_parser.PRIMARY)

	# Make sure the position_data contains the items we care about
	if 'depth' not in position_data.keys():
		# Initialize position_data if this is the first merged data
		ds = xarray.Dataset({
			'longitude': (['position'],  numpy.array([], dtype=numpy.float32)),
			'latitude': (['position'],  numpy.array([], dtype=numpy.float32)),
			'depth': (['position'],  numpy.array([], dtype=numpy.float32)),
			'contributors': (['position'],  numpy.array([], dtype=numpy.uint32)),
		}, coords={'position': (['position'], numpy.array([], dtype=numpy.uint64))})
		position_data = position_data.merge(ds)

	logger.debug('Sort new sonar log by position')

	# @todo Ideally we would use below .assign() instead of making a copy:
	# However it doesnt work correctly. Its possible that at least one issue is that the variables in channel are dimensioned on time not the new position so the sort below fails.
	# For now we will just create a copy structured as we want
	#merged_postion_channel = channel.assign(position=lambda x: LongitudeLatitudeToPosition(x.longitude, x.latitude))

	merged_postion_channel = xarray.Dataset({
			'longitude': (['position'],  channel.longitude),
			'latitude': (['position'],  channel.latitude),
			'depth': (['position'],  channel.depth),
			'contributors': (['position'],  numpy.ones(len(channel.depth))),
		}, coords={'position': (['position'], LongitudeLatitudeToPosition(channel.longitude, channel.latitude))})
	sorted_channel = merged_postion_channel.sortby('position')

	return MergePositionData(sorted_channel, position_data)

def GenerateSurfaceMeshFromPositionData(position_data, include_corners=False):
	if len(position_data.longitude) == 0:
		logger.warning('No data to triangulate')
		return None
	
	longitude_range = [position_data.longitude.min().values.item(), position_data.longitude.max().values.item()]
	latitude_range = [position_data.latitude.min().values.item(), position_data.latitude.max().values.item()]
	depth_range = [position_data.depth.min().values.item(), position_data.depth.max().values.item()]

	offset = [longitude_range[0], latitude_range[0], 0]
	
	# Scale z axis to make it more easily visible in the mesh for now
	# We will choose scaling only INCREASE to make it at least 1/10 the x/y dists
	min_xy_dist = min(longitude_range[1]-longitude_range[0], latitude_range[1]-latitude_range[0])
	min_z_dist = min_xy_dist / 10
	z_dist = depth_range[1] - depth_range[0]
	z_scale = 1
	if z_dist < min_z_dist:
		z_scale  = min_z_dist / z_dist

	scale = [1, 1, z_scale]
	
	extra_points = 0
	if include_corners:
		extra_points = 4
	tmp_xy = numpy.zeros((len(position_data.longitude) + extra_points, 2))
	tmp_z = numpy.zeros(len(position_data.depth) + extra_points)

	for index in range(0, len(position_data.longitude)):
		tmp_xy[index][0] = (position_data.longitude.values[index] - offset[0]) * scale[0]
		tmp_xy[index][1] = (position_data.latitude.values[index] - offset[1]) * scale[1]
		tmp_z[index] = -1 * (position_data.depth.values[index] - offset[2]) * scale[2]

	if include_corners:
		adjusted_longitude_range = (longitude_range[0] - offset[0]) * scale[0], (longitude_range[1] - offset[0]) * scale[0]
		adjusted_latitude_range = (latitude_range[0] - offset[1]) * scale[1], (latitude_range[1] - offset[1]) * scale[1]
		
		# Bottom left
		tmp_xy[-4][0] = adjusted_longitude_range[0]
		tmp_xy[-4][1] = adjusted_latitude_range[0]
		tmp_z[-4] = 0

		# Top left
		tmp_xy[-3][0] = adjusted_longitude_range[0]
		tmp_xy[-3][1] = adjusted_latitude_range[1]
		tmp_z[-3] = 0

		# Bottom right
		tmp_xy[-2][0] = adjusted_longitude_range[1]
		tmp_xy[-2][1] = adjusted_latitude_range[0]
		tmp_z[-2] = 0

		# Top right
		tmp_xy[-1][0] = adjusted_longitude_range[1]
		tmp_xy[-1][1] = adjusted_latitude_range[1]
		tmp_z[-1] = 0

	surface_mesh = scipy.spatial.Delaunay(tmp_xy) #, incremental=True) #, qhull_options='QbB')
	surface_mesh.scale = scale
	surface_mesh.offset = offset
	surface_mesh.z = tmp_z
	surface_mesh.longitude_range = longitude_range
	surface_mesh.latitude_range = latitude_range
	surface_mesh.depth_range = depth_range

	return surface_mesh

def CreateStlFromSurfaceMesh(filename, surface_mesh):
	# We will re-adjust the data to the correct scale, but not put the offset back in
	# @todo We probably should adjust the offset a bit though so the middle of the map is at 0,0 maybe best? Or smallest at 0,0
	# Right now that isnt the case
	vertices_xy = surface_mesh.points
	vertices_z = surface_mesh.z
	faces = surface_mesh.simplices

	# https://pypi.org/project/numpy-stl/
	stl_mesh = stl.mesh.Mesh(numpy.zeros(faces.shape[0], dtype=stl.mesh.Mesh.dtype))
	for i, f in enumerate(faces):
		stl_mesh.vectors[i][0] = [vertices_xy[f[0]][0] * surface_mesh.scale[0], vertices_xy[f[0]][1] * surface_mesh.scale[1], vertices_z[f[0]] * surface_mesh.scale[2]]
		stl_mesh.vectors[i][1] = [vertices_xy[f[1]][0] * surface_mesh.scale[0], vertices_xy[f[1]][1] * surface_mesh.scale[1], vertices_z[f[1]] * surface_mesh.scale[2]]
		stl_mesh.vectors[i][2] = [vertices_xy[f[2]][0] * surface_mesh.scale[0], vertices_xy[f[2]][1] * surface_mesh.scale[1], vertices_z[f[2]] * surface_mesh.scale[2]]
	stl_mesh.save(filename)


def CreateObjFromSurfaceMesh(file_name, surface_mesh):
	# We will re-adjust the data to the correct scale, but not put the offset back in
	# @todo We probably should adjust the offset a bit though so the middle of the map is at 0,0 maybe best? Or smallest at 0,0
	# Right now that isnt the case
	vertices_xy = surface_mesh.points
	vertices_z = surface_mesh.z
	faces = surface_mesh.simplices

	x_range = (surface_mesh.longitude_range[0] - surface_mesh.offset[0]) * surface_mesh.scale[0], (surface_mesh.longitude_range[1] - surface_mesh.offset[0]) * surface_mesh.scale[0]
	y_range = (surface_mesh.latitude_range[0] - surface_mesh.offset[1]) * surface_mesh.scale[1], (surface_mesh.latitude_range[1] - surface_mesh.offset[1]) * surface_mesh.scale[1]
	
	# The image generated has a 11 pixel border I cant remove so
	# we will map the texture to exclude it. Load the image file and
	# find the images dimensions used for remapping
	texture_image_file_name = os.path.splitext(file_name)[0]+'.png'
	image = PIL.Image.open(texture_image_file_name)
	image_width,image_height = image.size
	BORDER_SIZE = 11
	
	base_file_name, ext = os.path.splitext(os.path.basename(file_name))
	with open(file_name, 'w') as file:
		print ('mtllib '+str(base_file_name)+'.mtl', file=file)
		print ('o ' + base_file_name + '.001', file=file)

		for i in range(0, len(vertices_z)):
			x = vertices_xy[i][0]
			y = vertices_xy[i][1]
			z = vertices_z[i]
			print ('v %s %s %s' % (x, y, z), file=file)
			
			x_without_border_ratio = (x - x_range[0]) / (x_range[1] - x_range[0])
			x_pixel = x_without_border_ratio * (image_width - (2 * BORDER_SIZE)) + 11
			x_with_border_ratio = x_pixel / image_width
			
			y_without_border_ratio = (y - y_range[0]) / (y_range[1] - y_range[0])
			y_pixel = y_without_border_ratio * (image_height - (2 * BORDER_SIZE)) + 11
			y_with_border_ratio = y_pixel / image_height

			u = x_with_border_ratio
			v = y_with_border_ratio
			print ('vt %s %s' % (u, v), file=file)

		print ('usemtl ' + base_file_name + '.png', file=file)
		print ('# Enable smooth shading', file=file)
		print ('s 1', file=file)
		
		for i, f in enumerate(faces):
			print ('f %s/%s %s/%s %s/%s' % (f[0]+1, f[0]+1, f[1]+1, f[1]+1, f[2]+1, f[2]+1), file=file)

	with open(os.path.splitext(file_name)[0]+'.mtl', 'w') as file:
		print ('# define a material named ' + base_file_name, file=file)
		print ('newmtl ' + base_file_name + '.png', file=file)
		print ('Ka 1.000 1.000 1.000     # white', file=file)
		print ('Kd 1.000 1.000 1.000     # white', file=file)
		print ('Ks 0.000 0.000 0.000     # black (off)', file=file)
		print ('d 0.9                    # 0-1, 0 is fully transparent', file=file)
		print ('illum 2', file=file)
		print ('map_Ka '+str(base_file_name+'.png'), file=file)
		print ('map_Kd '+str(base_file_name+'.png'), file=file)
		print ('map_Ks '+str(base_file_name+'.png'), file=file)


def GenerateDepthMapImageFromPositionData(position_data):
	longitude_range = [position_data.longitude.min().values.item(), position_data.longitude.max().values.item()]
	latitude_range = [position_data.latitude.min().values.item(), position_data.latitude.max().values.item()]
	data = (position_data.longitude.values, position_data.latitude.values, position_data.depth.values)
	#import code
	#code.interact(local=dict(globals(), **locals()))
	
	# For this which is a texture map for a 3D model we will use ESRI satellite images
	tiles = holoviews.element.tiles.EsriImagery()
	#tiles = holoviews.Tiles('https://maps.wikimedia.org/osm-intl/{Z}/{X}/{Y}@2x.png', name="Wikipedia")

	longitude_dist = longitude_range[1] - longitude_range[0]
	latitude_dist = latitude_range[1] - latitude_range[0]
	if longitude_dist >= latitude_dist:
		dimensions = dict(frame_width=600)
	else:
		dimensions = dict(frame_height=600)
	dimensions['aspect'] = (longitude_dist / latitude_dist)
	tiles = tiles.opts(**dimensions)
	
	tile = tiles.redim(
		x=holoviews.Dimension('x', range=(longitude_range[0], longitude_range[1])), 
		y=holoviews.Dimension('y', range=(latitude_range[0], latitude_range[1]))
		)
	
	# Remove the axis and toolbar to make a clean image we can use later for texturing
	tiles = tiles.opts(xaxis=None, yaxis=None, toolbar=None)

	points = holoviews.Points(data, vdims='depth', kdims=[
		holoviews.Dimension('easting'), 
		holoviews.Dimension('northing')]).opts(color='depth', cmap='viridis').opts(**dimensions)
	return holoviews.Overlay([tiles, points])

def LoadMergedData(merge_data_file_name):
	merged_data = xarray.open_mfdataset([merge_data_file_name], combine='by_coords', parallel=True, engine='netcdf4')
	with open(merge_data_file_name + '.meta.json') as json_file:
		cache_meta = json.load(json_file)
	merged_data.attrs['meta'] = cache_meta
	assert('merged_files' in merged_data.attrs['meta'])

	return merged_data.load()

def CreateEmptyMergedData():
	merged_data = xarray.Dataset({})
	merged_data.attrs['meta'] = {'merged_files':[]}
	return merged_data

def SaveMergedData(merge_data_file_name, merged_data):
	if not os.path.isdir(os.path.dirname(merge_data_file_name)):
		os.makedirs(os.path.dirname(merge_data_file_name))
	meta = merged_data.attrs['meta']
	del(merged_data.attrs['meta'])
	merged_data.to_netcdf(merge_data_file_name, engine='netcdf4')
	merged_data.attrs['meta'] = meta

	with open(merge_data_file_name + '.meta.json', 'w') as outfile:
		json.dump(merged_data.attrs['meta'], outfile)

	# Note: We will re-load and return that instead to help fild issues in the saved file earlier
	return LoadMergedData(merge_data_file_name)
