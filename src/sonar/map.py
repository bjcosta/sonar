import math

import overpy.exception
import holoviews
import datashader.geo
import sonar.sl2_parser

import logging
logger = logging.getLogger('sonar_map')


def AdjustToIncludeNearestLandmark(lon_min, lon_max, lat_min, lat_max):
	orig_lon_min = lon_min
	orig_lon_max = lon_max

	orig_lat_min = lat_min
	orig_lat_max = lat_max

	lon_dist = orig_lon_max - orig_lon_min
	lat_dist = orig_lat_max - orig_lat_min
	
	# Find the nearest suburb to the middle of the bounding box
	middle_lon = orig_lon_min + lon_dist / 2.0
	middle_lat = orig_lat_min + lat_dist / 2.0
	
	# Overpass seems to take degrees not web mercator metres, so convert the middle to use in overpass
	middle_lon_degrees, middle_lat_degrees = sonar.sl2_parser.meters_to_lnglat(middle_lon, middle_lat)
	
	api = overpy.Overpass()
	radius = 100.0

	# Permit path area to be as small as 1/10 of overall map to provide context
	# So if we travel 10km, will look upto 100km
	max_radius = 10 * max((orig_lat_max - orig_lat_min), (orig_lon_max - orig_lon_min))

	# If only travell 1 cm, still permit the graph to search context up to 5km
	max_radius = max(max_radius, 5000)
	
	nearest_landmark_debug = ''
	node = None
	node_lon = None
	node_lat = None
	while node is None and radius <= max_radius:
		logger.debug('Trying radius: %s', radius)
		
		query = '<query type="node"><has-kv k="place" v="suburb"/><around lon="%s" lat="%s" radius="%s"/></query><print/>' % (middle_lon_degrees, middle_lat_degrees, radius)
		logger.debug('Query: %s', query)
		
		try: result = api.query(query)
		except overpy.exception.OverpassTooManyRequests as e:
			logger.error('Failed to query nearby suburb: %s', e)
			break
		except overpy.exception.OverpassGatewayTimeout as e:
			logger.error('Failed to query nearby suburb: %s', e)
			break
		logger.debug('Looking at result: %s nodes:%s ways:%s relations:%s areas:%s', 
			result,
			result.get_node_ids(),
			result.get_way_ids(),
			result.get_relation_ids(),
			result.get_area_ids())

		radius *= 5
		
		min_dist_index = None
		distances = []
		for i in range(0, len(result.nodes)):
			logger.debug('Looking at result node: %s of %s', i, len(result.nodes))
			n = result.nodes[i]
			n_lon, n_lat = sonar.sl2_parser.lnglat_to_meters(float(n.lon), float(n.lat))
			lat_dist = middle_lat - n_lat
			lon_dist = middle_lon - n_lon
			dist = math.sqrt((lat_dist * lat_dist) + (lon_dist * lon_dist))
			logger.debug ('Distance: %s from middle of travelled area to node: %s %s', dist, n, n.tags)
			distances.append(dist)
			
			if min_dist_index is None or dist < distances[min_dist_index]:
				min_dist_index = i
		
		if min_dist_index is not None:
			node = result.nodes[min_dist_index]
			node_lon, node_lat = sonar.sl2_parser.lnglat_to_meters(float(node.lon), float(node.lat))
			
			logger.debug('Selecting the nearest landmark: %s to include in graph with lat:%s lon:%s to the center of the travelled area lat:%s lon:%s with boundary: %s, %s, %s, %s', node.tags, node_lat, node_lon, middle_lat, middle_lon, lon_min, lat_min, lon_max, lat_max)
			nearest_landmark_debug = str(node.tags['name'])
	
	logger.debug('Adjusting map edges using node: %s', node)
	
	if node is None:
		logger.warning('Failed to locate the nearest landmark to (%s, %s) within radius: %s', middle_lon_degrees, middle_lat_degrees, radius)
		# You can debug this using the overpass API : https://overpass-turbo.eu/#
		# The query might look something like:
		# [out:json][timeout:30];
		# (
		#  node(around:<radius>,<latitude>,<longitude>) ["place"];
		# ); 
		# out body;
		
	else:
		# So lets first adjust bbox to fit in the new node
		node_lat = float(node_lat)
		node_lon = float(node_lon)
		
		if node_lat < lat_min:
			diff = lat_min - node_lat
			lat_min -= (diff + (diff / 4))
			lat_max += (diff / 4)

		if node_lat > lat_max:
			diff = node_lat - lat_max
			lat_min -= (diff / 4)
			lat_max += (diff + (diff / 4))

		if node_lon < lon_min:
			diff = lon_min - node_lon
			lon_min -= (diff + (diff / 4))
			lon_max += (diff / 4)

		if node_lon > lon_max:
			diff = node_lon - lon_max
			lon_min -= (diff / 4)
			lon_max += (diff + (diff / 4))

	return (lon_min, lon_max, lat_min, lat_max, nearest_landmark_debug)

def GenMap(sonar_data, include_nearest_landmark=True):
	gps_longitude_range = sonar_data.longitude.min().values.item(), sonar_data.longitude.max().values.item()
	gps_latitude_range = sonar_data.latitude.min().values.item(), sonar_data.latitude.max().values.item()
	lon_min = gps_longitude_range[0]
	lon_max = gps_longitude_range[1]
	lat_min = gps_latitude_range[0]
	lat_max = gps_latitude_range[1]

	nearest_landmark_debug = ''
	if include_nearest_landmark:
		lon_min, lon_max, lat_min, lat_max, nearest_landmark_debug = AdjustToIncludeNearestLandmark(lon_min, lon_max, lat_min, lat_max)
		if nearest_landmark_debug != '':
			nearest_landmark_debug = '. With nearest landmark: ' + nearest_landmark_debug

	logger.debug('Generating graph inside area: (%s, %s) - (%s, %s)%s', lon_min, lat_min, lon_max, lat_max, nearest_landmark_debug)

	tiles = holoviews.Tiles('https://maps.wikimedia.org/osm-intl/{Z}/{X}/{Y}@2x.png', name="Wikipedia")
	tiles = tiles.opts(width=600, height=600)
	
	# Adjust the framing of the tiles to show the path area and not the entire world
	# From: https://examples.pyviz.org/nyc_taxi/nyc_taxi.html
	#left, bottom = datashader.geo.lnglat_to_meters(lon_min, lat_min)
	#right, top = datashader.geo.lnglat_to_meters(lon_max, lat_max)
	# @todo cleanup, we are now using web mercator metres everywhere
	left, bottom = (lon_min, lat_min)
	right, top = (lon_max, lat_max)
	
	# http://holoviews.org/_modules/holoviews/core/dimension.html
	tiles = tiles.redim(
		x=holoviews.Dimension('x', range=(left, right)), 
		y=holoviews.Dimension('y', range=(bottom, top))
		)
	
	logger.debug('left:%s right:%s bottom:%s top:%s', left, right, bottom, top)
	#import code
	#code.interact(local=dict(globals(), **locals()))
	
	channel = sonar_data.sel(channel=sonar.sl2_parser.PRIMARY)
	latitude_arr = channel.latitude.values
	longitude_arr = channel.longitude.values
	time_arr = channel.time.values
	depth_arr = channel.depth.values
	
	#logger.info('Creating vector field for the path')
	#x_arr = []
	#y_arr = []
	#angle_arr = []
	#magnitude_arr = []
	#for i in range(0, len(time_arr)):
	#	x,y = datashader.geo.lnglat_to_meters(longitude_arr[i], latitude_arr[i])
	#	angle = channel.heading.values[i]
	#	magnitude = channel.speed_gps.values[i]
	#	x_arr.append(x)
	#	y_arr.append(y)
	#	angle_arr.append(angle)
	#	magnitude_arr.append(magnitude)
	## http://holoviews.org/reference/elements/bokeh/VectorField.html
	#path = holoviews.VectorField((x_arr, y_arr, angle_arr, magnitude_arr))
	#path = path.opts(magnitude='Magnitude', color='Magnitude')
	#logger.info('Done')
	
	
	path_points = []
	for i in range(0, len(time_arr)):
		# @todo cleanup
		#path_points.append(datashader.geo.lnglat_to_meters(longitude_arr[i], latitude_arr[i]))
		path_points.append((longitude_arr[i], latitude_arr[i], depth_arr[i]))
	path = holoviews.Path(path_points, vdims='depth', kdims=[
		holoviews.Dimension('easting'), 
		holoviews.Dimension('northing')])
	path = path.opts(color='depth', width=600, height=600, cmap='viridis')
	
	# See https://holoviz.org/tutorial/Composing_Plots.html for the easting/northing plot
	# @todo Note: Sometimes the x/y axis in overlay changes to use the path values not the tiles values which is not what we want we want to displat lon/lat not northings eastings
	overlay = tiles * path
	return overlay
