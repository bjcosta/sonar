import sys
import logging
logger = logging.getLogger('gen_sonar_images')
if __name__ == '__main__':
	logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s : %(message)s')

logger.info('Importing python modules (takes time for some reason)...')
import holoviews
import dask
import time
import glob
import argparse
import os.path

import xarray

import sonar.sl2_parser
import sonar.map
import sonar.normalized_depth
import sonar.all
import sonar.merged_position

# Configure the default renderer
holoviews.extension('bokeh')
renderer = holoviews.renderer('bokeh').instance(mode='server')

def GenMapImage(sonar_data):
	try:
		holoviews_map = sonar.map.GenMap(sonar_data)
		# @todo Give it time to download tiles
		logger.info('Sleeping or bokeh rendering tiles fails sometimes')
		time.sleep(60)
		map_file_name = sonar_data.attrs['gen_name'] + '.map.png'
		logger.info('Saving map image: %s', map_file_name)
		holoviews.save(holoviews_map, map_file_name)
	except:
		logger.exception('Failed to produce map image for: %s', sonar_data.attrs['file_name'])


def GenPositionFiles(file_base, position_data):
	if not os.path.isdir(os.path.dirname(file_base)):
		os.makedirs(os.path.dirname(file_base))

	# @todo Add flat edge boundary to the position data. Will want to use Tiles() and get bounds

	logger.info('Generating surface triangle mesh for: %s', file_base)
	surface_mesh = sonar.merged_position.GenerateSurfaceMeshFromPositionData(position_data)

	# Create a depth map image
	merged_position_image = sonar.merged_position.GenerateDepthMapImageFromPositionData(position_data)
	file_name = file_base + '.quantized_map.png'
	logger.info('Saving depth map image: %s', file_name)
	holoviews.save(merged_position_image, file_name)
	
	# Create a depth map STL
	file_name = file_base + '.quantized_map.stl'
	logger.info('Saving depth map STL: %s', file_name)
	sonar.merged_position.CreateStlFromSurfaceMesh(file_name, surface_mesh)

def GenMergedPositionImages(args, sonar_data, merged_data):
	try:
		merged_data, this_data = sonar.merged_position.MergeSonarLogByPosition(sonar_data, merged_data)
		GenPositionFiles(sonar_data.attrs['gen_name'], this_data)
	except:
		logger.exception('Failed to produce map image for: %s', sonar_data.attrs['file_name'])
	return merged_data

def GenDepthImage(sonar_data, chan_id, regen_cache=False):
	try:
		depth_image = sonar.normalized_depth.GenDepth(sonar_data, chan_id, regen_cache=regen_cache)

		chan_name = sonar.sl2_parser.ChannelToStr(chan_id)
		depth_file_name = sonar_data.attrs['gen_name'] + '.' + str(chan_name) + '.sonar.png'
		logger.info('Saving depth image: %s', depth_file_name)
		holoviews.save(depth_image, depth_file_name)
	except:
		logger.exception('Failed to produce depth images for: %s', sonar_data.attrs['file_name'])

def GenDebugAll(sonar_data, chan_id):
	try:
		plots = sonar.all.GenAll(sonar_data, chan_id)
		
		chan_name = sonar.sl2_parser.ChannelToStr(chan_id)
		for k,plot in plots.items():
			file_name = sonar_data.attrs['gen_name'] + '.' + str(chan_name) + '.' + k + '.png'
			logger.info('Saving image: %s', file_name)
			holoviews.save(plot, file_name)
	except:
		logger.exception('Failed to produce depth images for: %s', sonar_data.attrs['file_name'])

def ParseArgs():
	parser = argparse.ArgumentParser(description='')

	parser.add_argument('--regen-cache', default=False, action='store_true', help='Regenerate the cache files created after processing sonar logs to improve performance of future requests')
	parser.add_argument('--merge-file', metavar='PATH', default='', help='Location of file used to store data merged across multiple sonar logs.')
	parser.add_argument('--truncate-merge-file', default=False, action='store_true', help='Will erase the merge file and write a new one instead of appending data to an existing file')
	parser.add_argument('--debug', default=False, action='store_true', help='Generates many graphs used to help debug and reverse engineer the sl2 sonar logs')
	parser.add_argument('path', nargs='*', help='Sonar log file(s) to be parsed and processed')
	args = parser.parse_args()

	# Parse paths
	full_paths = [os.path.join(os.getcwd(), path) for path in args.path]
	files = []
	for path in args.path:
		if os.path.isfile(path):
			files.append(path)
		else:
			files += sorted(glob.glob(path))

	args.files = files
	return args

def Main():
	args = ParseArgs()
	logger.debug('Loading files: %s', args.files)

	logger.info('Starting dask client')
	dask_client = dask.distributed.Client()
	
	if args.merge_file != '' and not args.truncate_merge_file and os.path.isfile(args.merge_file):
		logger.info('Loading merged data from: %s', args.merge_file)
		merged_data = sonar.merged_position.LoadMergedData(args.merge_file)

	else:
		logger.info('Starting with empty merge data')
		if args.merge_file != '' and not args.truncate_merge_file:
			logger.warning('Failed to load merge file: %s, we will create a new one', args.merge_file)
		merged_data = sonar.merged_position.CreateEmptyMergedData()

	for file_name in args.files:
		try:
			if os.path.basename(file_name) in merged_data.attrs['meta']['merged_files']:
				logger.info('Skipping file %s as already merged', file_name)
				continue
			
			sonar_data = sonar.sl2_parser.LoadSonarFile(file_name, regen_cache=args.regen_cache)
			GenMapImage(sonar_data)
			for chan_id in sonar_data.channel.values:
				GenDepthImage(sonar_data, chan_id, regen_cache=args.regen_cache)
				if args.debug:
					GenDebugAll(sonar_data, chan_id)

			merged_data = GenMergedPositionImages(args, sonar_data, merged_data)
			merged_data.attrs['meta']['merged_files'].append(os.path.basename(file_name))

			# Save the merged data after each update if there is a args.merge_file
			# in case we exit early we at least have some updated data
			if args.merge_file != '':
				logger.info('Saving merged data into: %s', args.merge_file)
				merged_data = sonar.merged_position.SaveMergedData(args.merge_file, merged_data)
				
				# Lets also incrementally update the merged results instead of 
				# just at end in case something is cancelled
				GenPositionFiles(args.merge_file, merged_data)
		except:
			logger.exception('Failed to load file: %s', file_name)
			continue
	
	if len(args.files) == 0 and args.merge_file != '':
		GenPositionFiles(args.merge_file, merged_data)
	logger.info('All completed successfully')
	
	dask_client.close()
	
if __name__ == '__main__':
	Main()
