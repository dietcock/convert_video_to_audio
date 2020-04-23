import os
import subprocess

from concurrent import futures

import time
import youtube_dl
import grpc
import voice_bucket_pb2
import voice_bucket_pb2_grpc
import requests
import uuid
from urllib.request import urlretrieve

class Servicer(voice_bucket_pb2_grpc.AudioDownloadServiceServicer):
	def _downloader(self, url, download_path):
		ydl_opts = {
			'format': 'bestaudio[ext=m4a]/bestaudio',  # 가장 좋은 화질로 선택(화질을 선택하여 다운로드 가능)
			'outtmpl': download_path, # 다운로드 경로 설정
			# 'writeautomaticsub': True, # 자동 생성된 자막 다운로드
			# 'subtitleslangs': 'ko',
			'postprocessors': [{
				'key': 'FFmpegExtractAudio',
				'preferredcodec': 'wav',
				'preferredquality': '192'}]
			}
		try:
			with youtube_dl.YoutubeDL(ydl_opts) as ydl:
				ydl.download([url])
		except Exception as e:
			print('error', e)

	def _other_platform_downloader(self, url, full_source_path, full_download_path):
		print("Downloading: " + full_source_path)
		urlretrieve(url, full_source_path)
		print("End of downloaded: " + full_source_path)

		self._just_converter(full_source_path, full_download_path)

	def _just_converter(self, full_source_path, full_download_path):
		command = "ffmpeg -i \"" + full_source_path + "\" -ab 160k -ac 2 -ar 44100 -vn \"" + full_download_path + "\""
		print("Command: " + command)
		print('Converting... from video to audio...')

		subprocess.call(command, shell=True)
		print("End of converted: " + full_download_path)

	def _is_exist_dir(self, platform):
		if os.path.isdir(os.path.abspath('./videos/' + platform + '/')) == False:
			os.makedirs(os.path.abspath('./videos/' + platform + '/'))

		if os.path.isdir(os.path.abspath('./audios/' + platform + '/')) == False:
			os.makedirs(os.path.abspath('./audios/' + platform + '/'))

		if os.path.isdir(os.path.abspath('./ts/' + platform + '/')) == False:
			os.makedirs(os.path.abspath('./ts/' + platform + '/'))

	def ConvertVideoToAudio(self, request, context):
		self.url = str(request.SourceVideoURL).strip()

		if self.url.find('youtube') != -1:
			self._youtube_downloader()
		elif self.url.find('voda') != -1:
			self._voda_downloader()
		elif self.url.find('zum') != -1:
			self._zum_downloader()
		elif self.url.find('naver') != -1:
			self._naver_downloader()

	def _youtube_downloader(self):
		try:
			filename = str(uuid.uuid4())
			platform = 'youtube'

			download_path = os.path.join('./audios/' + platform + '/' + filename + '.wav')

			self._downloader(self.url, download_path)
		except Exception as e:
			print('error', e)

	def _voda_downloader(self):
		try:
			filename = str(uuid.uuid4())
			platform = 'voda'

			self._is_exist_dir(platform)

			full_source_path = os.path.abspath('./videos/' + platform + '/' + filename + '.mp4')
			full_download_path = os.path.abspath('./audios/' + platform + '/' + filename + '.wav')

			self._other_platform_downloader(self.url, full_source_path, full_download_path)
		except Exception as e:
			print('error', e)

	def _zum_downloader(self):
		try:
			r = requests.get(self.url, stream = True)

			filename = str(uuid.uuid4())
			platform = 'zum'

			self._is_exist_dir(platform)

			full_source_path = os.path.abspath('./videos/' + platform + '/' + filename + '.mp4')
			full_download_path = os.path.abspath('./audios/' + platform + '/' + filename + '.wav')

			with open(full_source_path, 'wb') as f:
				for chunk in r.iter_content(chunk_size = 1024*1024):
					if chunk:
						f.write(chunk)

			self._just_converter(full_source_path, full_download_path)
		except Exception as e:
			print('error', e)

	def _naver_downloader(self):
		try:
			baseurl = self.url.split('playlist')[0].replace('https', 'http')
			print('Downloaded playlist from url')

			r = requests.get(self.url)
			playlist_for_resolution = str(r.content).split('\\n')

			ts_url = ""
			for index, item in enumerate(playlist_for_resolution):
				if item.find("RESOLUTION=480x270") != -1:
					ts_url = baseurl + playlist_for_resolution[index+1]

			print('Downloaded ts list in playlist')
			ts_url = ts_url.replace('\\r', '')

			ts_r = requests.get(ts_url)
			ts_contents = str(ts_r.content).split('\\n')

			filename = str(uuid.uuid4())
			platform = 'zum'

			folder_name = str(uuid.uuid4())
			print('Got save file infos.')

			self._is_exist_dir(platform)
			if os.path.isdir(os.path.abspath('./ts/' + platform + '/' + folder_name + '/')) == False:
				os.makedirs(os.path.abspath('./ts/' + platform + '/' + folder_name + '/'))

			try:
				for index, item in enumerate(ts_contents):
					if item.find('content') != -1:
						print('ts file downloading... index: ' + str(index))
						tmp_ts_content_name = item.split('.ts')
						tmp_ts_name_arr = tmp_ts_content_name[0].split('_')
						if len(tmp_ts_name_arr[-1]) < 2:
							tmp_ts_name_arr[-1] = '0' + tmp_ts_name_arr[-1]

						remade_ts_name = '_'.join(tmp_ts_name_arr) + '.ts'
						converted_ts_name = remade_ts_name + tmp_ts_content_name[1]

						full_ts_path = os.path.abspath('./ts/' + platform + '/' + folder_name + '/' + remade_ts_name)

						rr = requests.get(baseurl + converted_ts_name, stream = True)
						with open(full_ts_path, 'wb') as f:
							for chunk in rr.iter_content(chunk_size = 1024*1024):
								if chunk:
									f.write(chunk)
			except Exception as e:
				print('error ', e)

			print('All ts files downloaded...')

			full_source_path = os.path.abspath('./videos/' + platform + '/' + folder_name + '.mp4')
			full_converted_path = os.path.abspath('./audios/' + platform + '/' + filename + '.wav')
			full_folder_path = os.path.abspath('./ts/' + platform + '/' + folder_name)
			full_merged_ts_path = os.path.abspath('./ts/' + platform + '/' + folder_name + '/all.ts')

			command = 'cat ' + full_folder_path + '/*.ts >> ' + full_folder_path + '/all.ts'

			subprocess.call(command, shell=True)
			print('To merge all ts files to all.ts')

			command = 'ffmpeg -i ' + full_merged_ts_path + ' -bsf:a aac_adtstoasc -vcodec copy ' + full_source_path
			subprocess.call(command, shell=True)
			print('To merge ts file to mp4')

			self._just_converter(full_source_path, full_converted_path)

			command = "rm -rf " + os.path.abspath('./ts/' + platform + '/' + folder_name + '')
			subprocess.call(command, shell=True)
			print('Removed ts files from disk.')

		except Exception as e:
			print('error ', e)

class Server:
	@staticmethod
	def start():
		server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
		voice_bucket_pb2_grpc.add_AudioDownloadServiceServicer_to_server(
			Servicer(), server
			)
		server.add_insecure_port('0.0.0.0:30051')
		server.start()
		print("Server is running...")

		try:
			while True:
				time.sleep(86400)
		except KeyboardInterrupt:
			server.stop(0)

if __name__ == '__main__':
	Server.start()