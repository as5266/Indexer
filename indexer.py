import json
import gevent
import urllib2
import feedparser
from gevent import monkey
monkey.patch_socket()
from gevent.queue import Queue

"""
maintaing a dictionary of articles already seen.
source id and article title is used as key
"""
ARTICLE_DICT = {}

"""
queue from which task are fetched by different threads
TODO: Queue_size needs to be optimized by testing; using
a randomly selected number for now
"""
QUEUE_SIZE = 20
JOB_QUEUE = Queue(maxsize=QUEUE_SIZE)

class NewsSource:
	"""
	NewsSource stores the source id, url and source name
	and provides methods to retrieve the same
	"""
	def __init__(self,source,url,id):
		self._source = source
		if "http" not in url:
			url = "http://"+url
		self._url = url
		self._id = id
	
	def get_url(self):
		return self._url
	
	def get_source(self):
		return self._source
	
	def get_id(self):
		return self._id

def get_tasks(filename):
	"""
	creates an object of type NewsSource for each
	source and pushes it into a queue 
	"""
	try:
		json_data = open(filename)
	except:
		print "Error! Cannot open file",filename
		return
	data = json.load(json_data)
	tasks = []
	for line in data:
		my_source = NewsSource(line['source_name'], line['feed_url'], line['id'])
		tasks.append(my_source)
	return tasks

def fetch(source):
	"""
	this function fetches the content from the the source url
	and prints the article url, name and size of html from article url
	and stores the entry in ARTICLE_DICT with source_id and article_title
	as key. After the source has been exhausted for new content,source 
	gets inserted into a queue 'new_tasks' for processing in the next cycle
	"""
	url = source.get_url()
	try:
		response = urllib2.urlopen(url)
		raw_data = response.read()
		result = feedparser.parse(raw_data)
	except urllib2.HTTPError, e:
		print "Error code: %s" % e.code, "for link", url
		return
	except urllib2.URLError, e:
		print "Error!", e.args
		return
	
	for entry in result.entries:
		article_url = entry.link
		article_title = entry.title
		# if article not seen before, process
		if (source.get_id(),article_title) not in ARTICLE_DICT:
			try:
				article_data = urllib2.urlopen(article_url).read()
				print 'URL:',article_url, 'Title:',article_title, 'Size:', len(article_data)
				# Can store the article here; but saving memory for now
				ARTICLE_DICT[(source.get_id(),article_title)] = True
				
			except urllib2.HTTPError, e:
				print "Error code: %s" % e.code, "for link", article_url
				continue
			except urllib2.URLError, e:
				print "Error!", e.args
				continue
		else:
			print "article already seen"
			## breaking as remaining part is already seen
			return

def consumer(n):
	"""
	Consumer fetches a job from the front of a queue
	and creates a thread to finish the job. queue
	size gets decreased by 1 whenever a new job is fetched
	"""
	while True:
		threads = []
		try:
			while not JOB_QUEUE.empty():
				task = JOB_QUEUE.get()
				threads.append(gevent.spawn(fetch, task))
				gevent.sleep(1)
			gevent.joinall(threads)
		except Empty:
			pass

def producer(tasks):
	"""
	Producer is given a list of all news sources.
	it uses the source list to push jobs into job queue
	for the consumer. Note that the queue being
	used is of limited size. Whenever queue is not full
	new job gets pushed into it. The producer keeps on
	iterating over the entire list forever
	"""
	while True:
		for task in tasks:
			try:
				JOB_QUEUE.put(task)
				gevent.sleep(1)
			except:
				pass

def main():
	FILENAME = "newsle_feeds.json"
	tasks = get_tasks(FILENAME)
	if (len(tasks)!= 0):
		p = gevent.spawn(producer,tasks)
		## multiple consumers can be produced; passed
		## argument will act as identifier in that case
		c = gevent.spawn(consumer,1)
		gevent.joinall([p,c])


if __name__ == '__main__':
	main()