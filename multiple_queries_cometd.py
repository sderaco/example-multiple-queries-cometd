import csv
import logging
import threading

import importio

# We define a latch class as python doesn't have a counting latch built in
class _Latch(object):
  def __init__(self, count=1):
    self.count = count
    self.lock = threading.Condition()

  def countDown(self):
    with self.lock:
      self.count -= 1

      if self.count <= 0:
        self.lock.notifyAll()

  def await(self):
    with self.lock:
      while self.count > 0:
        self.lock.wait()

logging.basicConfig(level=logging.INFO)

# Initialise the library
client = importio.ImportIO(host="http://query.import.io", 
	userId="YOUR_USER_GUID", 
	apiKey="YOUR_API_KEY")
client.connect()

# Use a latch to stop the program from exiting
latch = _Latch(1)

results={}
page=0
def callback(query, message):    
	if message["type"] == "MESSAGE":
		global results
		global page 
		#print message
		try:
			results[page]=message["data"]["results"]
			page=page+1
		except:
		    error=message["data"]["errorType"] 
		    print error
	    
	if query.finished(): latch.countDown()

# Now we read the file where we have 
companies=[]
with open("Data Set: assintel link extractor.csv","rb") as infile:
	reader=csv.reader(infile)
	for index, row in enumerate(reader):
		if index>0:
			try:
				link=row[5]
				# We only need the latest part of the url, where the name of the company is.
				# I know the first part is http://www.assintel.it/soci/, with a length of 27 characteres, so: 
				company=link[28:(len(link)-1)]
				companies.append(company)
			except:
				pass

# We create a new file
with open("Results Assintel connector.csv","wb") as outfile:
	writer=csv.writer(outfile)
	header=["Name","Sector","Url","Url logo","Address","Telephone number"]
	writer.writerow(header)
	# And for each company
	for index, company in enumerate(companies):
		print index

		# we make the query:
		page=0
		results={}
		latch = _Latch(1)
		client.query({"connectorGuids": ["75021a3d-8615-41d0-89bb-ce36c1d79c99"],
			  "input": {"company_name": company}}, callback)
		latch.await()

		# and write the results
		row=[]
		try:
			row.append(results[0][0]["name"].encode("ascii","ignore"))
		except:
			row.append("")
		try:
			row.append(results[0][0]["sector"].encode("ascii","ignore"))
		except:
			row.append("")
		try:
			row.append(results[0][0]["url"].encode("ascii","ignore"))
		except:
			row.append("")
		try:
			row.append(results[0][0]["url_logo"].encode("ascii","ignore"))
		except:
			row.append("")
		try:
			row.append(results[0][0]["address"].encode("ascii","ignore"))
		except:
			row.append("")
		try:
			row.append(results[0][0]["telephone_number"].encode("ascii","ignore"))
		except:
			row.append("")

		writer.writerow(row)

print "finsihed!"
