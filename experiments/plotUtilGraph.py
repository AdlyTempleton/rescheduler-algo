import plotly.plotly as py
import plotly.offline
import sys
import csv
from plotly.graph_objs import *

filename = sys.argv[1]

listUpX, listUpY = [], []

listFFDX, listFFDY = [], []

listTRX, listTRY = [], []

with open(filename + '_up.csv') as csvfile:
	readCSV = csv.reader(csvfile, delimiter=',')
	for row in readCSV:
		listUpX.append(int(row[4]))
		listUpY.append(int(row[1]))
		
listUpX.append(3000000)
listUpY.append(listUpY[-1])
		
with open(filename + '_ffd.csv') as csvfile:
	readCSV = csv.reader(csvfile, delimiter=',')
	for row in readCSV:
		listFFDX.append(int(row[2]))
		listFFDY.append(int(row[1]))
		
with open(filename + '_tr.csv') as csvfile:
	readCSV = csv.reader(csvfile, delimiter=',')
	for row in readCSV:
		listTRX.append(int(row[3]))
		listTRY.append(float(row[2]))
		
traceUp = Scatter(name = "Scheduled", line={'shape': 'hv', 'width': 1}, x=listUpX, y=listUpY)
traceFFD = Scatter(name = "FFD", line={'width': 1}, x=listFFDX, y = listFFDY)
traceTR = Scatter(name = "Total Resources", line={'width': 1}, x=listTRX, y = listTRY)
data = Data([traceUp, traceFFD, traceTR])

plotly.offline.plot({
	"data": data,
	"layout": Layout(title="Node Utilization Graph")
})