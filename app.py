# coding=utf8
from flask import Flask
from parse import get_data
from clear import clear_data
from database import DB
import json

app = Flask(__name__)

@app.route('/<name>')

def main(name):
	return f'hello {name}'

@app.route('/api/get_data_by_location/<location>')
def parsedata(location=''):
	try:
		dataList = get_data(location)
		df = clear_data(dataList)
		db = dataBaseEngTool(df)
		return json.dumps(db.dataToJson(), ensure_ascii=False)
	except Exception as e:
		return json.dumps({"status": "err", "error_text": str(e)})

@app.route('/api/get_view_from_db/<view>')
def dataFromView(view):
	try:
		db = DB()
		return json.dumps(db.dataToJson(view), ensure_ascii=False)
	except Exception as e:
		return json.dumps({"status": "err", "error_text": str(e)})

def dataBaseEngTool(df):
	db = DB()
	db.deleteTmpTables()
	db.data2sql(df)
	db.createFlatTable()
	db.createTableNewFlats()
	db.createTableUpdateFlats()
	db.createTableDeleteFlats()
	db.insertData()
	db.createTableCheapFlats()
	db.createTableExpensiveFlats()
	return db


if  __name__ == '__main__':
	app.debug = True
	app.run()