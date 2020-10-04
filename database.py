import sqlite3
import json
import collections

class DB:

	def __init__(self):
		self.conn = sqlite3.connect('flat.db')
		self.cursor = self.conn.cursor()

	def data2sql(self, df):
		df.to_sql('flat_00', con=self.conn, if_exists='replace')
		self.cursor.execute('''
			create view if not exists v_flat_00 as
				select
					*
				from flat_00
			''')

	def createFlatTable(self):
		self.cursor.execute('''
			create table if not exists flat(
				id integer primary key autoincrement,
				cost integer,
				district varchar(128),
				count_rooms varchar(128),
				square varchar(128),
				count_floors varchar(128),
				flat_key integer,
				start_dttm datetime default current_timestamp,
				end_dttm datetime default (datetime('2999-12-31 23:59:59'))
			);
			''')

		self.cursor.execute('''
			create view if not exists v_flat as
				select 
					id
					,cost
					,district
					,count_rooms
					,square
					,count_floors
					,flat_key
				from flat
				where current_timestamp between start_dttm and end_dttm
			;
			''')

	def createTableNewFlats(self):
		self.cursor.execute('''
			create table if not exists flat_01 as 
				select
					t1.*
				from flat_00 t1
				left join v_flat t2
				on t1.flat_key = t2.flat_key
				where t2.flat_key is null;
			''')

		self.cursor.execute('''
			create view if not exists v_flat_01 as
				select
					*
				from flat_01
			''')

	def createTableUpdateFlats(self):
		self.cursor.execute('''
			create table flat_04 as 
				select 
					t1.*
				from flat_00 t1
				inner join v_flat t2
				on t1.flat_key = t2.flat_key
				and (
					t1.cost 			<> t2.cost
					or t1.district		<> t2.district
					or t1.count_rooms	<> t2.count_rooms
					or t1.square		<> t2.square
					or t1.count_floors	<> t2.count_floors
				);
			''')

	def createTableDeleteFlats(self):
		self.cursor.execute('''
			create table flat_05 as 
				select
					t1.flat_key
				from v_flat t1
				left join flat_00 t2
				on t1.flat_key = t2.flat_key
				where t2.flat_key is null;
		''')

	def createTableCheapFlats(self):
		self.cursor.execute('''
			create table if not exists flat_02 as
				select
					t1.*
				from flat_00 t1
				inner join v_flat t2
				on t1.flat_key = t2.flat_key
				where t1.cost < t2.cost
			''')

		self.cursor.execute('''
			create view if not exists v_flat_02 as
				select
					*
				from flat_02
			''')

	def createTableExpensiveFlats(self):
		self.cursor.execute('''
				create table if not exists flat_03 as
				select
					t1.district
				from flat_00 t1
				inner join v_flat t2
				on t1.flat_key = t2.flat_key
				where t1.cost > t2.cost
			''')

		self.cursor.execute('''
			create view if not exists v_flat_03 as
				select
					*
				from flat_03
			''')

	def insertData(self):
		self.cursor.execute('''
			update flat
			set end_dttm = current_timestamp
			where flat_key in (select flat_key from flat_05)
			and end_dttm = datetime('2999-12-31 23:59:59');
		''')
		
		self.cursor.execute('''
			update flat
			set end_dttm = current_timestamp
			where flat_key in (select flat_key from flat_04)
			and end_dttm = datetime('2999-12-31 23:59:59');
		''')

		self.cursor.execute('''
			insert into flat (
				cost
				,district
				,count_rooms
				,square
				,count_floors
				,flat_key
			)
			select 
				cost
				,district
				,count_rooms
				,square
				,count_floors
				,flat_key
			from flat_04;
		''')

		self.cursor.execute('''
			insert into flat (
				cost
				,district
				,count_rooms
				,square
				,count_floors
				,flat_key
			)
			select 
				cost
				,district
				,count_rooms
				,square
				,count_floors
				,flat_key
			from flat_01;
		''')

		self.conn.commit()

	def deleteTmpTables(self):
		self.cursor.execute('''
			drop table if exists flat_00;
		''')
		self.cursor.execute('''
			drop view if exists v_flat_00;
		''')
		self.cursor.execute('''
			drop table if exists flat_01;
		''')
		self.cursor.execute('''
			drop view if exists v_flat_01;
		''')
		self.cursor.execute('''
			drop table if exists flat_02;
		''')
		self.cursor.execute('''
			drop view if exists v_flat_02;
		''')
		self.cursor.execute('''
			drop table if exists flat_03;
		''')
		self.cursor.execute('''
			drop view if exists v_flat_03;
		''')
		self.cursor.execute('''
			drop table if exists flat_04;
		''')
		self.cursor.execute('''
			drop table if exists flat_05;
		''')

	def dataToJson(self, views_='v_flat_00'):
		self.cursor.execute(r'''
			select
				*
			from {}
			'''.format(views_))
		rows = self.cursor.fetchall()

		objects_list = []

		for row in rows:
		    d = collections.OrderedDict()
		    d["id"] = row[0]
		    d["cost"] = row[1]
		    d["district"] = row[2]
		    d["count_rooms"] = row[3]
		    d["square"] = row[4]
		    d["count_floors"] = row[5]
		    d["flat_key"] = row[6]
		    #d["start_dttm"] = row[7]
		    #d["end_dttm"] = row[8]
		    objects_list.append(d)
		j = json.dumps(objects_list)
		return json.loads(j)