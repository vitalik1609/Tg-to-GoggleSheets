import configparser
import json
import httplib2
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from telethon.sync import TelegramClient
from telethon import connection
from datetime import date, datetime

# класс для работы с сообщениями
from telethon.tl.functions.messages import GetHistoryRequest

# Считываем учетные данные
config = configparser.ConfigParser()
config.read("config.ini")

# Присваиваем значения внутренним переменным
api_id = config['Telegram']['api_id']
api_hash = config['Telegram']['api_hash']
username = config['Telegram']['username']

client = TelegramClient(username, api_id, api_hash, system_version="4.16.30-vxCUSTOM")

client.start()

async def dump_all_messages(channel):
	"""Записывает json-файл с информацией о всех сообщениях канала/чата"""
	offset_msg = 0    # номер записи, с которой начинается считывание
	limit_msg = 100   # максимальное число записей, передаваемых за один раз

	all_messages = []   # список всех сообщений
	total_count_limit = 0  # поменяйте это значение, если вам нужны не все сообщения

	class DateTimeEncoder(json.JSONEncoder):
		'''Класс для сериализации записи дат в JSON'''
		def default(self, o):
			if isinstance(o, datetime):
				return o.isoformat()
			if isinstance(o, bytes):
				return list(o)
			return json.JSONEncoder.default(self, o)

	while True:
		history = await client(GetHistoryRequest(
			peer=channel,
			offset_id=offset_msg,
			offset_date=None, add_offset=0,
			limit=limit_msg, max_id=0, min_id=0,
			hash=0))
		if not history.messages:
			break
		messages = history.messages
		for message in messages:
			all_messages.append(message.to_dict())
		offset_msg = messages[len(messages) - 1].id
		total_messages = len(all_messages)
		if total_count_limit != 0 and total_messages >= total_count_limit:
			break

	with open('channel_messages.json', 'w', encoding='utf8') as outfile:
		json.dump(all_messages, outfile, ensure_ascii=False, cls=DateTimeEncoder)

async def main():
	url = 'https://t.me/+Y5XvfbOnsu5lNDIy' # url канала
	channel = await client.get_entity(url)
	await dump_all_messages(channel)

	with open('channel_messages.json', encoding='utf8') as file:
		data = json.load(file) # Читаем записанные ранее сообщения
		data.pop() # Для корректной работы удаляем из общего списка сообщение o создании канала

	path_json = r"C:\Users\RedmiBook\PycharmProjects\TgOrdersParser\tg-orders-161089186def.json"
	creds_auth = ServiceAccountCredentials.from_json_keyfile_name(path_json, [
		'https://www.googleapis.com/auth/spreadsheets']).authorize(httplib2.Http())
	service = build('sheets', 'v4', http=creds_auth)
	sss = service.spreadsheets()
	sheet_id = '1EwhS7wAKvr8uceLjG52Io3ZbjXqu2H5gW2XX4_gYSuA'

	rows = []
	i = 0
	'''Считываем номер последней записанной в таблицу заявки'''
	with open('last_order.txt', encoding='utf-8') as f:
		last_order = f.read()
	while True:
		msg = data[i]["message"]
		if msg == '':   # обходим пустые сообщения (фото)
			i += 1
			continue
		msg = msg.split('\n')
		order = msg[0].split()[1]
		if order == last_order: # выходим из цикла если заявка уже есть в таблице
			break
		phone, name = msg.pop().split()
		msg.pop()
		address = msg.pop()[6:]
		tags = ('ванн', 'туалет', 'кухн', 'коридор', 'прихож', 'фартук', 'квартир', 'пол', 'стен', 'квартир', 'сануз')
		info = ' '.join([word for word in msg[2].split() if word.lower().startswith(tags)])
		'''пример записи в таблице'''
		'''28.02 | 1068 | Имя назначенного мастера | Статут заявки | Ремонт ванной Удельная | Сумма заказа | Комиссия | Виталий | 89991234455'''
		rows.append([date.today().strftime('%d.%m.%Y'), '№ '+order, 'ОТДАТЬ', None, None, None, info, address, name, phone])
		i += 1
	last_order = rows[0][1].split()[1]
	'''Записываем номер последней записанной в таблицу заявки'''
	with open('last_order.txt', 'w', encoding='utf-8') as f:
		f.write(last_order)
	rows.reverse()


	# Получаем данные из первого столбца
	range_ = 'Заказы!A:A'
	result = sss.values().get(spreadsheetId=sheet_id, range=range_).execute()
	values = result.get('values', [])

	# Находим первую пустую ячейку в первом столбце
	next_empty_row = len(values)+1
	#for row in values:
	#	if row and row[0]:
	#		next_empty_row = values.index(row) + 1
	#		break
	# Преобразуем данные в виде, пригодный для вставки
	data = []
	for row in rows:
		data.append({
			'range': f'A{next_empty_row + i}:J{next_empty_row + i}',
			'majorDimension': 'ROWS',
			'values': [
				[cell for cell in row if cell is not None]  # Пропускаем ячейки со значением None
			]
		})
	# Добавляем данные, начиная с найденной строки
	#range_ = f'A{next_empty_row}:L'
	#body = {'values': rows}
	#result = sss.values().update(
	#	spreadsheetId=sheet_id, range=range_, valueInputOption='RAW', body=body
	#).execute()
	# Вставляем данные
	#body = {'data': data}
	#result = sss.spreadsheets().values().batchUpdate(
	#	spreadsheetId=sheet_id, body=body
	#).execute()
	r = sss.values().append(
		spreadsheetId=sheet_id,
		range=f'Заказы!A{next_empty_row}:J{next_empty_row}',
		valueInputOption='RAW',
		body={'values': rows}
	).execute()

with client:
	client.loop.run_until_complete(main())

