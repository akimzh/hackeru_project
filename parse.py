import requests
from bs4 import BeautifulSoup
import unidecode
import re

def get_data(location):
	url = 'https://krisha.kz/prodazha/kvartiry/{}/?page={}'
	page_num = 1
	result_all_flatsList = []
	while page_num < 2:
		result_page = __parse_Page(url, location, page_num)
		if result_page == -1:
			break
		else:
			for row in result_page:
				result_all_flatsList.append(row)
		page_num += 1
	return result_all_flatsList


def __parse_Page(url, location, page_num):
	result = requests.get(url.format(location, page_num))

	resultFlatList = []

	soup = BeautifulSoup(result.content, 'html.parser')

	flatList = soup.select('.a-card__header')

	for flat in flatList:
		resultRow = []
		try:	
			resultRow.append(flat.select_one('.a-card__title').text)
			price_ = flat.select_one('.a-card__price').text
			price_ = unidecode.unidecode(price_)
			price_ = price_.strip('\n ')
			price_ = re.sub('\D', '', price_)
			resultRow.append(price_)
			district_ = flat.select_one('.a-card__subtitle').text
			district_ = district_.strip('\n ')
			resultRow.append(district_)
			key_ = flat.select_one('.a-card__title').get('href')
			key_ = key_.split('/')[-1]
			resultRow.append(key_)
			resultFlatList.append(resultRow)

		except Exception:
			pass

	return resultFlatList
