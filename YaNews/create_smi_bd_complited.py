from parsing_smi_info import make_smi_profile
import time
import random

with open('log.txt', 'r', encoding='utf8') as f:
	for el in f:
		folder_name = el.strip()
		smi_link = ('https://news.yandex.ru/smi/' + folder_name)
		# print(smi_link, folder_name)
		print(f'Записываю {smi_link}...')
		make_smi_profile(smi_link, folder_name)
		delay_time = random.randint(3, 8)
		time.sleep(delay_time)
