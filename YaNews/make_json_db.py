import glob
from openpyxl import Workbook

workbook = Workbook()
db_full = workbook.active  # "Создали вкладку"

j = 0  # smi counter
filenames_list = glob.glob("smi_bd\\*")
smi_db_json = {}
wrong_records = []
headers = {
    'A': 'id',
    'B': 'url',
    'C': 'ya_link',
    'D': 'description',
    'E': 'email',
    'F': 'phone',
    'G': 'address',
    'H': 'chief_position',
    'I': 'chief_name'
}

for files in filenames_list:
    j += 1
    with open(f'{files}', 'r', encoding='utf8') as f:
        smi_data = f.read().splitlines()
        folder_name = files.replace('smi_bd', '').replace('.txt', '').strip()[1:]
        try:
            site = smi_data[4].split(sep=': ')[1]
            ya_link = 'https://news.yandex.ru/smi/' + folder_name
            discription = smi_data[0]
            email = smi_data[5].split(sep=':')[1]
            phone = smi_data[3].split(sep=': ')[1]
            address = smi_data[2].split(sep=': ')[1]
            chief_position = smi_data[1].split(sep=': ')[0]
            chief_name = smi_data[1].split(sep=': ')[1]
            smi_db_json[j] = [site, ya_link, discription, email, phone, address, chief_position, chief_name]
        except IndexError:
            wrong_records.append(j)

for letter in headers:
    db_full[letter + '1'] = headers[letter]

for number in range(len(filenames_list)):
    try:
        db_full['A' + str(number + 2)] = (number + 1)
        db_full['B' + str(number + 2)] = str(smi_db_json[number + 1][0])
        db_full['C' + str(number + 2)] = str(smi_db_json[number + 1][1])
        db_full['D' + str(number + 2)] = str(smi_db_json[number + 1][2])
        db_full['E' + str(number + 2)] = str(smi_db_json[number + 1][3])
        db_full['F' + str(number + 2)] = str(smi_db_json[number + 1][4])
        db_full['G' + str(number + 2)] = str(smi_db_json[number + 1][5])
        db_full['H' + str(number + 2)] = str(smi_db_json[number + 1][6])
        db_full['I' + str(number + 2)] = str(smi_db_json[number + 1][7])
    except KeyError:
        pass
#
#
workbook.save(filename="bd.xlsx")  # Сохранили

# for el in wrong_records:
#     print(el)
