from openpyxl import Workbook

workbook = Workbook() # "Сделали файл"
# print(workbook.active)
sheet = workbook.active # "Создали вкладку"
# print(sheet)


for i in range(10):
    # print(i)
   sheet['A' + str(i+2)] = str(i+1)




#
#
# for cell in sheet['A']:
#     cell = 'hello'


workbook.save(filename="hello_world.xlsx")  # Сохранили