def make_df_from_telegram_list(filename):
    """Преобразовывает список айтишных телеграмм-каналов из https://github.com/goq/telegram-list в pandas_df"""
    """Для начала работы нужно сохранить нужные элементы страницы в txt-файл на компе, и передать его имя в эту функцию"""
    it_telegram_dict = {}

    with open(f'{filename}.txt', 'r', encoding='utf-8') as f:
        text = f.readlines()

    for el in range(len(text)):
        smi = text[el].splitlines()[0].split("""[""")[1].split("""]""")
        smi_name = smi[0]
        smi_descr = smi[1].split(' — ')[-1]
        smi_telegram = smi[1].split(' — ')[0][1:-1]
        it_telegram_dict[el] = (smi_name, smi_telegram, 'telegram', filename, smi_descr)

    telegram_df = pd.DataFrame(it_telegram_dict).T
    telegram_df.columns = ['name', 'telegram', 'type', 'subtype', 'description']
    
    return telegram_df