import pandas as pd
from datetime import datetime
import gspread
import numpy as np
from gspread_formatting import *
import time
import configparser

config = configparser.ConfigParser()
config.read('./config.example.INI')
gs = gspread.service_account(filename='./credits.json') # подключаем файл с ключами и пр.
gen_sh = gs.open_by_url(config['DEFAULT']['TABLE_KEY']) # подключаем таблицу
database_list = gen_sh.sheet1 # получаем лист с бд
database_head = database_list.row_values(1) # копируем шапку таблицы
workers_sheet = gen_sh.worksheet("Сотрудники")  # получаем лист со списком сотруднков


# валидация
dict_sheet = gen_sh.worksheet("dic")
dict_value = dict_sheet.get_values()
dict_head = dict_sheet.row_values(1)
dictionary = pd.DataFrame(dict_value, columns=dict_head)

dictionary = dictionary.drop(index=[0])

def validation_workers(sheet):
    validation_rule = DataValidationRule(
        BooleanCondition('ONE_OF_LIST', dictionary['Источник контакта'].tolist()),
        showCustomUi=True)
    set_data_validation_for_cell_range(sheet, 'V2:V', validation_rule)

    validation_rule = DataValidationRule(
        BooleanCondition('ONE_OF_LIST', dictionary['Соцсеть'].tolist()),
        showCustomUi=True)
    set_data_validation_for_cell_range(sheet, 'E2:E', validation_rule)

    validation_rule = DataValidationRule(
        BooleanCondition('ONE_OF_LIST', dictionary['Ниша'].tolist()),
        showCustomUi=True)
    set_data_validation_for_cell_range(sheet, 'P2:P', validation_rule)

    validation_rule = DataValidationRule(
        BooleanCondition('ONE_OF_LIST', dictionary['Стадия сделки'].tolist()),
        showCustomUi=True)
    set_data_validation_for_cell_range(sheet, 'F2:F', validation_rule)

    validation_rule = DataValidationRule(
        BooleanCondition('ONE_OF_LIST', dictionary['Заход'].tolist()),
        showCustomUi=True)
    set_data_validation_for_cell_range(sheet, 'G2:G', validation_rule)

    validation_rule = DataValidationRule(
        BooleanCondition('ONE_OF_LIST', dictionary['Город'].tolist()),
        showCustomUi=True)
    set_data_validation_for_cell_range(sheet, 'O2:O', validation_rule)

    validation_rule = DataValidationRule(
        BooleanCondition('ONE_OF_LIST', dictionary['Цель контакта'].tolist()),
        showCustomUi=True)
    set_data_validation_for_cell_range(sheet, 'I2:I', validation_rule)

def parse_df(vals):
    header = vals[0]
    vals.pop(0)
    return pd.DataFrame(vals, columns=header)

def getSheetData(sheet):
    sheet1 = sheet.sheet1
    return parse_df(sheet1.get_values())

def get_all_data(urls, names):
    df_resp = pd.DataFrame()
    for url in urls:
        df_temp = getSheetData(gs.open_by_url(url))
        df_temp["Ответственный"] = names[urls.index(url)]
        df_resp = pd.concat([df_resp, df_temp], ignore_index=True)
        validation_workers(gs.open_by_url(url).sheet1)
    return df_resp

bad_stages = config['DEFAULT']['BAD_STAGES'].split(',')

def count_time(pand_data, names_list):
    days_needed = int(config['DEFAULT']['DAYS_NEEDED'])
    todays_date = datetime.now()
    for i in pand_data.index:
        date_actual = datetime.strptime(pand_data.loc[i]['Дата контакта'], "%d.%m.%Y")
        delta = todays_date - date_actual
        if delta.days >= days_needed and pand_data.loc[i]['Стадия сделки'] in bad_stages:
            pand_data.loc[i, 'Дата контакта'] = todays_date
            pand_data.loc[i, 'Ответственный'] = names_list[
                (names_list.index(pand_data.loc[i]['Ответственный']) + 1) % len(names_list)]

    pand_data['Дата контакта'] = pd.to_datetime(pand_data['Дата контакта'], dayfirst=True).dt.date
    #return pand_data.sort_values(by=['Дата контакта', 'Заход', 'Стадия сделки', 'Цель контакта'])
    return pand_data

def update_sheet(sheet, data):
    sheet.clear()
    sheet.update(data)

def updateSheets(data, temp, urls, names):
    for name in names:
        name_data = data.loc[data["Ответственный"] == name]
        name_temp = temp.loc[temp["Ответственный"] == name]
        if not comparison_df(name_temp, name_data):
            sheet = gs.open_by_url(urls[names.index(name)])
            df_temp = getSheetData(sheet)
            df_temp = df_temp.set_index('ID', drop=False)
            for_delete = np.setdiff1d(df_temp['ID'].tolist(), name_data['ID'].tolist())
            for_add = np.setdiff1d(name_data['ID'].tolist(), df_temp['ID'].tolist())
            df_temp = df_temp.drop(index=for_delete)
            df_temp = pd.concat([df_temp, name_data.loc[for_add]], join="inner")
            df_temp = df_temp.sort_values(by=['Дата контакта', 'Заход', 'Стадия сделки', 'Цель контакта'])
            df_temp['Дата контакта'] = pd.to_datetime(df_temp['Дата контакта'], dayfirst=True).dt.date
            df_temp['Дата контакта'] = df_temp['Дата контакта'].apply(date_trasform)
            vals = [df_temp.columns.values.tolist()] + df_temp.values.tolist()
            update_sheet(sheet.sheet1, vals)


    return

def date_trasform(date):
    return date.strftime("%d.%m.%Y")

def comparison_df(data1, data2):
    d1 = data1.index.tolist()
    d2 = data2.index.tolist()
    d1.sort()
    d2.sort()
    return d1 == d2

def validation(sheet):
    validation_rule = DataValidationRule(
        BooleanCondition('ONE_OF_LIST', dictionary['Источник контакта'].tolist()),
        showCustomUi=True)
    set_data_validation_for_cell_range(sheet, 'V2:V', validation_rule)

    validation_rule = DataValidationRule(
        BooleanCondition('ONE_OF_LIST', dictionary['Соцсеть'].tolist()),
        showCustomUi=True)
    set_data_validation_for_cell_range(sheet, 'D2:D', validation_rule)

    validation_rule = DataValidationRule(
        BooleanCondition('ONE_OF_LIST', dictionary['Ниша'].tolist()),
        showCustomUi=True)
    set_data_validation_for_cell_range(sheet, 'P2:P', validation_rule)

    validation_rule = DataValidationRule(
        BooleanCondition('ONE_OF_LIST', dictionary['Стадия сделки'].tolist()),
        showCustomUi=True)
    set_data_validation_for_cell_range(sheet, 'F2:F', validation_rule)

    validation_rule = DataValidationRule(
        BooleanCondition('ONE_OF_LIST', dictionary['Заход'].tolist()),
        showCustomUi=True)
    set_data_validation_for_cell_range(sheet, 'G2:G', validation_rule)

    validation_rule = DataValidationRule(
        BooleanCondition('ONE_OF_LIST', dictionary['Город'].tolist()),
        showCustomUi=True)
    set_data_validation_for_cell_range(sheet, 'O2:O', validation_rule)

    validation_rule = DataValidationRule(
        BooleanCondition('ONE_OF_LIST', dictionary['Цель контакта'].tolist()),
        showCustomUi=True)
    set_data_validation_for_cell_range(sheet, 'I2:I', validation_rule)


def main():
    database_values = database_list.get_values()
    urls_list = workers_sheet.col_values(3)  # получаем список ссылок на файлы сотрудников (первый элеменнт - название колонки)
    urls_list.pop(0)
    names_list = workers_sheet.col_values(1)  # получаем список имен сотрудников (первый элемент - название колонки)
    names_list.pop(0)


    temp_data = get_all_data(urls_list, names_list)
    bd_data = parse_df(database_values)

    temp_data = temp_data.set_index('ID', drop=False)
    bd_data = bd_data.set_index('ID', drop=False)

    bd_data.update(temp_data)
    bd_data = count_time(bd_data, names_list)

    updateSheets(bd_data, temp_data, urls_list, names_list)

    bd_data = bd_data.sort_values(by=['Дата контакта', 'Заход', 'Стадия сделки', 'Цель контакта'])
    bd_data['Дата контакта'] = pd.to_datetime(bd_data['Дата контакта'], dayfirst=True).dt.date
    bd_data['Дата контакта'] = bd_data['Дата контакта'].apply(date_trasform)
    vals = [bd_data.columns.values.tolist()] + bd_data.values.tolist()
    update_sheet(database_list, vals)

if __name__ == '__main__':
    while True:
        try:
            main()
        except Exception:
            print(Exception)
        time.sleep(int(config['DEFAULT']['SLEEP']))
