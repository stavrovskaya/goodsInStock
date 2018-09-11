
# coding: utf-8

# In[1]:


# -*- coding: utf-8 -*-
import requests, json
from requests.exceptions import ConnectionError
from time import sleep
import sys
import pandas as pd
import re
import datetime

from feed_parser import FeedParser
import pygsheets
#in case of time out
#pygsheets.client.GOOGLE_SHEET_CELL_UPDATES_LIMIT=5000

gc = pygsheets.authorize()


# In[2]:


# --- Входные данные ---
# Логин клиента рекламного агентства
# Обязательный параметр, если запросы выполняются от имени рекламного агентства
clientLogin = 'XXX'

feedUrl = "XXX" 


# OAuth-токен пользователя, от имени которого будут выполняться запросы
token = 'XXX'

googleSpreadsheets = 'XXX'


# In[3]:


# --- Подготовка, выполнение и обработка запроса ---
#  Создание HTTP-заголовков запроса
headers = {"Authorization": "Bearer " + token,  # OAuth-токен. Использование слова Bearer обязательно
           "Client-Login": clientLogin,  # Логин клиента рекламного агентства
           "Accept-Language": "ru",  # Язык ответных сообщений
           }
  #  Метод для корректной обработки строк в кодировке UTF-8 как в Python 3, так и в Python 2
if sys.version_info < (3,):
        def u(x):
            try:
                return x.encode("utf8")
            except UnicodeDecodeError:
                return x
else:
        def u(x):
            if type(x) == type(b''):
                return x.decode('utf8')
            else:
                return x
            
def getCampaigns():
    #  Адрес сервиса Campaigns для отправки JSON-запросов (регистрозависимый)
    CampaignsURL = 'https://api.direct.yandex.com/json/v5/campaigns'

    # Создание тела запроса
    body = {"method": "get",  # Используемый метод.
            "params": {"SelectionCriteria": {
                "States": ["ON", "SUSPENDED"]},  # Критерий отбора кампаний. Для получения всех кампаний должен быть пустым
                       "FieldNames": ["Id", "Name"]  # Имена параметров, которые требуется получить.
                       }}

    # Кодирование тела запроса в JSON
    jsonBody = json.dumps(body, ensure_ascii=False).encode('utf8')

    campaigns = {}
    # Выполнение запроса
    try:
        result = requests.post(CampaignsURL, jsonBody, headers=headers)

        # Отладочная информация
        # print("Заголовки запроса: {}".format(result.request.headers))
        # print("Запрос: {}".format(u(result.request.body)))
        # print("Заголовки ответа: {}".format(result.headers))
        # print("Ответ: {}".format(u(result.text)))
        # print("\n")

        # Обработка запроса
        if result.status_code != 200 or result.json().get("error", False):
            print("Произошла ошибка при обращении к серверу API Директа.")
            print("Код ошибки: {}".format(result.json()["error"]["error_code"]))
            print("Описание ошибки: {}".format(u(result.json()["error"]["error_detail"])))
            print("RequestId: {}".format(result.headers.get("RequestId", False)))
        else:
            print("RequestId: {}".format(result.headers.get("RequestId", False)))
            print("Информация о баллах: {}".format(result.headers.get("Units", False)))
            # Вывод списка кампаний
            for campaign in result.json()["result"]["Campaigns"]:
                #print("Рекламная кампания: {} №{}".format(u(campaign['Name']), campaign['Id']))
                campaigns[campaign['Name']] = campaign['Id']


            if result.json()['result'].get('LimitedBy', False):
                # Если ответ содержит параметр LimitedBy, значит,  были получены не все доступные объекты.
                # В этом случае следует выполнить дополнительные запросы для получения всех объектов.
                # Подробное описание постраничной выборки - https://tech.yandex.ru/direct/doc/dg/best-practice/get-docpage/#page
                print("Получены не все доступные объекты.")


    # Обработка ошибки, если не удалось соединиться с сервером API Директа
    except ConnectionError:
        # В данном случае мы рекомендуем повторить запрос позднее
        print("Произошла ошибка соединения с сервером API.")

    # Если возникла какая-либо другая ошибка
    except:
        # В данном случае мы рекомендуем проанализировать действия приложения
        print("Произошла непредвиденная ошибка.", sys.exc_info()[0])
    return(campaigns)


# In[4]:


#get adds


def getAdds(camp_ids):
    addURL = "https://api.direct.yandex.com/json/v5/ads"

    # Создание тела запроса
    body = {"method": "get",  # Используемый метод.
            "params": {"SelectionCriteria": {
                "CampaignIds": camp_ids,#list(campaigns.values())[0:10],
                "States": ["ON", "SUSPENDED"],
                "Types": [ "TEXT_AD"]
            },  # Критерий отбора кампаний. Для получения всех кампаний должен быть пустым
                       "FieldNames": ["Id", "AdGroupId", "State", "Status", "StatusClarification", "Type"],  # Имена параметров, которые требуется получить.
                       "TextAdFieldNames": ["Title", "Text", "Href"]
                       }}

    # Кодирование тела запроса в JSON
    jsonBody = json.dumps(body, ensure_ascii=False).encode('utf8')

    ids = []
    addGroupIds = []
    state = []
    title = []
    text = []
    hrefs = []
    # Выполнение запроса
    try:
        result = requests.post(addURL, jsonBody, headers=headers)

        # Отладочная информация
        # print("Заголовки запроса: {}".format(result.request.headers))
        # print("Запрос: {}".format(u(result.request.body)))
        # print("Заголовки ответа: {}".format(result.headers))
        # print("Ответ: {}".format(u(result.text)))
        # print("\n")

        # Обработка запроса
        if result.status_code != 200 or result.json().get("error", False):
            print("Произошла ошибка при обращении к серверу API Директа.")
            print("Код ошибки: {}".format(result.json()["error"]["error_code"]))
            print("Описание ошибки: {}".format(u(result.json()["error"]["error_detail"])))
            print("RequestId: {}".format(result.headers.get("RequestId", False)))
        else:
            print("RequestId: {}".format(result.headers.get("RequestId", False)))
            print("Информация о баллах: {}".format(result.headers.get("Units", False)))
            # Вывод списка кампаний
            for add in result.json()["result"]["Ads"]:
                #print("Рекламное объявление: №{} состояние={} статус={} тип={}".format(add['Id'], add['State'], add['Status'], add['Type']))

#                 print("{} {} {}".format(add["TextAd"]["Title"], 
#                                            add["TextAd"]["Text"], add["TextAd"]["Href"]))
                href = re.sub(r"((/?\?)|&)utm_.*$", "", add["TextAd"]["Href"])

                ids.append(add['Id'])
                addGroupIds.append(add['AdGroupId'])
                state.append(add['State'])
                title.append(add["TextAd"]["Title"])
                text.append(add["TextAd"]["Text"])
                hrefs.append(href)

            if result.json()['result'].get('LimitedBy', False):
                # Если ответ содержит параметр LimitedBy, значит,  были получены не все доступные объекты.
                # В этом случае следует выполнить дополнительные запросы для получения всех объектов.
                # Подробное описание постраничной выборки - https://tech.yandex.ru/direct/doc/dg/best-practice/get-docpage/#page
                print("Получены не все доступные объекты.")


    # Обработка ошибки, если не удалось соединиться с сервером API Директа
    except ConnectionError:
        # В данном случае мы рекомендуем повторить запрос позднее
        print("Произошла ошибка соединения с сервером API.")

    # Если возникла какая-либо другая ошибка
    except:
        # В данном случае мы рекомендуем проанализировать действия приложения
        print("Произошла непредвиденная ошибка.", sys.exc_info())
    
    adds = pd.DataFrame({"id" : ids, 
                       "addGroupId" : addGroupIds,
                       "state" : state,
                       "title" : title,
                       "text" : text,
                       "href" : hrefs}, dtype = "object")
    return(adds)


# In[5]:


def getAddsForCampaigns(campaignIds):
    i = 0
    step = 10
    adds = pd.DataFrame({"id" : [],
                         "addGroupId" : [],
                         "state" : [],
                         "title" : [],
                         "text" : [],
                         "href" : []})
    while i < len(campaignIds):
        print(i)
        left, right = i, min(i + step, len(campaignIds))
        adds = adds.append(getAdds(campaignIds[left:right]))
        i += step
    return(adds)


# In[6]:


def printToExcel(newGoods, potentialPagesAdds, appearedGoodsAdds, outOfStockGoodsAdds, client):
        #excel
    now = datetime.datetime.now()
    writer = pd.ExcelWriter(client + '_adds_' + now.strftime("%d-%m-%Y") + '.xlsx')
    
    newGoods.to_excel(writer,'Новые товары', index = False)

    potentialPagesAdds.to_excel(writer,'Нет в фиде', index = False)
    appearedGoodsAdds.to_excel(writer,'Появились в наличии', index = False)
    outOfStockGoodsAdds.to_excel(writer,'Сейчас не в наличии', index = False)   
    
    workbook  = writer.book
    text_format = workbook.add_format({'text_wrap': True})
    
    worksheet = writer.sheets['Новые товары'] 
    worksheet.set_column('A:A', 15, text_format)
    worksheet.set_column('B:B', 15, text_format)
    worksheet.set_column('C:C', 15, text_format)
    worksheet.set_column('D:D', 15, text_format)
    worksheet.set_column('E:E', 15, text_format)
    worksheet.set_column('F:F', 15, text_format)
    worksheet.set_column('G:G', 30, text_format)
    worksheet.set_column('H:H', 15, text_format)
    
    for sheet in ['Нет в фиде', 'Появились в наличии', 'Сейчас не в наличии']:
        worksheet = writer.sheets[sheet] 
        worksheet.set_column('A:A', 15, text_format)
        worksheet.set_column('B:B', 15, text_format)
        worksheet.set_column('C:C', 15, text_format)
        worksheet.set_column('D:D', 50, text_format)
        worksheet.set_column('E:E', 50, text_format)

    
    writer.save()
    writer.close()

def printToGoogleSpreadsheets(newGoods, potentialPagesAdds, appearedGoodsAdds, outOfStockGoodsAdds, gsprshtBook):
    now = datetime.datetime.now()
    sht0 = gsprshtBook.worksheet_by_title("Дата обновления")
    sht0.update_cell('A1', now.strftime("%d-%m-%Y %H:%M:%S"))
    
    sht1 = gsprshtBook.worksheet_by_title("Новые товары")
    sht1.clear()
    sht1.set_dataframe(newGoods,(1,1))
    sht2 = gsprshtBook.worksheet_by_title("Нет в фиде")
    sht2.clear()
    sht2.set_dataframe(potentialPagesAdds,(1,1))

    sht3 = gsprshtBook.worksheet_by_title("Появились в наличии")
    sht3.clear()
    sht3.set_dataframe(appearedGoodsAdds,(1,1))

    sht4 = gsprshtBook.worksheet_by_title("Сейчас не в наличии")
    sht4.clear()
    sht4.set_dataframe(outOfStockGoodsAdds,(1,1))


# In[7]:


def formAddChangesList(feedDf, adds, prodPageRe = None, client = None, gsprshtBook = None):
    allGoods = pd.merge(feedDf, adds, left_on = "url", right_on = "href", how = "outer")
    
    newGoods = allGoods.loc[allGoods.id.isnull() & allGoods.available, feedDf.columns].drop_duplicates()
    
    newGoods = newGoods[["available", 
              "category", 
              "vendor", 
              "vendorCode", 
              "typePrefix", 
              "model", 
              "name", 
              "price", 
              "url"]].sort_values("name")
    
    disappearedGoodsAdds = allGoods.loc[(allGoods.url.isnull()) & (allGoods.state == "ON") , adds.columns]
    disappearedGoodsAddsUrls = list(set(disappearedGoodsAdds.href))
    disappearedGoodsAddsUrls.sort()
    if prodPageRe is not None:
        potentialPages = [x for x in disappearedGoodsAddsUrls if not re.match(prodPageRe, x) is None]
    else:
        potentialPages = disappearedGoodsAddsUrls
        
    potentialPagesAdds = adds[adds.href.isin(potentialPages)]
    potentialPagesAdds = potentialPagesAdds.sort_values("id")
    
    appearedGoodsAdds = allGoods.loc[(allGoods.available) & (allGoods.state == "SUSPENDED") , adds.columns].drop_duplicates()
    appearedGoodsAdds = appearedGoodsAdds.sort_values("id")
    
    outOfStockGoodsAdds = allGoods.loc[(allGoods.available == False) & (allGoods.state == "ON") , adds.columns].drop_duplicates()
    outOfStockGoodsAdds = outOfStockGoodsAdds.sort_values("id")
    #excel
    if client is not None:
        printToExcel(newGoods, potentialPagesAdds, appearedGoodsAdds, outOfStockGoodsAdds, client)
    #google spreadsheets
    if gsprshtBook is not None:
        printToGoogleSpreadsheets(newGoods, potentialPagesAdds, appearedGoodsAdds, outOfStockGoodsAdds, gsprshtBook)
    return(newGoods, potentialPagesAdds, appearedGoodsAdds, outOfStockGoodsAdds)
    


# In[8]:


def getCampaignsFormGoogleSpreadsheet(googleSpreadsheetsBook):
    try:
            sht = googleSpreadsheetsBook.worksheet_by_title("Кампании")
            campaignsDf = sht.get_as_df()
            campaignsDf = campaignsDf.drop('', axis='columns')
    except:
            googleSpreadsheetsBook.add_worksheet("Кампании")
            sht = googleSpreadsheetsBook.worksheet_by_title("Кампании")
            campaigns = getCampaigns()
            campaignsDf = pd.DataFrame({"campaignName":list(campaigns.keys()), "campaignId":list(campaigns.values())})
            sht.set_dataframe(campaignsDf,(1,1))
    return(campaignsDf)
    


# In[9]:


if __name__ == '__main__':
    googleSpreadsheetsBook = gc.open_by_key(googleSpreadsheets)
    
    campaignsDf = getCampaignsFormGoogleSpreadsheet(googleSpreadsheetsBook)
    
    adds = getAddsForCampaigns(list(campaignsDf.campaignId))
     # get feed
    feedParser = FeedParser(feedUrl)
    feedDf = feedParser.url_offer_df

    prodPageRe = r'^XXX$'
    
    formAddChangesList(feedDf, adds, prodPageRe, clientLogin, googleSpreadsheetsBook)
    

