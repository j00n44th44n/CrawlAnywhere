import queue
import urllib3
import socket
import requests
import os
import sys
import re
from bs4 import BeautifulSoup, SoupStrainer
from requests_html import HTMLSession
from requests_html import HTML
import nltk
import pymongo
from pymongo import MongoClient
from requests.utils import quote
from more import Consult
from sklearn.externals.joblib import dump, load

username = ''
password = ''
host = "10.6.100.71"
port = 3128

dict = {}

client = MongoClient('localhost', 27017,connect=False)
rev_path = ''

proxies = {}
#     'http': f'http://{username}:{password}@{host}:{port}',
#     'https': f'https://{username}:{password}@{host}:{port}'
# }

def download(url,use_proxy,offline):
    if offline and url.startswith(rev_path[:-9]):
        with open(url, encoding='utf8', errors='ignore') as f:
            return f.read()
    else:
        return requests.get(url, proxies=proxies).text if use_proxy else requests.get(url).text

def get_links(page,url,offline):
    if offline:
        links = re.findall('(href=".*.htm")',page)
        html_links = re.findall('(href=".*.html" tppabs)',page)
        name,path = op(url)
        for i in range(len(html_links)):
            html_links[i] = (os.path.join(rev_path[:-9],path,html_links[i][6:-8]),'')
        for i in range(len(links)):            
            links[i] = (os.path.join(rev_path,links[i][6:-1]),'')
        links.extend(html_links)
        return links
    return re.findall('"((http)s?://.*?)"', page)

def get_text_from_html(data):
    # removing js
    while(True):
        i = data.find('<script',0,len(data))
        if i == -1:
            break
        j = data.find('</script>',i,len(data))
        data = data.replace(data[i:j+9],' ',1)

    # data = re.sub('<script*</script>','',data)

    # removing CSS
    while(True):
        i = data.find('<style',0,len(data))
        if i == -1:
            break
        j = data.find('</style>',i,len(data))
        data = data.replace(data[i:j+8],' ',1)

    # data = re.sub('<style*</script>','',data)

    # removing Comments
    while(True):
        i = data.find('/*',0,len(data))
        if i == -1:
            break
        j = data.find('*/',i,len(data))
        if j == -1:
            break
        data = data.replace(data[i:j+2],' ',1)

    soup = BeautifulSoup(data,"html5lib")
    text = soup.get_text(strip=True,separator=' ')
    text = text.lower()
    text = text.replace('&aacute;','á')
    text = text.replace('&eacute;','é')
    text = text.replace('&iacute;','í')
    text = text.replace('&oacute;','ó')
    text = text.replace('&uacute;','ú')
    text = text.replace('&ntilde;','ñ')
    return text

def get_ip_from_host(url_host):
    try:
        ips = socket.gethostbyname_ex(url_host)
    except socket.gaierror:
        ips=[]
    return ips[0] if len(ips) > 0 else None

def create_name(folder):    
    return os.path.join(folder,str(len(os.listdir('crawling')))+".txt")

def create_url_name(folder):    
    return os.path.join(folder,"urls.txt")

def op(url,offline):
    if offline:
        url = url.replace(rev_path,'')
        url = 'revolico' + url
        try:
            i = url.rindex('\\')
        except ValueError:
            i = 0
        try:
            j = url.rindex('/')
        except ValueError:
            j = 0
        i = max(i,j)
        name = url[i+1:]
        path = url[:i]
        return name,path
    else:
        url = url.replace(rev_path,'')
        url = 'revolico' + url
        name = ''
        path = ''
        if url.endswith(".html") or url.endswith(".htm"):
            try:
                i = url.rindex('/')
            except ValueError:
                i = 0
            name = url[i+1:]
            path = url[:i]
            return name,path
        else:
            name = 'index.html'
            path = url
        return name,path


def create_path(url,word,offline):
    if offline:
        name,path = op(url,offline)
        path = path.replace('revolico',word)
        new_path = os.path.join(os.getcwd(),path)
        if not os.path.exists(new_path):
            os.makedirs(new_path)
        return os.path.join(new_path,name)
    else:
        name,path = op(url,offline)
        path = path.replace('revolico',word)
        new_path = os.path.join(os.getcwd(),path)
        if not os.path.exists(new_path):
            os.makedirs(new_path)
        return os.path.join(new_path,name)



def save_doc(url,text,offline):
    with open(create_path(url,'crawling',offline),'w', encoding='utf8', errors='ignore') as f:
        f.write(text)

def save_indexer(links):
    directory = os.path.join(os.getcwd(),'ind_url')
    if not os.path.exists(directory):
        os.makedirs(directory)
    with open(create_url_name('ind_url'),'w', encoding='utf8', errors='ignore') as f:
        for link in links:
            f.write(link+"\n")

def real_web_name(url : str):
    return (url.count('.ico') + url.count('.gif') + url.count('.jpg') + url.count('.js') + url.count('.png') + url.count('.css')) == 0

def crawler(seed_url,offline=False,deep = -1,proxy = False,user_name = None,passwrd = None,host_ip = None,lport = None):
    l = []
    c = 0
    global proxies,username,password,host,port
    if proxy:
        username = quote(user_name)
        password = quote(passwrd)
        host = host_ip
        port = lport
        proxies = {
            'http': f'http://{username}:{password}@{host}:{port}',
            'https': f'https://{username}:{password}@{host}:{port}'
        }
    # db_words,db_global_words = init_db()
    q = queue.Queue()
    for url in seed_url:
        q.put((url,0))
    while(not q.empty()):        
        url,d = q.get()
        if url in l:
            continue
        c+=1
        # if url.endswith('.html'):
        #     print('OOOOOOOOOOO')
        l.append(url)
        print(url)
        print(f'page: {c}')
        html = download(url,proxy,offline)
        # html = crawl_js(html)
        links = get_links(html,url,offline)
        # ! getting message headers
        # if url.endswith('.html'):
        # title = get_title(html,url)
        # print(links)
        text = get_text_from_html(html)
        # Count_Words(html,url,db_words,db_global_words)
        save_doc(url,text,offline)
        # if d >= deep:
        #     q.get()
        #     continue
        for (link, _) in links:
            # if link.endswith('.html'):
            #     print('OOOOOOOOOOOOOOO')
            if real_web_name(link) and keep_domain(url,link,offline) and ((link, _) not in q.queue or link not in l) and os.path.isfile(link):
                q.put((link,d+1))
    # save_indexer(l)
    save_dict()

def save_dict():
    global dict
    dump(dict,'words.joblib')

def get_title(html,url):
    i = html.find('class="headingText">',0,len(html))
    if i == -1:
        return ''
    j = html.find('</h1>',i,len(html))
    if j == -1:
        return ''
    text = html[i+21:j]
    text = text.lower()
    text = text.replace('&aacute;','á')
    text = text.replace('&eacute;','é')
    text = text.replace('&iacute;','í')
    text = text.replace('&oacute;','ó')
    text = text.replace('&uacute;','ú')
    text = text.replace('&ntilde;','ñ')
    with open(create_path(url,'title'),'w', encoding='utf8', errors='ignore') as f:
        f.write(text)
    return text

def crawl_js(js_text):
    script = """
        () => {
            return {
                width: document.documentElement.clientWidth,
                height: document.documentElement.clientHeight,
                deviceScaleFactor: window.devicePixelRatio,
            }
        }
    """
    html = HTML(html=js_text)
    val = html.render(script=script,reload=False)
    return html.html

def keep_domain(url,link,offline):
    if offline:
        return True
    else:
        if link.find(url) == -1:
            return False
        return True

def init_db():
    global client
    db = client.crawler_database
    # ! db.words    <----------------->  (word,local_amount,url)
    # ! db.global_words    <---------->  (word,global_amount)
    return db.words, db.global_words

def Count_Words(text,url,db_words,db_global_words):
    tokens = [word for sent in nltk.sent_tokenize(text) for word in nltk.word_tokenize(sent)]
    global dict
    for t in tokens:
        if t in dict:
            dict[t] += 1
        else:
            dict[t] = 1
    print(f'dict-keys:{len(dict.keys())}')  
    # inserting words into the mongo DB
    # for key in dict.keys():
    #     db_words.insert({"word":key,"count":dict[key],"url":url})
    #     db_global_words.update(
    #         {'word':key},
    #         {'$inc':{'count':dict[key]}},
    #         upsert=True
    #     )

# ? Given a word how many times appear in the hole corpus
def count_word(db_global_words,word):
    filtro = db_global_words.find_one({'word': word})
    return word, filtro['count']

# ? Given a word how many times appear in an specific url
def count_word_in_url(db_words,word,url):
    filtro = db_words.find(
        { '$and': [{'word': word},
                {'url':url}]}
    )
    return sum([c['count'] for c in filtro],0)

def most_common_word(db_global_words):
    filtro = db_global_words.find({}).sort('count',pymongo.DESCENDING)
    return filtro[0]['word'], filtro[0]['count']

def most_common_word_in_url(db_words,url):
    filtro = db_words.find( { "url": url } ).sort('count',pymongo.DESCENDING)
    return filtro[0]['word'], filtro[0]['count']

def erase_content(db):
    db.delete_many({})
    db.remove({})

def show_content(db):
    filtro = db.find({})
    for item in filtro:
        print(f"{item['word']} : {item['count']}")

def consult(consulta,word='',url=''):
    db_words,db_global_words = init_db()
    if consulta is Consult.word_repetition:
        return count_word(db_global_words,word)
    elif consulta is Consult.word_repetition_in_url:
        return count_word_in_url(db_words,word,url)
    elif consulta is Consult.most_common_word:
        return most_common_word(db_global_words)
    elif consulta is Consult.most_common_word_in_url:
        return most_common_word_in_url(db_words,url)

# db1,db2 = init_db()
# erase_content(db1)
# erase_content(db2)
# print("Todo Borrado")

url = ["https://www.revolico.com/"]
rev_path = "https://www.revolico.com/"
              
#crawler call
crawler(rev_path, True)
