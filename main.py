import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup
import time
import urllib3
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
import os
from datetime import datetime
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

# Импортируем необходимые модули Tkinter
import tkinter as tk
from tkinter import filedialog, messagebox
import threading

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Глобальная переменная для пути к chromedriver
chromedriver_path = ''

# Общее количество страниц для парсинга
TOTAL_PAGES = 311952

# Глобальная переменная для результата парсинга
parsing_result = None

def get_view_state_and_cookies(driver):
    driver.get(url)
    WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.ID, 'j_id1:javax.faces.ViewState:3')))
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    view_state_element = soup.find('input', {'id': 'j_id1:javax.faces.ViewState:3'})
    view_state = view_state_element['value'] if view_state_element else None
    cookies = {cookie['name']: cookie['value'] for cookie in driver.get_cookies()}
    return view_state, cookies

headers = {
    'Accept': 'application/xml, text/xml, */*; q=0.01',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'Faces-Request': 'partial/ajax',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)',
    'X-Requested-With': 'XMLHttpRequest',
    'Referer': 'https://eokno.gov.kz/public-register/register-ktrm.xhtml',
    'Origin': 'https://eokno.gov.kz',
}

# Функция для чтения обработанных страниц из файла
def load_processed_pages():
    if os.path.exists("processed_pages.txt"):
        with open("processed_pages.txt", "r") as f:
            processed_pages = set(int(line.strip()) for line in f if line.strip().isdigit())
    else:
        processed_pages = set()
    return processed_pages

# Функция для записи обработанной страницы в файл
def save_processed_page(page_number):
    with open("processed_pages.txt", "a") as f:
        f.write(f"{page_number}\n")

def fetch_links_from_page(page_number, session, view_state, cookies, max_retries=30):
    offset = (page_number - 1) * 5
    data = {
        'javax.faces.partial.ajax': 'true',
        'javax.faces.source': 'dApplicationListOpened:ktrmListForm:listTable',
        'javax.faces.partial.execute': 'dApplicationListOpened:ktrmListForm:listTable',
        'javax.faces.partial.render': 'dApplicationListOpened:ktrmListForm:listTable',
        'dApplicationListOpened:ktrmListForm:listTable': 'dApplicationListOpened:ktrmListForm:listTable',
        'dApplicationListOpened:ktrmListForm:listTable_pagination': 'true',
        'dApplicationListOpened:ktrmListForm:listTable_first': offset,
        'dApplicationListOpened:ktrmListForm:listTable_rows': 5,
        'dApplicationListOpened:ktrmListForm:listTable_skipChildren': 'true',
        'dApplicationListOpened:ktrmListForm:listTable_encodeFeature': 'true',
        'dApplicationListOpened:ktrmListForm': 'dApplicationListOpened:ktrmListForm',
        'javax.faces.ViewState': view_state,
    }
    
    session.cookies.clear()
    for name, value in cookies.items():
        session.cookies.set(name, value)

    for attempt in range(max_retries):
        try:
            response = session.post(url, headers=headers, data=data, verify=False)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'xml')
                links = [
                    f"https://eokno.gov.kz/ktrm/ktrm1OpenedApp.xhtml?id={row['data-rk']}"
                    for update in soup.find_all('update', id="dApplicationListOpened:ktrmListForm:listTable")
                    for row in BeautifulSoup(update.text, 'html.parser').select('tr[data-rk]')
                ]
                if len(links) > 0:
                    log_page_links(page_number, links)  # Запись логов
                    return links
                else:
                    print(f"Недостаточно ссылок ({len(links)}) на странице {page_number}. Попытка {attempt + 1}")
            else:
                print(f"Ошибка: Страница {page_number} вернула статус {response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"Ошибка на странице {page_number}, попытка {attempt + 1}/{max_retries}: {e}")
        
        # Экспоненциальная задержка с добавлением случайного интервала
        delay = 2 ** min(attempt, 6) + random.uniform(0.5, 1.5)
        time.sleep(delay)
    
    print(f"Не удалось получить данные со страницы {page_number} после {max_retries} попыток.")
    return []

def log_page_links(page_number, links):
    with open("detailed_logs.txt", "a", encoding="utf-8") as log_file:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        for index, link in enumerate(links, start=1):
            log_file.write(f"Страница {page_number} - Ссылка {index} - Время: {timestamp} - {link}\n")

def collect_links(pages_to_scrape, driver, processed_pages):
    session = requests.Session()
    all_links = []
    view_state, cookies = get_view_state_and_cookies(driver)
    if not view_state:
        print("Не удалось получить ViewState. Проверьте доступ к странице.")
        driver.quit()
        return []
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_page = {}
        for page in pages_to_scrape:
            if page in processed_pages:
                print(f"Страница {page} уже обработана, пропуск.")
                continue
            future = executor.submit(fetch_links_from_page, page, session, view_state, cookies)
            future_to_page[future] = page
            
        for future in as_completed(future_to_page):
            page = future_to_page[future]
            try:
                page_links = future.result()
                if page_links:
                    all_links.extend(page_links)
                    with open("links.txt", "a") as file:
                        for link in page_links:
                            file.write(link + "\n")
                    print(f"Собрано {len(page_links)} ссылок на странице {page}")
                    
                    # Сохраняем номер обработанной страницы
                    save_processed_page(page)
                else:
                    print(f"Не удалось получить ссылки со страницы {page}")
            except Exception as exc:
                print(f"Страница {page} вызвала исключение: {exc}")

    driver.quit()
    return all_links

def main():
    global chromedriver_path
    global parsing_thread
    # Создаем главное окно
    root = tk.Tk()
    root.withdraw()  # Скрываем главное окно

    messagebox.showinfo("Добро пожаловать", "Добро пожаловать в парсер.")

    # Открываем диалоговое окно для выбора файла chromedriver.exe
    messagebox.showinfo("Выбор chromedriver", "Пожалуйста, выберите файл chromedriver.exe")
    chromedriver_path = filedialog.askopenfilename(
        title="Выберите файл chromedriver.exe",
        filetypes=(("Chromedriver", "chromedriver.exe"), ("Все файлы", "*.*"))
    )

    if not chromedriver_path:
        messagebox.showerror("Ошибка", "Вы не выбрали файл chromedriver.exe. Программа будет закрыта.")
        return

    # Проверяем, существует ли файл chromedriver.exe
    if not os.path.exists(chromedriver_path):
        messagebox.showerror("Ошибка", f"Файл {chromedriver_path} не найден. Программа будет закрыта.")
        return

    # Указываем URL для парсинга
    global url
    url = "https://eokno.gov.kz/public-register/register-ktrm.xhtml"

    # Создаем сервис и драйвер с использованием выбранного chromedriver
    service = Service(executable_path=chromedriver_path)
    driver = webdriver.Chrome(service=service)

    # Загружаем список уже обработанных страниц
    processed_pages = load_processed_pages()

    # Формируем список страниц для парсинга, исключая уже обработанные
    pages_to_scrape = [page for page in range(1, TOTAL_PAGES + 1) if page not in processed_pages]

    if not pages_to_scrape:
        messagebox.showinfo("Информация", "Все страницы уже были обработаны.")
        return

    # Запускаем парсинг в отдельном потоке, чтобы не блокировать GUI
    parsing_thread = threading.Thread(target=run_parsing, args=(pages_to_scrape, driver, processed_pages))
    parsing_thread.start()

    # Функция для проверки завершения потока
    def check_thread():
        if parsing_thread.is_alive():
            root.after(1000, check_thread)  # Проверяем снова через 1 секунду
        else:
            # Поток завершен, показываем сообщение и закрываем приложение
            if parsing_result is not None:
                messagebox.showinfo("Готово", f"Парсинг завершён. Собрано {parsing_result} уникальных ссылок.")
            else:
                messagebox.showinfo("Готово", "Парсинг завершён.")
            root.quit()  # Завершаем главный цикл Tkinter

    # Запускаем проверку потока
    root.after(1000, check_thread)

    # Запускаем главный цикл Tkinter
    root.mainloop()

    # Ждем завершения потока парсинга после закрытия GUI
    parsing_thread.join()

def run_parsing(pages_to_scrape, driver, processed_pages):
    global parsing_result
    all_links = collect_links(pages_to_scrape, driver, processed_pages)

    unique_links = sorted(set(all_links), key=lambda x: int(x.split('=')[-1]))
    with open("final_links.txt", "a") as final_file:
        for link in unique_links:
            final_file.write(link + "\n")

    parsing_result = len(unique_links)

if __name__ == "__main__":
    main()
