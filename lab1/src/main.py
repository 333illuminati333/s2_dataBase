from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from lxml import etree
import os
import webbrowser


def cleanup():
    try:
        os.remove("task1.xml")
        os.remove("task2.xml")
        os.remove("task2.xhtml")
    except OSError:
        pass


def scrap_data():
    process = CrawlerProcess(get_project_settings())
    process.crawl('posolstva')
    process.crawl('odissey')
    process.start()


def task1():
    print("Task #1")
    root = etree.parse("task1.xml")
    pages = root.xpath("//page")
    maxText = {}
    firstMax = 0
    for page in pages:
        url = page.xpath("@url")[0]
        count = page.xpath("count(fragment[@type='text'])")
        if count > firstMax:

            firstMax = count
            maxText = {url, count}

    print(maxText)


def task2():
    print("Task #2")
    transform = etree.XSLT(etree.parse("task2.xsl"))
    result = transform(etree.parse("task2.xml"))
    result.write("task2.xhtml", pretty_print=True, encoding="UTF-8")
    print("Opening  web-browser...")
    webbrowser.open('file://' + os.path.realpath("task2.xhtml"))


if __name__ == '__main__':
    print("Lab #1")
    cleanup()
    scrap_data()
    while True:
        print("-" * 45)
        print("Input number of task to execute, or something else to exit:")
        print("1. Task #1")
        print("2. Task #2")
        print("> ", end='', flush=True)
        number = input()
        if number == "1":
            task1()
        elif number == "2":
            task2()
        else:
            break
