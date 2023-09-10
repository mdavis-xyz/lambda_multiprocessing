import multiprocessing
import time
import sys

from lambda_multiprocessing import Pool as LambdaPool
import requests

def CreatePageWithWatermark(body: bytes, url):
    time.sleep(1)
    ret = (len(body), url)
    print(f"Returning ({type(ret[0])}, {type(ret[1])})")
    return ret

def PDFMergeWorker(event={}, context=None):

    url='https://example.com/'
    r = requests.get(url)

    pages_data = [(
            r.content,
            url
        ) for _ in range(0,2)]
    
    with LambdaPool() as executor:
        rendered_pages = executor.starmap(CreatePageWithWatermark, pages_data)

    expected_length = len(requests.get(url).content)
    expected = [(expected_length, url) for _ in pages_data]
    assert(rendered_pages == expected)

if __name__ == '__main__':
    PDFMergeWorker()