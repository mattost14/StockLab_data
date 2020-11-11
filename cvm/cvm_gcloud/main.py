from checkPackages import checkPackages
from datetime import datetime


def downloadCVMdata(event,context):
    print('--- Downloading CVM data ---')
    start = datetime.now()
    checkPackages('cia_aberta-doc-itr')
    end = datetime.now()
    print(f'Operation completed in : {end - start}')
    

if __name__ == "__main__": 
    downloadCVMdata([],[])
    
    # checkPackages('cia_aberta-doc-dfp-dre')
    # checkPackages('cia_aberta-doc-dfp-bpa')
    # checkPackages('cia_aberta-doc-dfp-bpp')
