#!python3
# -*- coding: utf-8 -*-

import os
import sys
sys.path.append( os.path.join(os.path.dirname(__file__), '../lib') )
from service.mlit_fudousantorihiki import MlitFudousanTorihikiService

def main():
    fudosan_torihiki_service = MlitFudousanTorihikiService()
    fudosan_torihiki_service.download_save_master()
    

if __name__ == '__main__':
    main()
